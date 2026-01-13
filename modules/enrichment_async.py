"""Async enrichment pipeline."""

import asyncio
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp

from modules import storage
from modules.providers import SearchProvider

CONTACT_PATHS = ["/contato", "/fale-conosco", "/contact", "/contato/", "/fale-conosco/"]

TECH_SIGNATURES = {
    "google_tag_manager": ["googletagmanager.com/gtm.js", "GTM-"],
    "google_analytics": ["google-analytics.com/analytics.js", "gtag('config'"],
    "meta_pixel": ["connect.facebook.net", "fbq('init'"],
    "wordpress": ["wp-content", "wp-includes"],
    "shopify": ["cdn.shopify.com", "Shopify"],
    "vtex": ["vteximg.com.br", "vtex"],
    "rd_station": ["rdstation.com.br", "rdstation"],
    "hubspot": ["hs-scripts.com", "hubspot"],
}

TECH_WEIGHTS = {
    "google_tag_manager": 5,
    "google_analytics": 5,
    "meta_pixel": 5,
    "wordpress": 5,
    "shopify": 8,
    "vtex": 10,
    "rd_station": 10,
    "hubspot": 10,
}


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _has_form(html: str) -> bool:
    return "<form" in html.lower()


def _has_whatsapp_link(html: str) -> bool:
    return "wa.me/" in html or "api.whatsapp.com" in html


def _detect_tech(html: str) -> Tuple[Dict[str, bool], int]:
    html_lower = html.lower()
    detected: Dict[str, bool] = {}
    score = 0
    for key, patterns in TECH_SIGNATURES.items():
        found = False
        for pat in patterns:
            if pat.lower() in html_lower:
                found = True
                break
        if found:
            detected[key] = True
            score += TECH_WEIGHTS.get(key, 0)
    return detected, min(score, 30)


class AsyncEnricher:
    def __init__(
        self,
        provider: SearchProvider,
        concurrency: int = 10,
        timeout: int = 5,
        cache_ttl_hours: int = 24,
    ):
        self.provider = provider
        self.concurrency = max(1, min(concurrency, 20))
        self.timeout = timeout
        self.cache_ttl_hours = cache_ttl_hours

    async def _fetch_html(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        try:
            async with session.get(url, timeout=self.timeout) as resp:
                if resp.status >= 400:
                    return None
                return await resp.text()
        except Exception:
            return None

    async def _fetch_builtwith(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        api_key = os.getenv("BUILTWITH_API_KEY")
        if not api_key or not url:
            return []
        base_url = os.getenv("BUILTWITH_BASE_URL", "https://api.builtwith.com/v21/api.json")
        params = {"KEY": api_key, "LOOKUP": url}
        try:
            async with session.get(base_url, params=params, timeout=self.timeout) as resp:
                if resp.status >= 400:
                    return []
                data = await resp.json()
        except Exception:
            return []

        technologies: List[str] = []
        for result in data.get("Results", []) or []:
            for path in result.get("Result", {}).get("Paths", []) or []:
                for tech in path.get("Technologies", []) or []:
                    name = tech.get("Name") or tech.get("Tag")
                    if name:
                        technologies.append(name)
        return list(dict.fromkeys(technologies))

    async def _search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        cache_key = f"search:{_hash_key(query)}"
        cached = storage.cache_get(cache_key)
        if cached:
            return cached
        data = await self.provider.search(session, query)
        storage.cache_set(cache_key, data, ttl_hours=self.cache_ttl_hours)
        return data

    async def _enrich_one(self, session: aiohttp.ClientSession, lead: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        razao = lead.get("razao_social", "")
        municipio = lead.get("municipio", "")
        uf = lead.get("uf", "")
        query = f"{razao} {municipio} {uf}".strip()
        result = {
            "cnpj": lead.get("cnpj"),
            "run_id": run_id,
            "site": None,
            "instagram": None,
            "linkedin_company": None,
            "linkedin_people": [],
            "google_maps_url": lead.get("flags", {}).get("google_maps_url"),
            "has_contact_page": False,
            "has_form": False,
            "tech_stack": {},
            "tech_score": 0,
            "contact_quality": lead.get("contact_quality"),
            "notes": "",
        }

        if not query:
            return result

        search_data = await self._search(session, query)
        result.update({
            "site": search_data.get("site"),
            "instagram": search_data.get("instagram"),
            "linkedin_company": search_data.get("linkedin_company"),
            "linkedin_people": search_data.get("linkedin_people", []),
        })

        site = result.get("site")
        if site:
            html = await self._fetch_html(session, site)
            if html:
                result["has_form"] = _has_form(html)
                result["tech_stack"], result["tech_score"] = _detect_tech(html)
                result["tech_stack"]["has_whatsapp_link"] = _has_whatsapp_link(html)

            for path in CONTACT_PATHS:
                contact_url = urljoin(site, path)
                html_contact = await self._fetch_html(session, contact_url)
                if html_contact:
                    result["has_contact_page"] = True
                    if not result["has_form"]:
                        result["has_form"] = _has_form(html_contact)
                    if not result["tech_stack"]:
                        result["tech_stack"], result["tech_score"] = _detect_tech(html_contact)
                    break

            builtwith = await self._fetch_builtwith(session, site)
            if builtwith:
                result["tech_stack"]["builtwith"] = builtwith
                result["tech_score"] = min((result.get("tech_score") or 0) + 5, 30)

        return result

    async def enrich_batch(
        self,
        leads: List[Dict[str, Any]],
        run_id: str,
        cancel_event: Optional[asyncio.Event] = None,
    ) -> List[Dict[str, Any]]:
        semaphore = asyncio.Semaphore(self.concurrency)
        results: List[Dict[str, Any]] = []

        async def runner(lead: Dict[str, Any]):
            if cancel_event and cancel_event.is_set():
                return
            async with semaphore:
                try:
                    enriched = await self._enrich_one(session, lead, run_id)
                    results.append(enriched)
                except Exception as exc:
                    storage.log_event("error", "enrichment_error", {"cnpj": lead.get("cnpj"), "error": str(exc)})

        timeout = aiohttp.ClientTimeout(total=self.timeout + 2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [runner(lead) for lead in leads]
            await asyncio.gather(*tasks)

        return results
