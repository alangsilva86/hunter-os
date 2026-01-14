"""In-house tech detection based on HTML, headers, and cookies."""

import asyncio
import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

from modules import storage

TECH_SIGNATURES: Dict[str, Dict[str, Any]] = {
    "rd_station": {
        "category": "marketing",
        "script_src": [r"rdstation\.com\.br", r"rdstation\.com"],
        "html": [r"rdstation", r"rd\strack"],
        "cookies": [r"rdtrk", r"rdstation"],
        "headers": [],
    },
    "hubspot": {
        "category": "marketing",
        "script_src": [r"js\.hs-scripts\.com", r"hs-scripts\.com", r"hubspot\.net"],
        "html": [r"hubspot", r"_hsq"],
        "cookies": [r"hubspotutk"],
        "headers": [],
    },
    "salesforce": {
        "category": "enterprise",
        "script_src": [r"salesforce", r"force\.com"],
        "html": [r"salesforce", r"force\.com"],
        "cookies": [r"sid", r"sid_client", r"BrowserId"],
        "headers": [r"x-powered-by:\s*salesforce"],
    },
    "oracle": {
        "category": "enterprise",
        "script_src": [r"oracle", r"eloqua"],
        "html": [r"oracle", r"eloqua"],
        "cookies": [r"ora_", r"eloqua"],
        "headers": [r"x-powered-by:\s*oracle"],
    },
    "sap": {
        "category": "enterprise",
        "script_src": [r"sap", r"hybris"],
        "html": [r"sap", r"hybris"],
        "cookies": [r"sap", r"hybris"],
        "headers": [r"x-powered-by:\s*sap"],
    },
    "activecampaign": {
        "category": "marketing",
        "script_src": [r"trackcmp\.net", r"activecampaign\.com"],
        "html": [r"activecampaign"],
        "cookies": [r"ac_", r"activesession"],
        "headers": [],
    },
    "google_tag_manager": {
        "category": "analytics",
        "script_src": [r"googletagmanager\.com/gtm\.js"],
        "html": [r"GTM-[A-Z0-9]+"],
        "cookies": [],
        "headers": [],
    },
    "google_analytics": {
        "category": "analytics",
        "script_src": [r"googletagmanager\.com/gtag/js", r"google-analytics\.com"],
        "html": [r"gtag\('config'", r"ga\('create'"],
        "cookies": [r"_ga"],
        "headers": [],
    },
    "meta_pixel": {
        "category": "analytics",
        "script_src": [r"connect\.facebook\.net/.+/fbevents\.js"],
        "html": [r"fbq\('init'"],
        "cookies": [r"_fbp"],
        "headers": [],
    },
    "microsoft_clarity": {
        "category": "analytics",
        "script_src": [r"clarity\.ms"],
        "html": [r"clarity\("],
        "cookies": [r"_clck", r"_clsk"],
        "headers": [],
    },
    "wordpress": {
        "category": "ecommerce",
        "script_src": [r"wp-content", r"wp-includes"],
        "html": [r"wp-content", r"wp-includes"],
        "cookies": [r"wordpress_", r"wp-settings-"],
        "headers": [r"x-powered-by:\s*wordpress"],
    },
    "shopify": {
        "category": "ecommerce",
        "script_src": [r"cdn\.shopify\.com", r"shopify"],
        "html": [r"shopify"],
        "cookies": [r"_shopify", r"cart"],
        "headers": [],
    },
    "vtex": {
        "category": "ecommerce",
        "script_src": [r"vteximg", r"vtexassets", r"vtex"],
        "html": [r"vtex"],
        "cookies": [r"VtexIdclientAutCookie"],
        "headers": [],
    },
    "magento": {
        "category": "ecommerce",
        "script_src": [r"magento", r"mage/"],
        "html": [r"magento", r"mage/cookies"],
        "cookies": [r"mage-", r"frontend="],
        "headers": [],
    },
    "wix": {
        "category": "ecommerce",
        "script_src": [r"wix\.com", r"wixsite\.com"],
        "html": [r"wix"],
        "cookies": [r"wix"],
        "headers": [],
    },
    "loja_integrada": {
        "category": "ecommerce",
        "script_src": [r"lojaintegrada\.com\.br"],
        "html": [r"lojaintegrada"],
        "cookies": [r"lojaintegrada"],
        "headers": [],
    },
    "jivochat": {
        "category": "chat",
        "script_src": [r"jivochat\.com"],
        "html": [r"jivochat"],
        "cookies": [r"jivochat"],
        "headers": [],
    },
    "zendesk": {
        "category": "chat",
        "script_src": [r"static\.zendesk\.com", r"zdassets\.com"],
        "html": [r"zendesk"],
        "cookies": [r"zendesk"],
        "headers": [],
    },
    "intercom": {
        "category": "chat",
        "script_src": [r"intercomcdn\.com", r"widget\.intercom\.io"],
        "html": [r"intercom"],
        "cookies": [r"intercom"],
        "headers": [],
    },
}

CATEGORY_POINTS = {
    "marketing": 10,
    "analytics": 7,
    "ecommerce": 7,
    "chat": 3,
    "enterprise": 8,
}

CONFIDENCE_WEIGHTS = {
    "cookie": 30,
    "header": 25,
    "script_src": 20,
    "html": 10,
}

GOLDEN_TECHS = {
    "salesforce",
    "hubspot",
    "vtex",
    "oracle",
    "sap",
    "shopify",
    "magento",
    "zendesk",
    "rd_station",
}

TRANSIENT_STATUS = {429, 500, 502, 503, 504}


def _extract_script_srcs(html: str) -> List[str]:
    if not html:
        return []
    pattern = re.compile(r"<script[^>]+src=[\"']([^\"']+)[\"']", re.IGNORECASE)
    return pattern.findall(html)


def _normalize_url_candidates(url: str) -> List[str]:
    if not url:
        return []
    parsed = urlparse(url if "://" in url else f"https://{url}")
    domain = parsed.netloc or parsed.path
    domain = domain.strip("/")
    if not domain:
        return []
    candidates = [
        f"https://{domain}",
        f"http://{domain}",
    ]
    if not domain.startswith("www."):
        candidates.append(f"https://www.{domain}")
    return list(dict.fromkeys(candidates))


def _regex_search(patterns: Iterable[str], text: str) -> List[str]:
    matches = []
    for pat in patterns:
        if not pat:
            continue
        try:
            if re.search(pat, text, re.IGNORECASE):
                matches.append(pat)
        except re.error:
            if pat.lower() in text.lower():
                matches.append(pat)
    return matches


def _cookie_names(cookie_headers: List[str]) -> List[str]:
    names = []
    for header in cookie_headers:
        if not header:
            continue
        name = header.split(";", 1)[0].split("=", 1)[0].strip()
        if name:
            names.append(name)
    return names


class TechSniperDetector:
    def __init__(
        self,
        timeout: Optional[int] = None,
        cache_ttl_hours: Optional[int] = None,
        max_redirects: int = 3,
        retries: int = 2,
    ):
        self.timeout = timeout if timeout is not None else int(os.getenv("TIMEOUT", "5"))
        self.cache_ttl_hours = cache_ttl_hours if cache_ttl_hours is not None else int(os.getenv("CACHE_TTL_HOURS", "24"))
        self.max_redirects = max_redirects
        self.retries = retries

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> Dict[str, Any]:
        last_error = None
        for attempt in range(self.retries + 1):
            try:
                start = time.time()
                async with session.get(url, allow_redirects=True, max_redirects=self.max_redirects, timeout=self.timeout) as resp:
                    text = await resp.text()
                    fetch_ms = int((time.time() - start) * 1000)
                    if resp.status in TRANSIENT_STATUS and attempt < self.retries:
                        last_error = f"status:{resp.status}"
                        await asyncio.sleep(0.3 * (attempt + 1))
                        continue
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    try:
                        set_cookies = resp.headers.getall("Set-Cookie", [])
                    except AttributeError:
                        set_cookies = resp.headers.get("Set-Cookie", "").split("\n") if resp.headers.get("Set-Cookie") else []
                    return {
                        "html": text,
                        "headers": headers,
                        "cookies": _cookie_names(set_cookies),
                        "fetch_status": resp.status,
                        "fetch_ms": fetch_ms,
                        "fetched_url": str(resp.url),
                        "error": None,
                    }
            except asyncio.TimeoutError as exc:
                last_error = f"timeout: {exc}"
            except aiohttp.ClientResponseError as exc:
                last_error = f"http_error: {exc.status}"
                if exc.status not in TRANSIENT_STATUS:
                    break
            except aiohttp.ClientError as exc:
                last_error = f"client_error: {exc}"

            await asyncio.sleep(0.3 * (attempt + 1))

        return {
            "html": None,
            "headers": {},
            "cookies": [],
            "fetch_status": None,
            "fetch_ms": 0,
            "fetched_url": None,
            "error": last_error or "fetch_failed",
        }

    def analyze_content(
        self,
        html: str,
        headers: Dict[str, str],
        cookies: List[str],
    ) -> Dict[str, Any]:
        detected_stack: List[str] = []
        signals: Dict[str, List[str]] = {}
        tech_sources: Dict[str, List[str]] = {}
        confidence = 0
        html_lower = (html or "").lower()
        header_blob = "\n".join([f"{k}: {v}" for k, v in headers.items()]).lower()
        cookie_blob = ";".join([c.lower() for c in cookies])
        script_srcs = _extract_script_srcs(html)
        script_blob = "\n".join(script_srcs).lower()

        categories_found: Dict[str, bool] = {key: False for key in CATEGORY_POINTS}

        for tech, sig in TECH_SIGNATURES.items():
            evidence: List[str] = []
            matched_types = set()

            script_matches = _regex_search(sig.get("script_src", []), script_blob)
            if script_matches:
                for match in script_matches:
                    evidence.append(f"script_src:{match}")
                matched_types.add("script_src")

            html_matches = _regex_search(sig.get("html", []), html_lower)
            if html_matches:
                for match in html_matches:
                    evidence.append(f"html:{match}")
                matched_types.add("html")

            header_matches = _regex_search(sig.get("headers", []), header_blob)
            if header_matches:
                for match in header_matches:
                    evidence.append(f"header:{match}")
                matched_types.add("header")

            cookie_matches = _regex_search(sig.get("cookies", []), cookie_blob)
            if cookie_matches:
                for match in cookie_matches:
                    evidence.append(f"cookie:{match}")
                matched_types.add("cookie")

            if evidence:
                detected_stack.append(tech)
                signals[tech] = evidence
                sources = sorted({item.split(":", 1)[0] for item in evidence if ":" in item})
                tech_sources[tech] = sources
                category = sig.get("category")
                if category in categories_found:
                    categories_found[category] = True
                for match_type in matched_types:
                    confidence += CONFIDENCE_WEIGHTS.get(match_type, 0)

        detected_stack = list(dict.fromkeys(detected_stack))
        confidence = min(confidence, 100)

        tech_score = 0
        for category, enabled in categories_found.items():
            if enabled:
                tech_score += CATEGORY_POINTS.get(category, 0)
        if {
            "google_tag_manager",
            "meta_pixel",
        }.issubset(set(detected_stack)) and any(
            tech in detected_stack for tech in ("rd_station", "hubspot", "activecampaign")
        ):
            tech_score += 3
        tech_score = min(tech_score, 30)

        has_marketing = categories_found.get("marketing", False)
        has_analytics = categories_found.get("analytics", False)
        has_ecommerce = categories_found.get("ecommerce", False)
        has_chat = categories_found.get("chat", False)
        has_whatsapp_link = "wa.me/" in html_lower or "api.whatsapp.com" in html_lower
        golden_techs_found = [tech for tech in detected_stack if tech in GOLDEN_TECHS]
        tech_sources_flat = sorted({src for sources in tech_sources.values() for src in sources})

        return {
            "detected_stack": detected_stack,
            "signals": signals,
            "tech_score": tech_score,
            "confidence": confidence,
            "has_marketing": has_marketing,
            "has_analytics": has_analytics,
            "has_ecommerce": has_ecommerce,
            "has_chat": has_chat,
            "has_whatsapp_link": has_whatsapp_link,
            "golden_techs_found": golden_techs_found,
            "tech_sources": tech_sources,
            "tech_sources_flat": tech_sources_flat,
        }

    async def detect(
        self,
        url: str,
        session: aiohttp.ClientSession,
        return_html: bool = False,
    ) -> Dict[str, Any]:
        candidates = _normalize_url_candidates(url)
        if not candidates:
            return {
                "detected_stack": [],
                "signals": {},
                "tech_score": 0,
                "confidence": 0,
                "has_marketing": False,
                "has_analytics": False,
                "has_ecommerce": False,
                "has_chat": False,
                "has_whatsapp_link": False,
                "fetch_status": None,
                "fetch_ms": 0,
                "fetched_url": None,
                "error": "invalid_url",
                "rendered_used": False,
            }

        for candidate in candidates:
            cache_key = f"tech_sniper:{candidate}"
            cached = storage.cache_get(cache_key)
            if cached:
                cached["cache_hit"] = True
                cached["rendered_used"] = False
                if return_html:
                    cached["_html"] = ""
                return cached

            fetch_result = await self._fetch(session, candidate)
            if not fetch_result.get("html"):
                continue

            html_content = fetch_result.get("html") or ""
            analysis = self.analyze_content(
                html_content,
                fetch_result.get("headers") or {},
                fetch_result.get("cookies") or [],
            )
            result = {
                **analysis,
                "fetch_status": fetch_result.get("fetch_status"),
                "fetch_ms": fetch_result.get("fetch_ms"),
                "fetched_url": fetch_result.get("fetched_url"),
                "error": fetch_result.get("error"),
                "html_size": len(html_content),
                "rendered_used": False,
                "cache_hit": False,
            }
            cache_payload = dict(result)
            storage.cache_set(cache_key, cache_payload, ttl_hours=self.cache_ttl_hours)
            if return_html:
                result["_html"] = html_content
            return result

        return {
            "detected_stack": [],
            "signals": {},
            "tech_score": 0,
            "confidence": 0,
            "has_marketing": False,
            "has_analytics": False,
            "has_ecommerce": False,
            "has_chat": False,
            "has_whatsapp_link": False,
            "fetch_status": None,
            "fetch_ms": 0,
            "fetched_url": None,
            "error": "fetch_failed",
            "html_size": 0,
            "rendered_used": False,
            "cache_hit": False,
        }


class OptionalRenderedDetector:
    def __init__(self, enabled: bool = False, timeout_ms: int = 8000):
        self.enabled = enabled
        self.timeout_ms = timeout_ms

    async def detect(
        self,
        url: str,
        detector: TechSniperDetector,
        base_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self.enabled:
            base_result["rendered_used"] = False
            return base_result

        fetch_status = base_result.get("fetch_status")
        html_size = int(base_result.get("html_size") or 0)
        confidence = int(base_result.get("confidence") or 0)
        tech_score = int(base_result.get("tech_score") or 0)
        needs_render = fetch_status in {403, 429, 520} or html_size < 400
        if tech_score > 0 and confidence >= 30 and not needs_render:
            base_result["rendered_used"] = False
            return base_result

        try:
            from playwright.async_api import async_playwright
        except Exception:
            base_result["rendered_used"] = False
            return base_result

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    page = await browser.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    html = await page.content()
                finally:
                    await browser.close()

            analysis = detector.analyze_content(html, {}, [])
            rendered = {
                **analysis,
                "fetch_status": base_result.get("fetch_status"),
                "fetch_ms": base_result.get("fetch_ms"),
                "fetched_url": base_result.get("fetched_url") or url,
                "error": base_result.get("error"),
                "rendered_used": True,
                "cache_hit": base_result.get("cache_hit", False),
            }
            return rendered
        except Exception:
            base_result["rendered_used"] = False
            return base_result
