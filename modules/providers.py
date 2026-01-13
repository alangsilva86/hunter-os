"""Search provider abstractions for enrichment."""

import json
import os
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import aiohttp

SOCIAL_BLOCKLIST = [
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "tiktok.com",
    "youtube.com",
    "maps.google.com",
    "google.com/maps",
]


class ProviderResponseError(RuntimeError):
    """Raised when a search provider returns a non-JSON or error response."""

    def __init__(self, message: str, status_code: Optional[int] = None, payload: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


def _redact_api_key(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"(api_key=)[^&\\s]+", r"\\1***", text, flags=re.IGNORECASE)


class SearchProvider(ABC):
    name: str = "base"

    @abstractmethod
    async def search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        raise NotImplementedError

    async def _safe_json(self, resp: aiohttp.ClientResponse) -> Dict[str, Any]:
        content_type = resp.headers.get("Content-Type", "")
        text = await resp.text()
        if resp.status >= 400:
            excerpt = _redact_api_key(text).replace("\n", " ")[:200]
            payload: Dict[str, Any] = {}
            try:
                payload = json.loads(text)
                message = payload.get("message") or payload.get("error") or excerpt
            except json.JSONDecodeError:
                message = excerpt
            raise ProviderResponseError(
                f"{self.name} HTTP {resp.status}: {message}",
                status_code=resp.status,
                payload=payload,
            )
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            excerpt = _redact_api_key(text).replace("\n", " ")[:200]
            if content_type:
                raise ProviderResponseError(
                    f"{self.name} resposta nao-JSON (content-type={content_type}): {excerpt}",
                    status_code=resp.status,
                )
            raise ProviderResponseError(f"{self.name} resposta nao-JSON: {excerpt}", status_code=resp.status)

    def _extract_links(self, data: Dict[str, Any]) -> List[str]:
        links: List[str] = []
        if isinstance(data, dict):
            if "organic" in data:
                for item in data.get("organic", []) or []:
                    link = item.get("link") or item.get("url")
                    if link:
                        links.append(link)
            if "organic_results" in data:
                for item in data.get("organic_results", []) or []:
                    link = item.get("link") or item.get("url")
                    if link:
                        links.append(link)
            if "results" in data:
                for item in data.get("results", []) or []:
                    link = item.get("link") or item.get("url")
                    if link:
                        links.append(link)
            if "webPages" in data:
                for item in data.get("webPages", {}).get("value", []) or []:
                    link = item.get("url")
                    if link:
                        links.append(link)
        return links

    def _classify(self, links: List[str]) -> Dict[str, Any]:
        site = None
        instagram = None
        linkedin_company = None
        linkedin_people: List[str] = []

        for link in links:
            if "instagram.com" in link and not instagram:
                instagram = link
            if "linkedin.com/company" in link and not linkedin_company:
                linkedin_company = link
            if "linkedin.com/in/" in link:
                linkedin_people.append(link)

        for link in links:
            if any(block in link for block in SOCIAL_BLOCKLIST):
                continue
            site = link
            break

        return {
            "site": site,
            "instagram": instagram,
            "linkedin_company": linkedin_company,
            "linkedin_people": linkedin_people[:5],
            "links": links,
        }


class SerperProvider(SearchProvider):
    name = "serper"

    def __init__(self, api_key: str, base_url: str = None):
        self.api_key = api_key
        self.base_url = base_url or os.getenv("SERPER_BASE_URL", "https://google.serper.dev/search")
        self.gl = os.getenv("SERPER_GL", "br")
        self.hl = os.getenv("SERPER_HL", "pt-br")

    async def search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {"q": query, "gl": self.gl, "hl": self.hl}
        async with session.post(self.base_url, headers=headers, json=payload) as resp:
            data = await self._safe_json(resp)
        links = self._extract_links(data)
        return self._classify(links)


def select_provider(name: str) -> SearchProvider:
    name = (name or "").lower().strip()
    if name == "serper":
        key = os.getenv("SERPER_API_KEY")
        if not key:
            raise RuntimeError("SERPER_API_KEY nao configurada")
        return SerperProvider(key)
    raise RuntimeError("Search provider invalido (use 'serper')")
