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

    def _extract_candidates(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []

        def _add(url: Optional[str], title: str = "", snippet: str = "", source: str = "organic") -> None:
            if not url:
                return
            candidates.append(
                {
                    "url": url,
                    "title": title or "",
                    "snippet": snippet or "",
                    "source": source,
                }
            )

        if isinstance(data, dict):
            for item in data.get("organic", []) or []:
                _add(item.get("link") or item.get("url"), item.get("title") or "", item.get("snippet") or "", "organic")
            for item in data.get("organic_results", []) or []:
                _add(item.get("link") or item.get("url"), item.get("title") or "", item.get("snippet") or "", "organic")
            for item in data.get("results", []) or []:
                _add(item.get("link") or item.get("url"), item.get("title") or "", item.get("snippet") or "", "organic")
            for item in data.get("webPages", {}).get("value", []) or []:
                _add(item.get("url"), item.get("name") or "", item.get("snippet") or "", "organic")

            knowledge = data.get("knowledgeGraph") or data.get("knowledge_graph") or {}
            if isinstance(knowledge, dict):
                _add(
                    knowledge.get("website") or knowledge.get("url"),
                    knowledge.get("title") or knowledge.get("name") or "",
                    knowledge.get("description") or "",
                    "knowledge",
                )

            for item in data.get("places", []) or data.get("local_results", []) or []:
                if not isinstance(item, dict):
                    continue
                _add(
                    item.get("website") or item.get("link") or item.get("url"),
                    item.get("title") or item.get("name") or "",
                    item.get("address") or item.get("snippet") or "",
                    "map",
                )

        return candidates

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
        candidates = self._extract_candidates(data)
        links = [item.get("url") for item in candidates if item.get("url")]
        classified = self._classify(links)
        classified["candidates"] = candidates
        return classified


def select_provider(name: str) -> SearchProvider:
    name = (name or "").lower().strip()
    if name == "serper":
        key = os.getenv("SERPER_API_KEY")
        if not key:
            raise RuntimeError("SERPER_API_KEY nao configurada")
        return SerperProvider(key)
    raise RuntimeError("Search provider invalido (use 'serper')")
