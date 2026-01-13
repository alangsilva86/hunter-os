"""Search provider abstractions for enrichment."""

import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List

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


class SearchProvider(ABC):
    name: str = "base"

    @abstractmethod
    async def search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _extract_links(self, data: Dict[str, Any]) -> List[str]:
        links: List[str] = []
        if isinstance(data, dict):
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


class SerpDevProvider(SearchProvider):
    name = "serpdev"

    def __init__(self, api_key: str, base_url: str = None):
        self.api_key = api_key
        self.base_url = base_url or os.getenv("SERPDEV_BASE_URL", "https://serp.dev/api/v1/search")

    async def search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        params = {"api_key": self.api_key, "q": query}
        async with session.get(self.base_url, params=params) as resp:
            data = await resp.json()
        links = self._extract_links(data)
        return self._classify(links)


class SerpApiProvider(SearchProvider):
    name = "serpapi"

    def __init__(self, api_key: str, base_url: str = None):
        self.api_key = api_key
        self.base_url = base_url or os.getenv("SERPAPI_BASE_URL", "https://serpapi.com/search.json")

    async def search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        params = {"api_key": self.api_key, "q": query}
        async with session.get(self.base_url, params=params) as resp:
            data = await resp.json()
        links = self._extract_links(data)
        return self._classify(links)


class BingProvider(SearchProvider):
    name = "bing"

    def __init__(self, api_key: str, base_url: str = None):
        self.api_key = api_key
        self.base_url = base_url or os.getenv("BING_BASE_URL", "https://api.bing.microsoft.com/v7.0/search")

    async def search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        params = {"q": query}
        async with session.get(self.base_url, headers=headers, params=params) as resp:
            data = await resp.json()
        links = self._extract_links(data)
        return self._classify(links)


def select_provider(name: str) -> SearchProvider:
    name = (name or "").lower().strip()
    if name == "serpdev":
        key = os.getenv("SERPDEV_API_KEY")
        if not key:
            raise RuntimeError("SERPDEV_API_KEY nao configurada")
        return SerpDevProvider(key)
    if name == "serpapi":
        key = os.getenv("SERPAPI_API_KEY")
        if not key:
            raise RuntimeError("SERPAPI_API_KEY nao configurada")
        return SerpApiProvider(key)
    if name == "bing":
        key = os.getenv("BING_API_KEY")
        if not key:
            raise RuntimeError("BING_API_KEY nao configurada")
        return BingProvider(key)
    raise RuntimeError("Search provider invalido")
