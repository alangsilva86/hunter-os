"""Async enrichment pipeline."""

import asyncio
import hashlib
import json
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiohttp

from modules import storage
from modules.providers import ProviderResponseError, SearchProvider
from modules.tech_detection import OptionalRenderedDetector, TechSniperDetector

CONTACT_PATHS = ["/contato", "/fale-conosco", "/contact", "/contato/", "/fale-conosco/"]


class RateLimiter:
    def __init__(self, rate_per_sec: int, burst: Optional[int] = None):
        self.rate = max(1, int(rate_per_sec))
        self.capacity = burst or self.rate
        self.tokens = float(self.capacity)
        self.updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.updated_at = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait_time = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait_time)


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _has_form(html: str) -> bool:
    return "<form" in html.lower()


def _has_whatsapp_link(html: str) -> bool:
    return "wa.me/" in html or "api.whatsapp.com" in html


def _sanitize_error_message(message: str) -> str:
    if not message:
        return ""
    return re.sub(r"(api_key=)[^&\\s]+", r"\\1***", message, flags=re.IGNORECASE)


def _provider_hint(provider_name: str, message: str) -> Optional[str]:
    provider = (provider_name or "").lower()
    msg = (message or "").lower()
    if provider == "serper":
        if "text/html" in msg or "nao-json" in msg or "lander" in msg:
            return (
                "Serper.dev retornou HTML (lander/bloqueio). Verifique plano/chave no painel "
                "ou confirme se a chave tem permissao ativa."
            )
        return "Verifique a chave/plano no painel do Serper.dev."
    return None


class AsyncEnricher:
    def __init__(
        self,
        provider: SearchProvider,
        concurrency: int = 10,
        timeout: int = 5,
        cache_ttl_hours: int = 24,
    ):
        self.provider = provider
        max_concurrency = int(os.getenv("SERPER_CONCURRENCY", str(concurrency)))
        self.concurrency = max(1, min(concurrency, max_concurrency, 20))
        self.timeout = timeout
        self.cache_ttl_hours = cache_ttl_hours
        self.detector = TechSniperDetector(timeout=timeout, cache_ttl_hours=cache_ttl_hours)
        enable_playwright = os.getenv("ENABLE_PLAYWRIGHT", "0") == "1"
        self.rendered_detector = OptionalRenderedDetector(
            enabled=enable_playwright,
            timeout_ms=int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "8000")),
        )
        self.max_rps = max(1, int(os.getenv("SERPER_MAX_RPS", "5")))
        self.backoff_base = float(os.getenv("PROVIDER_BACKOFF_BASE", "1.5"))
        self.backoff_max = float(os.getenv("PROVIDER_BACKOFF_MAX", "60"))
        self.rate_limiter = RateLimiter(self.max_rps)

    def _backoff_seconds(self, attempt: int) -> float:
        base = self.backoff_base ** max(1, attempt)
        jitter = random.uniform(0, 1)
        return min(self.backoff_max, base + jitter)

    async def _fetch_html(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        try:
            async with session.get(url, timeout=self.timeout) as resp:
                if resp.status >= 400:
                    return None
                return await resp.text()
        except Exception:
            return None

    async def _search(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        cache_key = f"search:{_hash_key(query)}"
        cached = storage.cache_get(cache_key)
        if cached:
            return cached
        await self.rate_limiter.acquire()
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
            "tech_confidence": 0,
            "has_marketing": False,
            "has_analytics": False,
            "has_ecommerce": False,
            "has_chat": False,
            "signals": {},
            "fetched_url": None,
            "fetch_status": None,
            "fetch_ms": 0,
            "rendered_used": False,
            "cache_hit": False,
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
            detection = await self.detector.detect(site, session, return_html=True)
            html = detection.pop("_html", "")
            if self.rendered_detector.enabled and not detection.get("cache_hit"):
                detection = await self.rendered_detector.detect(site, self.detector, detection)
            result["tech_score"] = detection.get("tech_score", 0)
            result["tech_confidence"] = detection.get("confidence", 0)
            result["has_marketing"] = detection.get("has_marketing", False)
            result["has_analytics"] = detection.get("has_analytics", False)
            result["has_ecommerce"] = detection.get("has_ecommerce", False)
            result["has_chat"] = detection.get("has_chat", False)
            result["signals"] = detection.get("signals", {})
            result["fetched_url"] = detection.get("fetched_url")
            result["fetch_status"] = detection.get("fetch_status")
            result["fetch_ms"] = detection.get("fetch_ms") or 0
            result["rendered_used"] = detection.get("rendered_used", False)
            result["cache_hit"] = detection.get("cache_hit", False)
            if detection.get("error"):
                result["notes"] = detection.get("error")
            detected_stack = detection.get("detected_stack", [])
            has_whatsapp_link = detection.get("has_whatsapp_link", False)
            if html:
                result["has_form"] = _has_form(html)
                has_whatsapp_link = has_whatsapp_link or _has_whatsapp_link(html)
            result["tech_stack"] = {
                "detected_stack": detected_stack,
                "has_whatsapp_link": has_whatsapp_link,
            }

            if not detection.get("cache_hit"):
                for path in CONTACT_PATHS:
                    contact_url = urljoin(site, path)
                    html_contact = await self._fetch_html(session, contact_url)
                    if html_contact:
                        result["has_contact_page"] = True
                        if not result["has_form"]:
                            result["has_form"] = _has_form(html_contact)
                        if not result["tech_stack"].get("has_whatsapp_link"):
                            result["tech_stack"]["has_whatsapp_link"] = _has_whatsapp_link(html_contact)
                        if not result["tech_stack"].get("detected_stack"):
                            detection_extra = self.detector.analyze_content(html_contact, {}, [])
                            result["tech_stack"]["detected_stack"] = detection_extra.get("detected_stack", [])
                            result["tech_score"] = max(result["tech_score"], detection_extra.get("tech_score", 0))
                            result["tech_confidence"] = max(result["tech_confidence"], detection_extra.get("confidence", 0))
                        break

        return result

    async def enrich_batch(
        self,
        leads: List[Dict[str, Any]],
        run_id: str,
        cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        semaphore = asyncio.Semaphore(self.concurrency)
        results: List[Dict[str, Any]] = []
        durations_ms: List[int] = []
        error_count = 0
        cache_hits = 0
        progress_lock = asyncio.Lock()
        last_progress_emit = time.monotonic()
        provider_error: Dict[str, Any] = {}
        provider_error_count = 0
        provider_error_logged = False
        provider_error_lock = asyncio.Lock()
        stop_event = asyncio.Event()
        provider_limit_hit = False
        provider_http_status: Optional[int] = None
        provider_message: Optional[str] = None
        provider_backoff_seconds: Optional[float] = None
        start_ts = datetime.utcnow()
        cutoff = start_ts - timedelta(hours=self.cache_ttl_hours)

        def _parse_dt(value: Optional[str]) -> Optional[datetime]:
            if not value:
                return None
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

        cnpjs = [lead.get("cnpj") for lead in leads if lead.get("cnpj")]
        cached_by_cnpj = storage.fetch_enrichments_by_cnpjs(cnpjs)
        fresh_cache: Dict[str, Dict[str, Any]] = {}
        for cnpj, cached in cached_by_cnpj.items():
            enriched_at = _parse_dt(cached.get("enriched_at"))
            if enriched_at and enriched_at >= cutoff:
                fresh_cache[cnpj] = cached

        async def _emit_progress() -> None:
            nonlocal last_progress_emit
            processed_count = len(results)
            if processed_count == 0:
                return
            now = time.monotonic()
            if processed_count % 10 == 0 or (now - last_progress_emit) >= 5:
                last_progress_emit = now
                storage.update_run(run_id, enriched_count=processed_count, errors_count=error_count)
                storage.log_event(
                    "info",
                    "enrichment_progress",
                    {
                        "run_id": run_id,
                        "processed_count": processed_count,
                        "errors_count": error_count,
                        "cache_hits": cache_hits,
                    },
                )

        async def runner(lead: Dict[str, Any]):
            nonlocal cache_hits, error_count, provider_error_count, provider_error_logged
            nonlocal provider_limit_hit, provider_http_status, provider_message, provider_backoff_seconds
            if cancel_event and cancel_event.is_set():
                return
            if stop_event.is_set():
                return
            cached = fresh_cache.get(lead.get("cnpj"))
            if cached:
                cache_hits += 1
                cached_stack = cached.get("tech_stack_json")
                cached_signals = cached.get("signals_json")
                result = {
                    "cnpj": cached.get("cnpj"),
                    "run_id": run_id,
                    "site": cached.get("site"),
                    "instagram": cached.get("instagram"),
                    "linkedin_company": cached.get("linkedin_company"),
                    "linkedin_people": json.loads(cached.get("linkedin_people_json") or "[]"),
                    "google_maps_url": cached.get("google_maps_url"),
                    "has_contact_page": bool(cached.get("has_contact_page")),
                    "has_form": bool(cached.get("has_form")),
                    "tech_stack": json.loads(cached_stack) if cached_stack else {},
                    "tech_score": int(cached.get("tech_score") or 0),
                    "tech_confidence": int(cached.get("tech_confidence") or 0),
                    "has_marketing": bool(cached.get("has_marketing")),
                    "has_analytics": bool(cached.get("has_analytics")),
                    "has_ecommerce": bool(cached.get("has_ecommerce")),
                    "has_chat": bool(cached.get("has_chat")),
                    "signals": json.loads(cached_signals) if cached_signals else {},
                    "fetched_url": cached.get("fetched_url"),
                    "fetch_status": cached.get("fetch_status"),
                    "fetch_ms": cached.get("fetch_ms") or 0,
                    "rendered_used": bool(cached.get("rendered_used")),
                    "contact_quality": cached.get("contact_quality"),
                    "notes": cached.get("notes"),
                    "cache_hit": True,
                }
                results.append(result)
                async with progress_lock:
                    await _emit_progress()
                return
            async with semaphore:
                if stop_event.is_set():
                    return
                try:
                    lead_start = time.time()
                    enriched = await self._enrich_one(session, lead, run_id)
                    durations_ms.append(int((time.time() - lead_start) * 1000))
                    if enriched.get("cache_hit"):
                        cache_hits += 1
                    results.append(enriched)
                    async with progress_lock:
                        await _emit_progress()
                except ProviderResponseError as exc:
                    message = _sanitize_error_message(str(exc))
                    provider_name = getattr(self.provider, "name", "unknown")
                    hint = _provider_hint(provider_name, message)
                    if exc.status_code:
                        provider_http_status = exc.status_code
                    if exc.payload:
                        provider_message = exc.payload.get("message") or exc.payload.get("error")
                    if exc.status_code == 429:
                        provider_limit_hit = True
                        provider_backoff_seconds = self._backoff_seconds(1)
                        storage.log_event(
                            "warning",
                            "provider_limit_hit",
                            {
                                "run_id": run_id,
                                "provider": provider_name,
                                "http_status": exc.status_code,
                                "message": provider_message or message,
                                "backoff_seconds": provider_backoff_seconds,
                            },
                        )
                    async with provider_error_lock:
                        provider_error_count += 1
                        if not provider_error:
                            provider_error.update(
                                {
                                    "provider": provider_name,
                                    "message": message,
                                    "hint": hint,
                                }
                            )
                        if not provider_error_logged:
                            storage.log_event(
                                "error",
                                "enrichment_provider_error",
                                {
                                    "run_id": run_id,
                                    "provider": provider_error.get("provider"),
                                    "error": message,
                                    "hint": hint,
                                },
                            )
                            if hint:
                                storage.log_event(
                                    "warning",
                                    "enrichment_provider_hint",
                                    {"run_id": run_id, "provider": provider_name, "hint": hint},
                                )
                            storage.record_error(run_id, "enriching", f"{message} {hint or ''}".strip())
                            provider_error_logged = True
                    stop_event.set()
                except Exception as exc:
                    error_count += 1
                    storage.log_event(
                        "error",
                        "enrichment_error",
                        {"cnpj": lead.get("cnpj"), "error": _sanitize_error_message(str(exc))},
                    )

        timeout = aiohttp.ClientTimeout(total=self.timeout + 2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [runner(lead) for lead in leads]
            await asyncio.gather(*tasks)

        avg_fetch_ms = int(sum(durations_ms) / len(durations_ms)) if durations_ms else 0
        processed_count = len(results)
        stats = {
            "provider_error": provider_error or None,
            "provider_error_count": provider_error_count,
            "skipped_due_to_provider_error": bool(stop_event.is_set()),
            "provider_limit_hit": provider_limit_hit,
            "provider_http_status": provider_http_status,
            "provider_message": provider_message,
            "provider_backoff_seconds": provider_backoff_seconds,
            "processed_count": processed_count,
            "errors_count": error_count,
            "cache_hits": cache_hits,
            "avg_fetch_ms": avg_fetch_ms,
        }
        storage.log_event(
            "info",
            "enrichment_stats",
            {
                "run_id": run_id,
                "processed_count": processed_count,
                "errors_count": error_count,
                "cache_hits": cache_hits,
                "avg_fetch_ms": avg_fetch_ms,
            },
        )
        return results, stats
