"""Async enrichment pipeline."""

import asyncio
import hashlib
import json
import os
import random
import re
import socket
import time
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from modules import storage, providers
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


_UA = UserAgent()
_FALLBACK_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def get_stealth_headers() -> Dict[str, str]:
    try:
        ua = _UA.random
    except Exception:
        ua = random.choice(_FALLBACK_UAS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


GENERIC_EMAIL_DOMAINS = {
    "gmail.com",
    "hotmail.com",
    "outlook.com",
    "yahoo.com",
    "bol.com.br",
    "uol.com.br",
    "icloud.com",
    "live.com",
}


class AdaptiveLimiter:
    def __init__(self, concurrency: int):
        self._limit = max(1, int(concurrency))
        self._semaphore = asyncio.Semaphore(self._limit)
        self._lock = asyncio.Lock()
        self._pause_until = 0.0

    async def acquire(self) -> None:
        while True:
            now = time.monotonic()
            if now < self._pause_until:
                await asyncio.sleep(self._pause_until - now)
                continue
            await self._semaphore.acquire()
            return

    def release(self) -> None:
        self._semaphore.release()

    async def reduce(self) -> None:
        async with self._lock:
            self._pause_until = max(self._pause_until, time.monotonic() + 60)
            if self._limit > 1:
                self._limit -= 1
                asyncio.create_task(self._semaphore.acquire())


def _email_domain(email: Optional[str]) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@")[-1].strip().lower()


def _is_generic_email(email: Optional[str]) -> bool:
    return _email_domain(email) in GENERIC_EMAIL_DOMAINS


async def _dns_valid(domain: str) -> bool:
    if not domain:
        return False
    try:
        await asyncio.get_running_loop().getaddrinfo(domain, None)
        return True
    except socket.gaierror:
        return False
    except Exception:
        return False


_COMPANY_STOPWORDS = {
    "ltda",
    "me",
    "mei",
    "eireli",
    "sa",
    "s/a",
    "da",
    "de",
    "do",
    "dos",
    "das",
    "e",
    "empresa",
}


def _normalize_company_text(text: str) -> List[str]:
    text = re.sub(r"[^a-z0-9 ]+", " ", (text or "").lower())
    return [tok for tok in text.split() if tok and tok not in _COMPANY_STOPWORDS]


def _title_similarity(title: str, company: str) -> float:
    if not title or not company:
        return 0.0
    title_tokens = _normalize_company_text(title)
    company_tokens = _normalize_company_text(company)
    if not title_tokens or not company_tokens:
        return 0.0
    token_overlap = len(set(title_tokens) & set(company_tokens)) / max(1, len(set(company_tokens)))
    seq_ratio = SequenceMatcher(None, " ".join(title_tokens), " ".join(company_tokens)).ratio()
    return max(token_overlap, seq_ratio)


async def _direct_fetch(
    session: aiohttp.ClientSession,
    domain: str,
    detector: TechSniperDetector,
) -> Optional[Dict[str, Any]]:
    candidates = [f"https://{domain}", f"http://{domain}"]
    if not domain.startswith("www."):
        candidates.append(f"https://www.{domain}")
    for url in candidates:
        start = time.time()
        try:
            async with session.get(url, headers=get_stealth_headers()) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text(errors="ignore")
                soup = BeautifulSoup(html, "html.parser")
                title = (soup.title.string or "").strip() if soup.title else ""
                meta_tag = soup.find("meta", attrs={"name": "description"}) or soup.find(
                    "meta", attrs={"property": "og:description"}
                )
                meta_desc = meta_tag.get("content", "").strip() if meta_tag else ""
                headers = {k.lower(): v for k, v in resp.headers.items()}
                cookies = [cookie.key for cookie in resp.cookies.values()]
                analysis = detector.analyze_content(html, headers, cookies)
                return {
                    "title": title,
                    "meta_description": meta_desc,
                    "analysis": analysis,
                    "fetch_status": resp.status,
                    "fetch_ms": int((time.time() - start) * 1000),
                    "fetched_url": str(resp.url),
                    "has_form": _has_form(html),
                    "has_whatsapp_link": analysis.get("has_whatsapp_link") or _has_whatsapp_link(html),
                }
        except (aiohttp.ClientError, asyncio.TimeoutError):
            continue
    return None


async def enrich_leads_hybrid(
    leads: List[Dict[str, Any]],
    provider: Optional[SearchProvider] = None,
    concurrency: int = 8,
    timeout: int = 5,
) -> List[Dict[str, Any]]:
    if provider is None:
        provider = providers.select_provider("serper")
    limiter = AdaptiveLimiter(concurrency)
    detector = TechSniperDetector(timeout=timeout)
    timeout_cfg = aiohttp.ClientTimeout(sock_connect=3, sock_read=5, total=max(8, timeout))

    async def _enrich_one(session: aiohttp.ClientSession, lead: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            "cnpj": lead.get("cnpj"),
            "run_id": lead.get("run_id"),
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
            "contact_quality": lead.get("contact_quality"),
            "notes": "",
        }
        try:
            emails = lead.get("emails_norm") or []
            email = emails[0] if emails else lead.get("email")
            domain = _email_domain(email)
            generic = _is_generic_email(email)
            dns_ok = await _dns_valid(domain) if domain else False

            title = ""
            meta_desc = ""
            direct_analysis: Dict[str, Any] = {}
            low_confidence_site = False

            if domain and dns_ok and not generic:
                direct = await _direct_fetch(session, domain, detector)
                if direct:
                    title = direct.get("title") or ""
                    meta_desc = direct.get("meta_description") or ""
                    direct_analysis = direct.get("analysis") or {}
                    result["fetched_url"] = direct.get("fetched_url")
                    result["fetch_status"] = direct.get("fetch_status")
                    result["fetch_ms"] = direct.get("fetch_ms")
                    result["has_form"] = bool(direct.get("has_form"))
                    result["site"] = result["fetched_url"]
                    similarity = _title_similarity(
                        title,
                        lead.get("razao_social") or lead.get("nome_fantasia") or "",
                    )
                    low_confidence_site = similarity < 0.3
                    if low_confidence_site:
                        result["notes"] = "low_confidence_site"

            needs_fallback = generic or low_confidence_site or not result.get("site")
            if needs_fallback:
                query = f"{lead.get('razao_social', '')} {lead.get('municipio', '')} {lead.get('uf', '')}".strip()
                try:
                    await limiter.acquire()
                    search_data = await provider.search(session, query)
                except ProviderResponseError as exc:
                    if exc.status_code == 429:
                        await limiter.reduce()
                    result["notes"] = _sanitize_error_message(str(exc))
                    search_data = {}
                except Exception as exc:
                    result["notes"] = _sanitize_error_message(str(exc))
                    search_data = {}
                finally:
                    limiter.release()

                if search_data:
                    result["site"] = search_data.get("site")
                    result["instagram"] = search_data.get("instagram")
                    result["linkedin_company"] = search_data.get("linkedin_company")
                    result["linkedin_people"] = search_data.get("linkedin_people", [])

            if direct_analysis:
                result["tech_score"] = direct_analysis.get("tech_score", 0)
                result["tech_confidence"] = direct_analysis.get("confidence", 0)
                result["has_marketing"] = direct_analysis.get("has_marketing", False)
                result["has_analytics"] = direct_analysis.get("has_analytics", False)
                result["has_ecommerce"] = direct_analysis.get("has_ecommerce", False)
                result["has_chat"] = direct_analysis.get("has_chat", False)
                result["signals"] = direct_analysis.get("signals", {})
                result["tech_stack"] = {
                    "detected_stack": direct_analysis.get("detected_stack", []),
                    "has_whatsapp_link": direct_analysis.get("has_whatsapp_link", False),
                }
            elif result.get("site"):
                detection = await detector.detect(result["site"], session, return_html=False)
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
                result["tech_stack"] = {
                    "detected_stack": detection.get("detected_stack", []),
                    "has_whatsapp_link": detection.get("has_whatsapp_link", False),
                }

            if title or meta_desc or low_confidence_site:
                result["signals"] = result.get("signals") or {}
                result["signals"].update(
                    {
                        "site_title": title,
                        "site_meta_description": meta_desc,
                        "low_confidence_site": low_confidence_site,
                    }
                )
        except Exception as exc:
            result["notes"] = _sanitize_error_message(str(exc))

        return result

    async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
        tasks = [_enrich_one(session, lead) for lead in leads]
        return await asyncio.gather(*tasks)
