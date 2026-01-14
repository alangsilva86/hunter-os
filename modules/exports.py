"""Webhook export helpers for Hunter OS."""

import json
import logging
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import requests

from modules import storage

logger = logging.getLogger("hunter")


def _chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def send_batch_to_webhook(
    leads: List[Dict[str, Any]],
    url: str,
    batch_size: int = 75,
    timeout: int = 15,
) -> Dict[str, Any]:
    if not url:
        raise RuntimeError("Webhook URL nao configurada")
    if not leads:
        return {"sent": 0, "failed": 0, "batches": 0}

    run_id = next((lead.get("run_id") for lead in leads if lead.get("run_id")), None)
    results = {"sent": 0, "failed": 0, "batches": 0}

    for batch in _chunked(leads, batch_size):
        payload = {
            "batch_id": uuid4().hex,
            "sent_at": storage._utcnow(),
            "count": len(batch),
            "run_id": run_id,
            "leads": batch,
        }
        try:
            body = json.dumps(payload, ensure_ascii=False, default=str)
            resp = requests.post(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=timeout,
            )
            ok = 200 <= resp.status_code < 300
            status = "success" if ok else "error"
            response_code = resp.status_code
        except Exception as exc:
            logger.warning("Webhook batch failed: %s", exc)
            status = "error"
            response_code = None

        for lead in batch:
            storage.record_webhook_delivery(
                run_id=run_id,
                lead_cnpj=lead.get("cnpj"),
                status=status,
                response_code=response_code,
            )

        results["batches"] += 1
        if status == "success":
            results["sent"] += len(batch)
        else:
            results["failed"] += len(batch)

    return results
