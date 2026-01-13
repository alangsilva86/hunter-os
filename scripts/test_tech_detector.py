import asyncio
import os

import aiohttp

from modules.tech_detection import TechSniperDetector


TEST_URLS = [
    "https://www.rdstation.com.br",
    "https://www.nubank.com.br",
    "https://www.lojaintegrada.com.br",
    "https://www.shopify.com.br",
    "https://www.vtex.com",
]


async def main() -> None:
    timeout = int(os.getenv("TIMEOUT", "5"))
    detector = TechSniperDetector(timeout=timeout)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout + 2)) as session:
        for url in TEST_URLS:
            try:
                result = await detector.detect(url, session)
                print("=" * 80)
                print(url)
                print("tech_score:", result.get("tech_score"))
                print("confidence:", result.get("confidence"))
                print("detected_stack:", result.get("detected_stack"))
                print("signals:", result.get("signals"))
            except Exception as exc:
                print("=" * 80)
                print(url)
                print("error:", exc)


if __name__ == "__main__":
    asyncio.run(main())
