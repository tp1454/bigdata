"""Minimal SEC HTTP client with the fair-access User-Agent and rate limit."""

import time

import requests

MIN_INTERVAL = 0.15  # ~6.6 req/s, safely under SEC's 10 req/s limit


class SECClient:
    def __init__(self, email=""):
        contact = f" ({email})" if email else ""
        self.headers = {"User-Agent": f"FinancialApp/1.0{contact}"}
        self._last_request = 0.0

    def get(self, url):
        for _ in range(3):
            elapsed = time.time() - self._last_request
            if elapsed < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - elapsed)
            resp = requests.get(url, headers=self.headers, timeout=30)
            self._last_request = time.time()
            if resp.status_code == 429:
                time.sleep(10)
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(f"SEC rate limit kept rejecting {url}")

    def json(self, url):
        return self.get(url).json()

    def text(self, url):
        return self.get(url).text
