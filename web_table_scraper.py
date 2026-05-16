# Web-Table-Scraper
# Author: C. Yildiz


"""
Advanced Web Table Scraper
==========================

Single-file version.

Features
--------
- Scrape HTML tables from URL or raw HTML
- Discover available tables before scraping
- Clean and normalize DataFrames
- Optional Selenium fallback for JavaScript-heavy pages
- Optional robots.txt check with in-memory cache
- Optional HTML file cache
- Optional async/concurrent scraping for multiple URLs
- European/Turkish and English numeric conversion support
- Structured error messages
- Logging
- Excel output
- argparse CLI

Install
-------
Required:
    pip install pandas beautifulsoup4 lxml requests openpyxl

Optional:
    pip install cloudscraper selenium httpx xlsxwriter
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import tempfile
import threading
import importlib.util
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

try:
    import cloudscraper
except ImportError:  # pragma: no cover
    cloudscraper = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None

try:
    import typer
except ImportError:  # pragma: no cover
    typer = None

import pandas as pd
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.common.exceptions import WebDriverException
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    HAVE_SELENIUM = True
except ImportError:  # pragma: no cover
    HAVE_SELENIUM = False


__version__ = "0.4.0"

LOGGER_NAME = "advanced_web_table_scraper"

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
    "Connection": "keep-alive",
}

EMPTY_VALUES = {
    "",
    "-",
    "—",
    "–",
    "--",
    "---",
    "nan",
    "NaN",
    "None",
    "none",
    "null",
    "NULL",
    "N/A",
    "n/a",
    "NA",
    "na",
}

CURRENCY_PATTERN = re.compile(r"[$€£¥₹₺₩₽]")
WHITESPACE_PATTERN = re.compile(r"\s+")
NON_SNAKE_PATTERN = re.compile(r"[^0-9a-zA-Z]+")
UNNAMED_COLUMN_PATTERN = re.compile(r"^Unnamed", flags=re.IGNORECASE)


# ============================================================
# ERRORS
# ============================================================

@dataclass(frozen=True)
class ScraperErrorInfo:
    """User-facing structured error information."""

    code: str
    message: str
    detail: str = ""
    suggestion: str = ""


class ScraperError(Exception):
    """Custom scraper exception with structured user-facing information."""

    def __init__(
        self,
        code: str,
        message: str,
        detail: str = "",
        suggestion: str = "",
        original_error: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.info = ScraperErrorInfo(
            code=code,
            message=message,
            detail=detail,
            suggestion=suggestion,
        )
        self.original_error = original_error


def format_error(error: BaseException) -> str:
    """Format errors in a user-friendly structured way."""

    if isinstance(error, ScraperError):
        parts = [
            f"Hata kodu: {error.info.code}",
            f"Mesaj: {error.info.message}",
        ]

        if error.info.detail:
            parts.append(f"Detay: {error.info.detail}")

        if error.info.suggestion:
            parts.append(f"Öneri: {error.info.suggestion}")

        if error.original_error is not None:
            parts.append(
                f"Teknik detay: {type(error.original_error).__name__}: {error.original_error}"
            )

        return "\n".join(parts)

    return f"Beklenmeyen hata: {type(error).__name__}: {error}"


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass(frozen=True)
class TableValidationRules:
    """Rules used to decide whether a scraped table is usable."""

    min_rows: int = 1
    min_columns: int = 1
    required_columns: tuple[str, ...] = ()
    max_null_ratio: float = 0.95

    def __post_init__(self) -> None:
        if self.min_rows < 0:
            raise ValueError("min_rows must be >= 0")
        if self.min_columns < 0:
            raise ValueError("min_columns must be >= 0")
        if not 0 <= self.max_null_ratio <= 1:
            raise ValueError("max_null_ratio must be between 0 and 1")


@dataclass(frozen=True)
class ScraperConfig:
    """Global scraper configuration."""

    timeout: int = 60
    wait_time: int = 10

    max_retries: int = 3
    retry_delay: float = 3.0
    retry_backoff: float = 1.5
    request_delay: float = 0.0

    respect_robots_txt: bool = False
    robots_cache_seconds: int = 3600

    cache_enabled: bool = False
    cache_dir: str = ".wts_cache"
    cache_ttl_seconds: int = 900

    use_selenium_first: bool = False
    use_selenium_fallback: bool = True
    selenium_headless: bool = True
    selenium_extra_wait: float = 0.5
    selenium_window_size: tuple[int, int] = (1920, 1080)
    selenium_driver_path: Optional[str] = None
    selenium_pool_size: int = 1

    async_concurrency: int = 5

    numeric_conversion_threshold: float = 0.85
    percent_as_decimal: bool = False
    number_locale: str = "auto"  # "auto", "en", "tr", "eu"

    clean_column_names: bool = True
    snake_case_columns: bool = False
    drop_empty_rows: bool = True
    drop_empty_columns: bool = True

    read_html_header: int | list[int] | None = 0
    read_html_displayed_only: bool = False
    thousands: str | None = None
    decimal: str | None = None

    parser: str = "lxml"
    log_level: str = "INFO"

    max_table_cells_warning: int = 2_000_000
    max_discovery_tables: Optional[int] = None

    headers: Mapping[str, str] = field(default_factory=lambda: DEFAULT_HEADERS.copy())

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        if self.wait_time < 0:
            raise ValueError("wait_time must be >= 0")
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")
        if self.retry_delay < 0:
            raise ValueError("retry_delay must be >= 0")
        if self.retry_backoff < 1:
            raise ValueError("retry_backoff must be >= 1")
        if self.request_delay < 0:
            raise ValueError("request_delay must be >= 0")
        if self.robots_cache_seconds < 0:
            raise ValueError("robots_cache_seconds must be >= 0")
        if self.cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds must be >= 0")
        if self.selenium_pool_size < 1:
            raise ValueError("selenium_pool_size must be >= 1")
        if self.async_concurrency < 1:
            raise ValueError("async_concurrency must be >= 1")
        if self.number_locale not in {"auto", "en", "tr", "eu"}:
            raise ValueError("number_locale must be one of: auto, en, tr, eu")
        if not 0 <= self.numeric_conversion_threshold <= 1:
            raise ValueError("numeric_conversion_threshold must be between 0 and 1")
        if not (
            isinstance(self.selenium_window_size, tuple)
            and len(self.selenium_window_size) == 2
            and all(isinstance(value, int) and value > 0 for value in self.selenium_window_size)
        ):
            raise ValueError("selenium_window_size must be a tuple of two positive integers")


@dataclass(frozen=True)
class ScrapedTable:
    """A scraped DataFrame with source metadata."""

    source: str
    table_index: int
    dataframe: pd.DataFrame

@dataclass(frozen=True)
class AsyncFetchResult:
    """One URL fetch result used by bulk async scraping."""

    url: str
    html: Optional[str] = None
    error: Optional[BaseException] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.html is not None


@dataclass(frozen=True)
class BulkScrapeResult:
    """One URL scrape result for robust batch scraping."""

    url: str
    dataframe: Optional[pd.DataFrame] = None
    error: Optional[BaseException] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.dataframe is not None

    @property
    def error_message(self) -> str:
        return "" if self.error is None else format_error(self.error)


# ============================================================
# LOGGING
# ============================================================

def setup_logger(log_level: str = "INFO") -> logging.Logger:
    """Create an idempotent logger without duplicate handlers."""

    logger = logging.getLogger(LOGGER_NAME)
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    logger.setLevel(numeric_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)

    for handler in logger.handlers:
        handler.setLevel(numeric_level)

    return logger


# ============================================================
# CACHE
# ============================================================

@dataclass(frozen=True)
class CacheMetadata:
    """Metadata stored next to cached HTML."""

    key: str
    url: str
    fetched_at: float
    status_code: Optional[int] = None
    headers: Mapping[str, str] = field(default_factory=dict)
    source: str = "http"

    @property
    def fetched_at_iso(self) -> str:
        return datetime.fromtimestamp(self.fetched_at).isoformat(timespec="seconds")


@dataclass(frozen=True)
class CacheEntry:
    """HTML cache entry with metadata."""

    html: str
    metadata: CacheMetadata

    def is_expired(self, ttl_seconds: int) -> bool:
        if ttl_seconds == 0:
            return False
        return (time.time() - self.metadata.fetched_at) > ttl_seconds


class FileCache:
    """
    File-based HTML cache with metadata and invalidation.

    The cache no longer stores only raw HTML. Each key is saved as a JSON file
    containing HTML plus status code, fetch timestamp, response headers and source.
    """

    def __init__(self, cache_dir: str | Path, ttl_seconds: int) -> None:
        self.cache_dir = Path(cache_dir)
        self.ttl_seconds = ttl_seconds
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def get_entry(self, key: str) -> Optional[CacheEntry]:
        path = self._key_to_path(key)

        if not path.exists():
            return None

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            metadata_raw = payload.get("metadata", {})
            entry = CacheEntry(
                html=str(payload.get("html", "")),
                metadata=CacheMetadata(
                    key=str(metadata_raw.get("key", key)),
                    url=str(metadata_raw.get("url", "")),
                    fetched_at=float(metadata_raw.get("fetched_at", path.stat().st_mtime)),
                    status_code=metadata_raw.get("status_code"),
                    headers=dict(metadata_raw.get("headers", {})),
                    source=str(metadata_raw.get("source", "unknown")),
                ),
            )
        except Exception:
            # Old/broken cache entries should not break scraping.
            self.invalidate(key)
            return None

        if not entry.html.strip():
            self.invalidate(key)
            return None

        if entry.is_expired(self.ttl_seconds):
            return None

        return entry

    def get(self, key: str) -> Optional[str]:
        entry = self.get_entry(key)
        return entry.html if entry is not None else None

    def set(
        self,
        key: str,
        value: str,
        *,
        url: str = "",
        status_code: Optional[int] = None,
        headers: Optional[Mapping[str, str]] = None,
        source: str = "http",
    ) -> None:
        path = self._key_to_path(key)
        metadata = {
            "key": key,
            "url": url,
            "fetched_at": time.time(),
            "status_code": status_code,
            "headers": dict(headers or {}),
            "source": source,
        }
        payload = {
            "metadata": metadata,
            "html": value,
        }

        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self.cache_dir,
            delete=False,
            suffix=".tmp",
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False)
            tmp_path = Path(tmp.name)

        tmp_path.replace(path)

    def invalidate(self, key: str) -> bool:
        path = self._key_to_path(key)

        with contextlib.suppress(FileNotFoundError):
            path.unlink()
            return True

        return False

    def clear(self) -> int:
        count = 0

        for path in self.cache_dir.glob("*.json"):
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
                count += 1

        return count


# ============================================================
# ROBOTS.TXT CACHE
# ============================================================

@dataclass
class CachedRobots:
    parser: RobotFileParser
    fetched_at: float


class RobotsChecker:
    """robots.txt checker with in-memory cache."""

    def __init__(self, cache_seconds: int = 3600) -> None:
        self.cache_seconds = cache_seconds
        self._cache: dict[str, CachedRobots] = {}

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        robots_url = f"{base_url}/robots.txt"

        parser = self._get_parser(base_url=base_url, robots_url=robots_url)

        return parser.can_fetch(user_agent, url)

    def assert_can_fetch(self, url: str, user_agent: str = "*") -> None:
        if not self.can_fetch(url=url, user_agent=user_agent):
            raise ScraperError(
                code="ROBOTS_TXT_BLOCKED",
                message="robots.txt bu URL için scraping izni vermiyor.",
                detail=f"URL: {url}",
                suggestion="Bu siteyi scrape etme veya resmi/izinli veri kaynağı kullan.",
            )

    def _get_parser(self, base_url: str, robots_url: str) -> RobotFileParser:
        cached = self._cache.get(base_url)

        if cached is not None:
            age = time.time() - cached.fetched_at

            if self.cache_seconds == 0 or age <= self.cache_seconds:
                return cached.parser

        parser = RobotFileParser()
        parser.set_url(robots_url)

        try:
            parser.read()
        except Exception as exc:
            raise ScraperError(
                code="ROBOTS_TXT_CHECK_FAILED",
                message="robots.txt kontrolü yapılamadı.",
                detail=f"robots.txt URL: {robots_url}",
                suggestion="--robots kullanmadan çalıştırabilir veya siteyi manuel kontrol edebilirsin.",
                original_error=exc,
            ) from exc

        self._cache[base_url] = CachedRobots(
            parser=parser,
            fetched_at=time.time(),
        )

        return parser


# ============================================================
# FETCHERS
# ============================================================

class SeleniumBrowserPool:
    """
    Small reusable Selenium browser pool.

    This is intentionally conservative. For a single URL it behaves like normal
    Selenium. For heavier systems, the pool can be increased, but concurrent
    browser automation is resource-heavy and should be sized carefully.

    Supports explicit cleanup and context-manager cleanup:
        with SeleniumBrowserPool(config, logger) as pool:
            with pool.acquire() as driver:
                ...
    """

    def __init__(self, config: ScraperConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self._drivers: list[Any] = []
        self._lock = threading.Lock()
        self._closed = False

    def __enter__(self) -> "SeleniumBrowserPool":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def _assert_open(self) -> None:
        if self._closed:
            raise ScraperError(
                code="SELENIUM_POOL_CLOSED",
                message="Selenium browser pool kapalı.",
                suggestion="Yeni bir scraper/pool oluştur veya close() sonrası tekrar kullanma.",
            )

    def _create_driver(self) -> Any:
        self._assert_open()

        if not HAVE_SELENIUM:
            raise ScraperError(
                code="SELENIUM_MISSING",
                message="Selenium kurulu değil.",
                suggestion="Kurulum: pip install selenium",
            )

        options = Options()

        if self.config.selenium_headless:
            options.add_argument("--headless=new")

        width, height = self.config.selenium_window_size
        options.add_argument(f"--window-size={width},{height}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        if self.config.selenium_driver_path:
            service = Service(executable_path=self.config.selenium_driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)

        driver.set_page_load_timeout(self.config.timeout)
        return driver

    @contextlib.contextmanager
    def acquire(self) -> Any:
        self._assert_open()
        driver = None

        with self._lock:
            if self._drivers:
                driver = self._drivers.pop()

        if driver is None:
            driver = self._create_driver()

        try:
            yield driver

        except Exception:
            with contextlib.suppress(Exception):
                driver.quit()
            raise

        else:
            with self._lock:
                if len(self._drivers) < self.config.selenium_pool_size:
                    self._drivers.append(driver)
                    driver = None

            if driver is not None:
                with contextlib.suppress(Exception):
                    driver.quit()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            drivers = list(self._drivers)
            self._drivers.clear()

        for driver in drivers:
            with contextlib.suppress(Exception):
                driver.quit()


class HtmlFetcher:
    """HTTP/Selenium HTML fetcher with retries, cache, robots check."""

    def __init__(
        self,
        config: ScraperConfig,
        logger: logging.Logger,
        robots_checker: Optional[RobotsChecker] = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.scraper = cloudscraper.create_scraper() if cloudscraper is not None else None
        self.robots_checker = robots_checker or RobotsChecker(config.robots_cache_seconds)
        self._last_fetch_metadata: dict[str, Any] = {}
        self.selenium_pool = SeleniumBrowserPool(config=config, logger=logger) if HAVE_SELENIUM else None

        self.cache = (
            FileCache(config.cache_dir, config.cache_ttl_seconds)
            if config.cache_enabled
            else None
        )

    def fetch(self, url: str, css_selector: str = "table") -> str:
        """Fetch URL HTML."""

        if self.config.respect_robots_txt:
            user_agent = dict(self.config.headers).get("User-Agent", "*")
            self.robots_checker.assert_can_fetch(url=url, user_agent=user_agent)

        cache_key = f"GET:{url}"

        if self.cache is not None:
            cached_entry = self.cache.get_entry(cache_key)

            if cached_entry is not None:
                self.logger.info(
                    "Cache hit: %s | fetched_at=%s | status=%s",
                    url,
                    cached_entry.metadata.fetched_at_iso,
                    cached_entry.metadata.status_code,
                )
                self._last_fetch_metadata = {
                    "url": url,
                    "status_code": cached_entry.metadata.status_code,
                    "headers": dict(cached_entry.metadata.headers),
                    "source": "cache",
                }
                return cached_entry.html

        if self.config.request_delay:
            self.logger.info("Sleeping %.2f seconds before request", self.config.request_delay)
            time.sleep(self.config.request_delay)

        if self.config.use_selenium_first:
            html = self._fetch_with_selenium(url=url, css_selector=css_selector)
        else:
            html = self._fetch_with_retries(url=url, css_selector=css_selector)

        if self.cache is not None:
            self.cache.set(
                cache_key,
                html,
                url=url,
                status_code=self._last_fetch_metadata.get("status_code"),
                headers=self._last_fetch_metadata.get("headers"),
                source=self._last_fetch_metadata.get("source", "http"),
            )

        return html

    def _fetch_with_retries(self, url: str, css_selector: str) -> str:
        last_error: Optional[BaseException] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                return self._fetch_with_http_client(url)

            except KeyboardInterrupt:
                raise

            except Exception as exc:
                last_error = exc

                self.logger.warning(
                    "Fetch attempt %s/%s failed for %s: %s",
                    attempt,
                    self.config.max_retries,
                    url,
                    exc,
                )

                if attempt < self.config.max_retries:
                    delay = self.config.retry_delay * (
                        self.config.retry_backoff ** (attempt - 1)
                    )
                    self.logger.info("Retrying after %.2f seconds", delay)
                    time.sleep(delay)

        if self.config.use_selenium_fallback:
            if HAVE_SELENIUM:
                self.logger.info("Trying Selenium fallback for %s", url)
                return self._fetch_with_selenium(url=url, css_selector=css_selector)

            self.logger.warning("Selenium fallback requested but Selenium is not installed.")

        raise ScraperError(
            code="FETCH_FAILED",
            message=f"HTML çekilemedi: {url}",
            detail="HTTP istemcisi başarısız oldu ve Selenium fallback kullanılamadı.",
            suggestion="URL erişilebilir mi kontrol et. JS tablosu varsa selenium kur.",
            original_error=last_error,
        )

    def _fetch_with_http_client(self, url: str) -> str:
        headers = dict(self.config.headers)

        if self.scraper is not None:
            self.logger.info("Fetching with cloudscraper: %s", url)
            response = self.scraper.get(
                url,
                headers=headers,
                timeout=self.config.timeout,
            )

        else:
            if requests is None:
                raise ScraperError(
                    code="HTTP_CLIENT_MISSING",
                    message="Ne cloudscraper ne de requests kurulu.",
                    suggestion="Kurulum: pip install requests cloudscraper",
                )

            self.logger.info("Fetching with requests: %s", url)
            response = requests.get(
                url,
                headers=headers,
                timeout=self.config.timeout,
            )

        response.raise_for_status()
        self._last_fetch_metadata = {
            "url": url,
            "status_code": int(response.status_code),
            "headers": dict(response.headers),
            "source": "http",
        }
        html = response.text

        if not html.strip():
            raise ScraperError(
                code="EMPTY_RESPONSE",
                message="Boş HTML cevabı alındı.",
                detail=f"URL: {url}",
            )

        return html

    def _fetch_with_selenium(self, url: str, css_selector: str = "table") -> str:
        if not HAVE_SELENIUM:
            raise ScraperError(
                code="SELENIUM_MISSING",
                message="Selenium kurulu değil.",
                suggestion="Kurulum: pip install selenium",
            )

        self.logger.info("Fetching with Selenium: %s", url)

        try:
            if self.selenium_pool is None:
                self.selenium_pool = SeleniumBrowserPool(config=self.config, logger=self.logger)

            with self.selenium_pool.acquire() as driver:
                driver.get(url)

                if self.config.wait_time:
                    try:
                        WebDriverWait(driver, self.config.wait_time).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, css_selector or "table")
                            )
                        )
                    except Exception:
                        self.logger.warning(
                            "Selenium wait finished without detecting selector: %s",
                            css_selector,
                        )

                if self.config.selenium_extra_wait:
                    time.sleep(self.config.selenium_extra_wait)

                html = driver.page_source

            self._last_fetch_metadata = {
                "url": url,
                "status_code": None,
                "headers": {},
                "source": "selenium",
            }

            if not html.strip():
                raise ScraperError(
                    code="EMPTY_SELENIUM_RESPONSE",
                    message="Selenium boş HTML döndürdü.",
                    detail=f"URL: {url}",
                )

            return html

        except WebDriverException as exc:
            raise ScraperError(
                code="SELENIUM_DRIVER_ERROR",
                message="Selenium ChromeDriver başlatılamadı veya sayfa yüklenemedi.",
                detail="Chrome/ChromeDriver sürüm uyumsuzluğu veya eksik driver olabilir.",
                suggestion="Selenium 4.6+ kullan veya --selenium-driver-path belirt.",
                original_error=exc,
            ) from exc

    def close(self) -> None:
        if self.selenium_pool is not None:
            self.selenium_pool.close()


class AsyncHtmlFetcher:
    """Async HTTP fetcher for many URLs. Selenium is intentionally not used here."""

    def __init__(
        self,
        config: ScraperConfig,
        logger: logging.Logger,
        robots_checker: Optional[RobotsChecker] = None,
    ) -> None:
        if httpx is None:
            raise ScraperError(
                code="HTTPX_MISSING",
                message="Async scraping için httpx kurulu olmalı.",
                suggestion="Kurulum: pip install httpx",
            )

        self.config = config
        self.logger = logger
        self.robots_checker = robots_checker or RobotsChecker(config.robots_cache_seconds)
        self.cache = (
            FileCache(config.cache_dir, config.cache_ttl_seconds)
            if config.cache_enabled
            else None
        )

    async def fetch_many(self, urls: list[str]) -> dict[str, AsyncFetchResult]:
        """
        Fetch many URLs without failing the whole batch when one URL fails.

        Each URL returns an AsyncFetchResult. This is safer for batch jobs than
        a plain gather(), because one broken URL no longer cancels all results.
        """

        limits = httpx.Limits(max_connections=self.config.async_concurrency)
        timeout = httpx.Timeout(self.config.timeout)
        semaphore = asyncio.Semaphore(self.config.async_concurrency)

        async with httpx.AsyncClient(
            headers=dict(self.config.headers),
            timeout=timeout,
            limits=limits,
            follow_redirects=True,
        ) as client:
            tasks = [
                self._fetch_one(client=client, semaphore=semaphore, url=url)
                for url in urls
            ]

            gathered = await asyncio.gather(*tasks, return_exceptions=True)

        results: dict[str, AsyncFetchResult] = {}

        for url, item in zip(urls, gathered):
            if isinstance(item, BaseException):
                results[url] = AsyncFetchResult(url=url, error=item)
            else:
                fetched_url, html = item
                results[fetched_url] = AsyncFetchResult(url=fetched_url, html=html)

        return results

    async def _fetch_one(
        self,
        client: "httpx.AsyncClient",
        semaphore: asyncio.Semaphore,
        url: str,
    ) -> tuple[str, str]:
        async with semaphore:
            user_agent = dict(self.config.headers).get("User-Agent", "*")

            if self.config.respect_robots_txt:
                # RobotFileParser is sync; run in a worker thread to keep async callers safe.
                await asyncio.to_thread(
                    self.robots_checker.assert_can_fetch,
                    url,
                    user_agent,
                )

            cache_key = f"GET:{url}"

            if self.cache is not None:
                cached_entry = self.cache.get_entry(cache_key)

                if cached_entry is not None:
                    self.logger.info(
                        "Async cache hit: %s | fetched_at=%s | status=%s",
                        url,
                        cached_entry.metadata.fetched_at_iso,
                        cached_entry.metadata.status_code,
                    )
                    return url, cached_entry.html

            if self.config.request_delay:
                await asyncio.sleep(self.config.request_delay)

            self.logger.info("Async fetching: %s", url)
            response = await client.get(url)
            response.raise_for_status()

            html = response.text

            if not html.strip():
                raise ScraperError(
                    code="EMPTY_RESPONSE",
                    message="Boş HTML cevabı alındı.",
                    detail=f"URL: {url}",
                )

            if self.cache is not None:
                self.cache.set(
                    cache_key,
                    html,
                    url=url,
                    status_code=int(response.status_code),
                    headers=dict(response.headers),
                    source="async_http",
                )

            return url, html


def run_coroutine_safely(coro: Any) -> Any:
    """
    Run a coroutine from sync code without calling asyncio.run() inside
    the currently running event loop.

    - Normal scripts: uses asyncio.run().
    - Jupyter/FastAPI-like running-loop contexts: runs the coroutine in a
      separate thread with its own event loop.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def worker() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover
            error["error"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join()

    if "error" in error:
        raise error["error"]

    return result.get("value")


# ============================================================
# CLEANER
# ============================================================

class DataFrameCleaner:
    """Clean and normalize scraped pandas DataFrames."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        out = df.copy()

        if self.config.clean_column_names:
            out.columns = self.clean_columns(
                out.columns,
                snake_case=self.config.snake_case_columns,
            )

        out = self.remove_unnamed_columns(out)
        out = self.deduplicate_columns(out)
        out = self.strip_text_cells(out)

        if self.config.drop_empty_rows or self.config.drop_empty_columns:
            out = self.drop_empty_rows_and_columns(out)

        out = self.convert_numeric_like_columns(out)

        if self.config.drop_empty_rows or self.config.drop_empty_columns:
            out = self.drop_empty_rows_and_columns(out)

        return out.reset_index(drop=True)

    @classmethod
    def clean_columns(cls, columns: Any, snake_case: bool = False) -> list[str]:
        cleaned: list[str] = []

        for col in columns:
            if isinstance(col, tuple):
                parts = [
                    str(part).strip()
                    for part in col
                    if str(part).strip()
                    and not str(part).strip().lower().startswith("unnamed")
                ]
                name = " ".join(parts)
            else:
                name = str(col).strip()

            name = WHITESPACE_PATTERN.sub(" ", name)

            if snake_case:
                name = cls.to_snake_case(name)

            cleaned.append(name or "column")

        return cleaned

    @staticmethod
    def to_snake_case(text: str) -> str:
        text = NON_SNAKE_PATTERN.sub("_", text.strip())
        text = re.sub(r"_+", "_", text)
        text = text.strip("_")

        return text.lower() or "column"

    @staticmethod
    def remove_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
        mask = ~pd.Series(df.columns).astype(str).str.match(
            UNNAMED_COLUMN_PATTERN,
            na=False,
        ).to_numpy()

        return df.loc[:, mask].copy()

    @staticmethod
    def deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
        columns: list[str] = []
        counts: dict[str, int] = {}

        for col in df.columns:
            base = str(col).strip() or "column"
            counts[base] = counts.get(base, 0) + 1
            columns.append(base if counts[base] == 1 else f"{base}_{counts[base]}")

        out = df.copy()
        out.columns = columns

        return out

    @staticmethod
    def strip_text_cells(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        empty_value_map = {value: pd.NA for value in EMPTY_VALUES}

        for col in out.columns:
            if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
                out[col] = (
                    out[col]
                    .astype("string")
                    .str.replace(r"\s+", " ", regex=True)
                    .str.strip()
                    .replace(empty_value_map)
                )

        return out

    def drop_empty_rows_and_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        if self.config.drop_empty_rows:
            out = out.dropna(axis=0, how="all")

        if self.config.drop_empty_columns:
            out = out.dropna(axis=1, how="all")

        return out

    def convert_numeric_like_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        for col in out.columns:
            if not (
                pd.api.types.is_object_dtype(out[col])
                or pd.api.types.is_string_dtype(out[col])
            ):
                continue

            series = out[col].astype("string")
            non_missing = int(series.notna().sum())

            if non_missing == 0:
                continue

            converted = self._convert_series_by_locale(series)
            numeric_share = converted.notna().sum() / non_missing

            if numeric_share >= self.config.numeric_conversion_threshold:
                out[col] = converted

        return out

    def _convert_series_by_locale(self, series: pd.Series) -> pd.Series:
        has_percent = series.dropna().str.contains("%", regex=False).any()

        candidate = (
            series
            .str.replace(CURRENCY_PATTERN, "", regex=True)
            .str.replace("%", "", regex=False)
            .str.replace(r"^\((.*)\)$", r"-\1", regex=True)
            .str.replace(r"^\+", "", regex=True)
            .str.replace("\u00a0", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.strip()
        )

        locale = self.config.number_locale

        if locale == "en":
            converted = self._parse_en(candidate)

        elif locale in {"tr", "eu"}:
            converted = self._parse_eu(candidate)

        else:
            converted = self._parse_auto(candidate)

        if has_percent and self.config.percent_as_decimal:
            converted = converted / 100

        return converted

    @staticmethod
    def _parse_en(series: pd.Series) -> pd.Series:
        # English-style:
        # 1,234.56 -> 1234.56
        candidate = series.str.replace(",", "", regex=False)
        return pd.to_numeric(candidate, errors="coerce")

    @staticmethod
    def _parse_eu(series: pd.Series) -> pd.Series:
        # European/Turkish-style:
        # 1.234,56 -> 1234.56
        candidate = (
            series
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        return pd.to_numeric(candidate, errors="coerce")

    @classmethod
    def _parse_auto(cls, series: pd.Series) -> pd.Series:
        """
        Safer auto mode.

        The old implementation chose the locale with more parsed values. That can
        silently misread ambiguous values such as "1.234" or "1,234". This version
        uses separator-pattern confidence first. If the column is genuinely
        ambiguous, it returns all-NA, so the original string column is preserved.
        """

        non_missing = series.dropna()

        if non_missing.empty:
            return pd.to_numeric(series, errors="coerce")

        en = cls._parse_en(series)
        eu = cls._parse_eu(series)

        # If all successfully parsed values are identical, no locale risk exists.
        comparable = en.notna() & eu.notna()

        if comparable.any() and (en[comparable].astype(float) == eu[comparable].astype(float)).all():
            return en

        en_score = cls._locale_confidence(non_missing, locale="en")
        eu_score = cls._locale_confidence(non_missing, locale="eu")
        ambiguous_count = cls._ambiguous_separator_count(non_missing)

        if en_score == 0 and eu_score == 0:
            return pd.to_numeric(series, errors="coerce")

        # If confidence is tied and there are ambiguous separators, do not guess.
        if en_score == eu_score and ambiguous_count > 0:
            return pd.Series(pd.NA, index=series.index, dtype="Float64")

        # If both are plausible and the margin is too small, do not guess.
        total = max(len(non_missing), 1)
        margin = abs(en_score - eu_score) / total

        if ambiguous_count > 0 and margin < 0.20:
            return pd.Series(pd.NA, index=series.index, dtype="Float64")

        return en if en_score > eu_score else eu

    @staticmethod
    def _locale_confidence(series: pd.Series, locale: str) -> int:
        score = 0

        for raw in series.astype(str):
            value = raw.strip()

            if not value:
                continue

            if locale == "en":
                if re.fullmatch(r"[+-]?\d{1,3}(,\d{3})+(\.\d+)?", value):
                    score += 3
                elif re.fullmatch(r"[+-]?\d+(\.\d+)?", value):
                    score += 1
                elif re.fullmatch(r"[+-]?\d+\.\d+", value):
                    score += 2

            else:
                if re.fullmatch(r"[+-]?\d{1,3}(\.\d{3})+(,\d+)?", value):
                    score += 3
                elif re.fullmatch(r"[+-]?\d+(,\d+)?", value):
                    score += 1
                elif re.fullmatch(r"[+-]?\d+,\d+", value):
                    score += 2

        return score

    @staticmethod
    def _ambiguous_separator_count(series: pd.Series) -> int:
        count = 0

        for raw in series.astype(str):
            value = raw.strip()

            # 1,234 and 1.234 are ambiguous without context:
            # either thousands or decimal notation.
            if re.fullmatch(r"[+-]?\d{1,3}[,.]\d{3}", value):
                count += 1

        return count


# ============================================================
# PARSER
# ============================================================

class TableParser:
    """Parse HTML table nodes into DataFrames."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config

    def find_table_nodes(self, html: str, css_selector: str = "table") -> list[Any]:
        soup = BeautifulSoup(html, self.config.parser)
        return list(soup.select(css_selector or "table"))

    def parse_table_html(self, table_html: str) -> pd.DataFrame:
        kwargs: dict[str, Any] = {
            "header": self.config.read_html_header,
            "displayed_only": self.config.read_html_displayed_only,
        }

        if self.config.thousands is not None:
            kwargs["thousands"] = self.config.thousands

        if self.config.decimal is not None:
            kwargs["decimal"] = self.config.decimal

        try:
            tables = pd.read_html(StringIO(table_html), **kwargs)

        except ValueError as exc:
            raise ScraperError(
                code="PANDAS_PARSE_FAILED",
                message="pandas bu tabloyu parse edemedi.",
                suggestion=(
                    "Tablo bozuk/karmaşık olabilir. Farklı selector dene veya "
                    "manual BeautifulSoup parser gerekebilir."
                ),
                original_error=exc,
            ) from exc

        if not tables:
            raise ScraperError(
                code="PANDAS_PARSE_EMPTY",
                message="pandas tablo döndürmedi.",
            )

        return tables[0]

    @staticmethod
    def normalize_table_indices(
        table_indices: Optional[Sequence[int]],
        table_count: int,
    ) -> list[int]:
        if table_indices is None:
            return list(range(table_count))

        selected = list(table_indices)

        if not selected:
            raise ScraperError(
                code="EMPTY_TABLE_INDICES",
                message="table_indices boş olamaz.",
            )

        normalized: list[int] = []

        for index in selected:
            if isinstance(index, bool) or not isinstance(index, int):
                raise ScraperError(
                    code="INVALID_TABLE_INDEX_TYPE",
                    message="Table index integer olmalı.",
                    detail=f"Given type: {type(index).__name__}",
                )

            if index < 0 or index >= table_count:
                raise ScraperError(
                    code="TABLE_INDEX_OUT_OF_RANGE",
                    message=f"Table index out of range: {index}",
                    detail=f"Found {table_count} table(s).",
                    suggestion=f"Geçerli aralık: 0 - {table_count - 1}",
                )

            normalized.append(index)

        return normalized

    @staticmethod
    def validate_dataframe(
        df: pd.DataFrame,
        rules: TableValidationRules,
        table_index: int,
        source: str,
    ) -> None:
        if df.shape[0] < rules.min_rows:
            raise ScraperError(
                code="TOO_FEW_ROWS",
                message=f"Table {table_index} has too few rows.",
                detail=f"{df.shape[0]} < {rules.min_rows}. Source: {source}",
            )

        if df.shape[1] < rules.min_columns:
            raise ScraperError(
                code="TOO_FEW_COLUMNS",
                message=f"Table {table_index} has too few columns.",
                detail=f"{df.shape[1]} < {rules.min_columns}. Source: {source}",
            )

        missing_columns = [
            column for column in rules.required_columns if column not in df.columns
        ]

        if missing_columns:
            raise ScraperError(
                code="MISSING_REQUIRED_COLUMNS",
                message=f"Table {table_index} is missing required columns.",
                detail=f"Missing columns: {missing_columns}. Source: {source}",
            )

        cell_count = df.shape[0] * df.shape[1]

        if cell_count:
            null_ratio = df.isna().sum().sum() / cell_count

            if null_ratio > rules.max_null_ratio:
                raise ScraperError(
                    code="TOO_MANY_NULLS",
                    message=f"Table {table_index} has too many missing values.",
                    detail=f"{null_ratio:.1%} > {rules.max_null_ratio:.1%}. Source: {source}",
                )


# ============================================================
# EXCEL
# ============================================================

def build_excel_output_path(url: str, table_index: int) -> Path:
    """Create a safe Excel output path on Desktop or home directory."""

    desktop = Path.home() / "Desktop"

    if not desktop.exists():
        desktop = Path.home()

    domain = urlparse(url).netloc.replace("www.", "") or "web_table"
    safe_domain = re.sub(r"[^0-9a-zA-Z_-]+", "_", domain).strip("_") or "web_table"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return desktop / f"scraped_table_{safe_domain}_table_{table_index}_{timestamp}.xlsx"


def choose_excel_engine() -> str:
    """Choose an installed Excel writer engine."""

    if importlib.util.find_spec("xlsxwriter") is not None:
        return "xlsxwriter"

    if importlib.util.find_spec("openpyxl") is not None:
        return "openpyxl"

    raise ScraperError(
        code="EXCEL_ENGINE_MISSING",
        message="Excel çıktısı için xlsxwriter veya openpyxl kurulu olmalı.",
        suggestion="Kurulum: pip install xlsxwriter openpyxl",
    )


def write_dataframe_to_excel(
    df: pd.DataFrame,
    output_path: Path,
    url: str,
    table_index: int,
    auto_refresh: bool = False,
) -> None:
    """Write DataFrame and metadata to Excel."""

    engine = choose_excel_engine()
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    metadata = pd.DataFrame(
        {
            "Field": [
                "Source URL",
                "Table index",
                "Scraped at",
                "Rows",
                "Columns",
                "Cells",
                "Mode",
            ],
            "Value": [
                url,
                table_index,
                scraped_at,
                len(df),
                len(df.columns),
                len(df) * len(df.columns),
                "Live / auto-refresh" if auto_refresh else "Static / one-time",
            ],
        }
    )

    with pd.ExcelWriter(output_path, engine=engine) as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
        metadata.to_excel(writer, sheet_name="Metadata", index=False)

        if engine == "xlsxwriter":
            _format_xlsxwriter(writer, df)

        elif engine == "openpyxl":
            _format_openpyxl(writer, df)


def _format_xlsxwriter(writer: pd.ExcelWriter, df: pd.DataFrame) -> None:
    workbook = writer.book
    data_sheet = writer.sheets["Data"]
    metadata_sheet = writer.sheets["Metadata"]

    header_format = workbook.add_format({"bold": True, "text_wrap": True})

    for col_num, col_name in enumerate(df.columns):
        data_sheet.write(0, col_num, col_name, header_format)

        series = df[col_name].astype("string")
        sample_values = series.dropna().head(100).tolist()
        max_len = max([len(str(col_name))] + [len(str(value)) for value in sample_values])

        data_sheet.set_column(col_num, col_num, min(max(max_len + 2, 10), 40))

    data_sheet.freeze_panes(1, 0)

    if df.shape[1] > 0:
        data_sheet.autofilter(0, 0, max(len(df), 1), max(len(df.columns) - 1, 0))

    metadata_sheet.set_column(0, 0, 18)
    metadata_sheet.set_column(1, 1, 90)


def _format_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame) -> None:
    data_sheet = writer.sheets["Data"]
    metadata_sheet = writer.sheets["Metadata"]

    data_sheet.freeze_panes = "A2"

    if df.shape[1] > 0:
        data_sheet.auto_filter.ref = data_sheet.dimensions

    for column_cells in data_sheet.columns:
        header = column_cells[0].value
        values = [cell.value for cell in column_cells[1:101]]
        max_len = max([len(str(header))] + [len(str(value)) for value in values if value is not None])
        data_sheet.column_dimensions[column_cells[0].column_letter].width = min(
            max(max_len + 2, 10),
            40,
        )

    metadata_sheet.column_dimensions["A"].width = 18
    metadata_sheet.column_dimensions["B"].width = 90


# ============================================================
# MAIN SCRAPER
# ============================================================

class WebTableDataFrameScraper:
    """Main scraper class."""

    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config or ScraperConfig()
        self.logger = logger or setup_logger(self.config.log_level)

        self.fetcher = HtmlFetcher(config=self.config, logger=self.logger)
        self.parser = TableParser(config=self.config)
        self.cleaner = DataFrameCleaner(config=self.config)
        self._closed = False

    def __enter__(self) -> "WebTableDataFrameScraper":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self.fetcher.close()
        self._closed = True

    def scrape_table(
        self,
        url: str,
        table_index: int = 0,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
    ) -> pd.DataFrame:
        """Scrape one table from a URL."""

        return self.scrape_tables(
            url=url,
            table_indices=[table_index],
            css_selector=css_selector,
            validation=validation,
            clean=clean,
        )[0]

    def scrape_tables(
        self,
        url: str,
        table_indices: Optional[Sequence[int]] = None,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
    ) -> list[pd.DataFrame]:
        """Scrape selected tables from a URL."""

        return [
            item.dataframe
            for item in self.scrape_tables_with_metadata(
                url=url,
                table_indices=table_indices,
                css_selector=css_selector,
                validation=validation,
                clean=clean,
            )
        ]

    def scrape_table_with_metadata(
        self,
        url: str,
        table_index: int = 0,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
    ) -> ScrapedTable:
        """Scrape one table from a URL with metadata."""

        return self.scrape_tables_with_metadata(
            url=url,
            table_indices=[table_index],
            css_selector=css_selector,
            validation=validation,
            clean=clean,
        )[0]

    def scrape_tables_with_metadata(
        self,
        url: str,
        table_indices: Optional[Sequence[int]] = None,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
    ) -> list[ScrapedTable]:
        """Scrape selected tables from a URL with metadata."""

        self._validate_url(url)
        html = self.fetcher.fetch(url=url, css_selector=css_selector)

        return self.parse_tables_from_html_with_metadata(
            html=html,
            source=url,
            table_indices=table_indices,
            css_selector=css_selector,
            validation=validation,
            clean=clean,
        )

    def parse_table_from_html(
        self,
        html: str,
        table_index: int = 0,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
        source: str = "raw_html",
    ) -> pd.DataFrame:
        """Parse one table from raw HTML."""

        return self.parse_tables_from_html(
            html=html,
            table_indices=[table_index],
            css_selector=css_selector,
            validation=validation,
            clean=clean,
            source=source,
        )[0]

    def parse_tables_from_html(
        self,
        html: str,
        table_indices: Optional[Sequence[int]] = None,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
        source: str = "raw_html",
    ) -> list[pd.DataFrame]:
        """Parse selected tables from raw HTML."""

        return [
            item.dataframe
            for item in self.parse_tables_from_html_with_metadata(
                html=html,
                table_indices=table_indices,
                css_selector=css_selector,
                validation=validation,
                clean=clean,
                source=source,
            )
        ]

    def parse_table_from_html_with_metadata(
        self,
        html: str,
        table_index: int = 0,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
        source: str = "raw_html",
    ) -> ScrapedTable:
        """Parse one table from raw HTML with metadata."""

        return self.parse_tables_from_html_with_metadata(
            html=html,
            table_indices=[table_index],
            css_selector=css_selector,
            validation=validation,
            clean=clean,
            source=source,
        )[0]

    def parse_tables_from_html_with_metadata(
        self,
        html: str,
        table_indices: Optional[Sequence[int]] = None,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
        source: str = "raw_html",
    ) -> list[ScrapedTable]:
        """Parse selected tables from raw HTML with metadata."""

        self._validate_html(html)
        self._validate_css_selector(css_selector)

        rules = validation or TableValidationRules()
        table_nodes = self.parser.find_table_nodes(html=html, css_selector=css_selector)

        if not table_nodes:
            raise ScraperError(
                code="NO_TABLES_FOUND",
                message=f"Tablo bulunamadı. Selector: {css_selector!r}",
                suggestion="Selector doğru mu kontrol et. JS ile yükleniyorsa Selenium kullan.",
            )

        selected_indices = self.parser.normalize_table_indices(
            table_indices=table_indices,
            table_count=len(table_nodes),
        )

        scraped_tables: list[ScrapedTable] = []

        for index in selected_indices:
            self.logger.info("Parsing table index %s from %s", index, source)

            df = self.parser.parse_table_html(str(table_nodes[index]))

            if clean:
                df = self.cleaner.clean(df)

            self._warn_if_large_dataframe(df=df, table_index=index, source=source)

            self.parser.validate_dataframe(
                df=df,
                rules=rules,
                table_index=index,
                source=source,
            )

            scraped_tables.append(
                ScrapedTable(
                    source=source,
                    table_index=index,
                    dataframe=df,
                )
            )

        return scraped_tables

    def discover_tables(
        self,
        url: str,
        css_selector: str = "table",
        preview_rows: int = 5,
        clean: bool = True,
    ) -> pd.DataFrame:
        """Discover available tables on a URL."""

        self._validate_url(url)
        html = self.fetcher.fetch(url=url, css_selector=css_selector)

        return self.discover_tables_from_html(
            html=html,
            css_selector=css_selector,
            preview_rows=preview_rows,
            clean=clean,
        )

    def discover_tables_from_html(
        self,
        html: str,
        css_selector: str = "table",
        preview_rows: int = 5,
        clean: bool = True,
    ) -> pd.DataFrame:
        """Discover available tables from raw HTML."""

        self._validate_html(html)
        self._validate_css_selector(css_selector)

        if preview_rows < 0:
            raise ValueError("preview_rows must be >= 0")

        table_nodes = self.parser.find_table_nodes(html=html, css_selector=css_selector)

        if self.config.max_discovery_tables is not None:
            table_nodes = table_nodes[: self.config.max_discovery_tables]

        records: list[dict[str, Any]] = []

        for index, node in enumerate(table_nodes):
            try:
                df = self.parser.parse_table_html(str(node))

                if clean:
                    df = self.cleaner.clean(df)

                records.append(
                    {
                        "table_index": index,
                        "rows": len(df),
                        "columns": len(df.columns),
                        "cell_count": int(len(df) * len(df.columns)),
                        "column_names": list(df.columns),
                        "preview": df.head(preview_rows).to_string(index=False),
                        "parse_error": None,
                    }
                )

            except Exception as exc:
                records.append(
                    {
                        "table_index": index,
                        "rows": None,
                        "columns": None,
                        "cell_count": None,
                        "column_names": [],
                        "preview": "",
                        "parse_error": str(exc),
                    }
                )

        return pd.DataFrame.from_records(records)

    async def async_scrape_many_urls(
        self,
        urls: list[str],
        table_index: int = 0,
        css_selector: str = "table",
        clean: bool = True,
    ) -> dict[str, BulkScrapeResult]:
        """
        Async/concurrent scraping for many URLs with per-URL result/error objects.

        Use this directly in Jupyter/FastAPI:
            results = await scraper.async_scrape_many_urls(urls)

        One failed URL does not destroy the whole batch. Check result.ok for each URL.
        Selenium is intentionally not used here; browser concurrency should go through
        a dedicated SeleniumBrowserPool.
        """

        output: dict[str, BulkScrapeResult] = {}
        valid_urls: list[str] = []

        for url in urls:
            try:
                self._validate_url(url)
                valid_urls.append(url)
            except BaseException as exc:
                output[url] = BulkScrapeResult(url=url, error=exc)

        if not valid_urls:
            return output

        fetcher = AsyncHtmlFetcher(config=self.config, logger=self.logger)
        fetch_results = await fetcher.fetch_many(valid_urls)

        for url, fetch_result in fetch_results.items():
            if not fetch_result.ok:
                output[url] = BulkScrapeResult(url=url, error=fetch_result.error)
                continue

            try:
                df = self.parse_table_from_html(
                    html=fetch_result.html or "",
                    table_index=table_index,
                    css_selector=css_selector,
                    clean=clean,
                    source=url,
                )
                output[url] = BulkScrapeResult(url=url, dataframe=df)

            except BaseException as exc:
                output[url] = BulkScrapeResult(url=url, error=exc)

        return output

    async def async_scrape_many_urls_strict(
        self,
        urls: list[str],
        table_index: int = 0,
        css_selector: str = "table",
        clean: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """
        Strict compatibility helper: raises if any URL fails.
        Prefer async_scrape_many_urls() for production batch jobs.
        """

        results = await self.async_scrape_many_urls(
            urls=urls,
            table_index=table_index,
            css_selector=css_selector,
            clean=clean,
        )

        errors = {url: result.error for url, result in results.items() if not result.ok}

        if errors:
            detail = "; ".join(
                f"{url}: {type(error).__name__}: {error}"
                for url, error in errors.items()
            )
            raise ScraperError(
                code="BULK_SCRAPE_PARTIAL_FAILURE",
                message="Toplu scraping sırasında en az bir URL başarısız oldu.",
                detail=detail,
                suggestion="Hata-toleranslı kullanım için async_scrape_many_urls() sonucunda result.ok kontrol et.",
            )

        return {url: result.dataframe for url, result in results.items() if result.dataframe is not None}

    def scrape_many_urls(
        self,
        urls: list[str],
        table_index: int = 0,
        css_selector: str = "table",
        clean: bool = True,
    ) -> dict[str, BulkScrapeResult]:
        """
        Sync wrapper around async_scrape_many_urls().

        Unlike the old version, this is event-loop safe and returns per-URL
        BulkScrapeResult objects instead of failing the entire batch.
        """

        return run_coroutine_safely(
            self.async_scrape_many_urls(
                urls=urls,
                table_index=table_index,
                css_selector=css_selector,
                clean=clean,
            )
        )

    def scrape_many_urls_strict(
        self,
        urls: list[str],
        table_index: int = 0,
        css_selector: str = "table",
        clean: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Strict sync compatibility helper: raises if any URL fails."""

        return run_coroutine_safely(
            self.async_scrape_many_urls_strict(
                urls=urls,
                table_index=table_index,
                css_selector=css_selector,
                clean=clean,
            )
        )

    def _warn_if_large_dataframe(
        self,
        df: pd.DataFrame,
        table_index: int,
        source: str,
    ) -> None:
        cell_count = int(df.shape[0] * df.shape[1])

        if cell_count >= self.config.max_table_cells_warning:
            self.logger.warning(
                "Large table warning: table_index=%s source=%s cells=%s. "
                "Memory/performance issues may occur.",
                table_index,
                source,
                cell_count,
            )

    @staticmethod
    def _validate_url(url: str) -> None:
        if not isinstance(url, str) or not url.strip():
            raise ScraperError(
                code="INVALID_URL",
                message="URL boş olamaz.",
            )

        parsed = urlparse(url.strip())

        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ScraperError(
                code="INVALID_URL",
                message="URL absolute http:// veya https:// URL olmalı.",
                detail=f"Invalid URL: {url!r}",
            )

    @staticmethod
    def _validate_html(html: str) -> None:
        if not isinstance(html, str) or not html.strip():
            raise ScraperError(
                code="INVALID_HTML",
                message="HTML boş olamaz.",
            )

    @staticmethod
    def _validate_css_selector(css_selector: str) -> None:
        if css_selector is None:
            raise ScraperError(
                code="INVALID_SELECTOR",
                message="css_selector None olamaz.",
            )

        if not isinstance(css_selector, str):
            raise ScraperError(
                code="INVALID_SELECTOR",
                message="css_selector string olmalı.",
                detail=f"Given type: {type(css_selector).__name__}",
            )

        if not css_selector.strip():
            raise ScraperError(
                code="INVALID_SELECTOR",
                message="css_selector boş olamaz.",
            )


# ============================================================
# DATAFRAME HELPERS
# ============================================================

def require_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Raise a clear error if required DataFrame columns are missing."""

    missing = [column for column in columns if column not in df.columns]

    if missing:
        raise ScraperError(
            code="MISSING_DATAFRAME_COLUMNS",
            message="Gerekli DataFrame kolonları eksik.",
            detail=f"Missing columns: {missing}",
        )


def with_columns(
    df: pd.DataFrame,
    **columns: Callable[[pd.DataFrame], Any],
) -> pd.DataFrame:
    """Return a copy of df with calculated columns added."""

    out = df.copy()

    for name, func in columns.items():
        out[name] = func(out)

    return out


def safe_with_columns(
    df: pd.DataFrame,
    required_columns: Sequence[str] = (),
    **columns: Callable[[pd.DataFrame], Any],
) -> pd.DataFrame:
    """Return a copy of df with calculated columns added after checking required columns."""

    require_columns(df, required_columns)

    return with_columns(df, **columns)



# ============================================================
# PRINT / INTERACTIVE HELPERS
# ============================================================

def print_section(title: str) -> None:
    """Print a clear section header."""

    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def print_table_discovery(discovery: pd.DataFrame) -> None:
    """Print all discovered tables with their preview rows."""

    print_section("BULUNAN TABLOLAR")

    for _, row in discovery.iterrows():
        print("\n" + "-" * 90)
        print(f"TABLE INDEX: {row['table_index']}")
        print(f"SATIR SAYISI: {row['rows']}")
        print(f"KOLON SAYISI: {row['columns']}")
        print(f"HÜCRE SAYISI: {row.get('cell_count', 'N/A')}")

        print("KOLONLAR / HEADERS:")
        for col_index, col_name in enumerate(row["column_names"]):
            print(f"  {col_index}: {col_name}")

        if row["parse_error"]:
            print(f"PARSE HATASI: {row['parse_error']}")
        else:
            print("\nÖNİZLEME:")
            print(row["preview"])

    print("\n" + "=" * 90)


def ask_table_index(valid_indices: set[int]) -> int:
    """Ask user which table_index should be scraped."""

    while True:
        selected = input("Çekmek istediğin table_index değerini yaz: ").strip()

        if not selected.isdigit():
            print("Lütfen sayı gir. Örnek: 0")
            continue

        table_index = int(selected)

        if table_index not in valid_indices:
            print(f"Geçersiz seçim. Seçilebilir indexler: {sorted(valid_indices)}")
            continue

        return table_index


def ask_yes_no(question: str, default: bool = False) -> bool:
    """Ask a yes/no question in terminal."""

    default_text = "E/h" if default else "e/H"

    while True:
        answer = input(f"{question} ({default_text}): ").strip().lower()

        if not answer:
            return default

        if answer in {"e", "evet", "y", "yes"}:
            return True

        if answer in {"h", "hayır", "hayir", "n", "no"}:
            return False

        print("Lütfen evet/hayır şeklinde cevap ver. Örnek: e veya h")


def print_current_data(df: pd.DataFrame, url: str, table_index: int, preview_rows: int) -> None:
    """Print the current scraped DataFrame and metadata."""

    print_section("GÜNCEL DATA")
    print(df.head(preview_rows).to_string(index=False))

    print_section("DATA BİLGİSİ")
    print(f"Kaynak link: {url}")
    print(f"Seçilen tablo index: {table_index}")
    print(f"Satır sayısı: {len(df)}")
    print(f"Kolon sayısı: {len(df.columns)}")
    print(f"Hücre sayısı: {len(df) * len(df.columns)}")

    print("Kolonlar / Headers:")
    for col_index, col_name in enumerate(df.columns):
        print(f"  {col_index}: {col_name}")


# ============================================================
# INTERACTIVE RUNNER:
# ============================================================

@dataclass
class RuntimeSettings:
    """Settings controlled from the SEÇİMLER section."""

    url: str
    css_selector: str = "table"
    preview_rows_for_selection: int = 4
    table_index: Optional[int] = None

    clean: bool = True
    clean_column_names: bool = True
    snake_case_columns: bool = False
    drop_empty_rows: bool = True
    drop_empty_columns: bool = True

    percent_as_decimal: bool = True
    numeric_conversion_threshold: float = 0.85
    number_locale: str = "auto"

    read_html_header: int | list[int] | None = 0
    read_html_displayed_only: bool = False
    thousands: str | None = None
    decimal: str | None = None

    timeout: int = 60
    max_retries: int = 3
    retry_delay: float = 3.0
    retry_backoff: float = 1.5
    request_delay: float = 0.0

    respect_robots_txt: bool = False
    robots_cache_seconds: int = 3600

    cache_enabled: bool = False
    cache_dir: str = ".wts_cache"
    cache_ttl_seconds: int = 900

    use_selenium_first: bool = False
    use_selenium_fallback: bool = True
    selenium_headless: bool = True
    selenium_driver_path: Optional[str] = None
    selenium_extra_wait: float = 0.5
    selenium_pool_size: int = 1
    wait_time: int = 10

    async_concurrency: int = 5

    refresh_seconds: Optional[int] = None
    auto_refresh: bool = False
    preview_rows: int = 10

    excel_output: Optional[bool] = None
    excel_path: Optional[Path] = None

    parser: str = "lxml"
    log_level: str = "INFO"

    max_table_cells_warning: int = 2_000_000
    max_discovery_tables: Optional[int] = None


def build_config(settings: RuntimeSettings) -> ScraperConfig:
    """Build ScraperConfig from RuntimeSettings."""

    return ScraperConfig(
        timeout=settings.timeout,
        wait_time=settings.wait_time,
        max_retries=settings.max_retries,
        retry_delay=settings.retry_delay,
        retry_backoff=settings.retry_backoff,
        request_delay=settings.request_delay,
        respect_robots_txt=settings.respect_robots_txt,
        robots_cache_seconds=settings.robots_cache_seconds,
        cache_enabled=settings.cache_enabled,
        cache_dir=settings.cache_dir,
        cache_ttl_seconds=settings.cache_ttl_seconds,
        use_selenium_first=settings.use_selenium_first,
        use_selenium_fallback=settings.use_selenium_fallback,
        selenium_headless=settings.selenium_headless,
        selenium_extra_wait=settings.selenium_extra_wait,
        selenium_driver_path=settings.selenium_driver_path,
        selenium_pool_size=settings.selenium_pool_size,
        async_concurrency=settings.async_concurrency,
        numeric_conversion_threshold=settings.numeric_conversion_threshold,
        percent_as_decimal=settings.percent_as_decimal,
        number_locale=settings.number_locale,
        clean_column_names=settings.clean_column_names,
        snake_case_columns=settings.snake_case_columns,
        drop_empty_rows=settings.drop_empty_rows,
        drop_empty_columns=settings.drop_empty_columns,
        read_html_header=settings.read_html_header,
        read_html_displayed_only=settings.read_html_displayed_only,
        thousands=settings.thousands,
        decimal=settings.decimal,
        parser=settings.parser,
        log_level=settings.log_level,
        max_table_cells_warning=settings.max_table_cells_warning,
        max_discovery_tables=settings.max_discovery_tables,
    )


def select_table_index(
    discovery: pd.DataFrame,
    configured_table_index: Optional[int],
) -> int:
    """Select table index either from SEÇİMLER or user input."""

    if discovery.empty:
        raise ScraperError(
            code="DISCOVERY_EMPTY",
            message="Bu linkte tablo bulunamadı.",
        )

    valid_indices = set(discovery["table_index"].dropna().astype(int).tolist())

    if configured_table_index is None:
        return ask_table_index(valid_indices)

    if not isinstance(configured_table_index, int):
        raise ScraperError(
            code="INVALID_CONFIG_TABLE_INDEX",
            message="table_index int veya None olmalı.",
            detail=f"Given type: {type(configured_table_index).__name__}",
            suggestion="Örnek: table_index = None veya table_index = 2",
        )

    if configured_table_index not in valid_indices:
        raise ScraperError(
            code="CONFIG_TABLE_INDEX_OUT_OF_RANGE",
            message=f"Seçilen table_index geçersiz: {configured_table_index}",
            detail=f"Seçilebilir indexler: {sorted(valid_indices)}",
        )

    return configured_table_index


def run_from_settings(settings: RuntimeSettings) -> int:
    """Run the scraper using settings from the SEÇİMLER section."""

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    config = build_config(settings)
    scraper = WebTableDataFrameScraper(config=config)

    try:
        discovery = scraper.discover_tables(
            url=settings.url,
            css_selector=settings.css_selector,
            preview_rows=settings.preview_rows_for_selection,
            clean=settings.clean,
        )

        print_table_discovery(discovery)

        selected_table_index = select_table_index(
            discovery=discovery,
            configured_table_index=settings.table_index,
        )

    except KeyboardInterrupt:
        print("\nProgram kullanıcı tarafından durduruldu.")
        return 130

    except Exception as error:
        print_section("TABLOLAR ÖNİZLENİRKEN HATA OLUŞTU")
        print(format_error(error))
        return 1

    if settings.excel_output is None:
        excel_output = ask_yes_no("Excel output istiyor musun?", default=False)
    else:
        excel_output = settings.excel_output

    refresh_seconds = settings.refresh_seconds

    if settings.auto_refresh and refresh_seconds is None:
        refresh_seconds = ask_int(
            "Canlı veri açık. Data kaç saniyede bir güncellensin?",
            10,
            minimum=1,
        )

    if settings.auto_refresh and refresh_seconds is not None and refresh_seconds <= 0:
        raise ScraperError(
            code="INVALID_REFRESH_SECONDS",
            message="refresh_seconds pozitif bir integer olmalı.",
            detail=f"Given value: {refresh_seconds}",
        )

    excel_path: Optional[Path] = settings.excel_path

    if excel_output and excel_path is None:
        excel_path = build_excel_output_path(
            url=settings.url,
            table_index=selected_table_index,
        )
        print(f"Excel dosyası burada oluşturulacak: {excel_path}")

    while True:
        try:
            df = scraper.scrape_table(
                url=settings.url,
                table_index=selected_table_index,
                css_selector=settings.css_selector,
                clean=settings.clean,
            )

            print_current_data(
                df=df,
                url=settings.url,
                table_index=selected_table_index,
                preview_rows=settings.preview_rows,
            )

            if excel_output and excel_path is not None:
                write_dataframe_to_excel(
                    df=df,
                    output_path=excel_path,
                    url=settings.url,
                    table_index=selected_table_index,
                    auto_refresh=settings.auto_refresh,
                )
                print(f"\nExcel güncellendi: {excel_path}")

        except KeyboardInterrupt:
            print("\nProgram kullanıcı tarafından durduruldu.")
            return 130

        except Exception as error:
            print_section("HATA OLUŞTU")
            print(format_error(error))

        if not settings.auto_refresh:
            break

        if refresh_seconds is None or refresh_seconds <= 0:
            print("\nrefresh_seconds ayarlanmadı veya geçersiz. Program durduruldu.")
            break

        print(f"\n{refresh_seconds} saniye sonra data tekrar güncellenecek...\n")
        time.sleep(refresh_seconds)

    return 0


# ============================================================
# OPTIONAL CLI RUNNER
# ============================================================
# Bu kısım ekstra. Normal kullanımda aşağıdaki SEÇİMLER bölümü yeterli.
# İstersen terminalden de çalıştırabilirsin:
#
#   python advanced_web_table_scraper_with_choices.py --url "https://example.com" --table-index 0 --excel
#
# Ama hiçbir argüman vermezsen doğrudan SEÇİMLER bölümü çalışır.

def _settings_from_common_cli(
    *,
    url: str,
    selector: str,
    table_index: int,
    excel: bool,
    output: Optional[str],
    selenium: bool,
    cache: bool,
    robots: bool,
    number_locale: str,
    log_level: str,
    cache_ttl_seconds: int = 900,
    async_concurrency: int = 5,
    selenium_pool_size: int = 1,
) -> RuntimeSettings:
    return RuntimeSettings(
        url=url,
        css_selector=selector,
        table_index=table_index,
        excel_output=excel,
        excel_path=Path(output) if output else None,
        use_selenium_first=selenium,
        cache_enabled=cache,
        cache_ttl_seconds=cache_ttl_seconds,
        respect_robots_txt=robots,
        number_locale=number_locale,
        log_level=log_level,
        async_concurrency=async_concurrency,
        selenium_pool_size=selenium_pool_size,
    )


def run_from_argparse(argv: Optional[list[str]] = None) -> int:
    """Fallback CLI when Typer is not installed."""

    cli = argparse.ArgumentParser(
        description="Advanced Web Table Scraper with SEÇİMLER section.",
    )

    cli.add_argument("--url", required=True)
    cli.add_argument("--selector", default="table")
    cli.add_argument("--table-index", type=int, default=0)
    cli.add_argument("--excel", action="store_true")
    cli.add_argument("--output", default=None)
    cli.add_argument("--selenium", action="store_true")
    cli.add_argument("--cache", action="store_true")
    cli.add_argument("--cache-ttl-seconds", type=int, default=900)
    cli.add_argument("--robots", action="store_true")
    cli.add_argument("--number-locale", choices=["auto", "en", "tr", "eu"], default="auto")
    cli.add_argument("--async-concurrency", type=int, default=5)
    cli.add_argument("--selenium-pool-size", type=int, default=1)
    cli.add_argument("--log-level", default="INFO")

    args = cli.parse_args(argv)

    settings = _settings_from_common_cli(
        url=args.url,
        selector=args.selector,
        table_index=args.table_index,
        excel=args.excel,
        output=args.output,
        selenium=args.selenium,
        cache=args.cache,
        cache_ttl_seconds=args.cache_ttl_seconds,
        robots=args.robots,
        number_locale=args.number_locale,
        log_level=args.log_level,
        async_concurrency=args.async_concurrency,
        selenium_pool_size=args.selenium_pool_size,
    )

    return run_from_settings(settings)


def run_from_typer(argv: Optional[list[str]] = None) -> int:
    """Typer-based CLI when Typer is installed."""

    if typer is None:
        return run_from_argparse(argv)

    app = typer.Typer(
        help="Advanced Web Table Scraper.",
        add_completion=False,
        no_args_is_help=True,
    )

    @app.command()
    def scrape(
        url: str = typer.Option(..., "--url", help="URL to scrape."),
        selector: str = typer.Option("table", "--selector", help="CSS selector for tables."),
        table_index: int = typer.Option(0, "--table-index", help="Table index to scrape."),
        excel: bool = typer.Option(False, "--excel", help="Write Excel output."),
        output: Optional[str] = typer.Option(None, "--output", help="Excel output path."),
        selenium: bool = typer.Option(False, "--selenium", help="Use Selenium first."),
        cache: bool = typer.Option(False, "--cache", help="Enable metadata HTML cache."),
        cache_ttl_seconds: int = typer.Option(900, "--cache-ttl-seconds", help="Cache TTL."),
        robots: bool = typer.Option(False, "--robots", help="Respect robots.txt."),
        number_locale: str = typer.Option("auto", "--number-locale", help="auto/en/tr/eu."),
        async_concurrency: int = typer.Option(5, "--async-concurrency", help="Async HTTP concurrency."),
        selenium_pool_size: int = typer.Option(1, "--selenium-pool-size", help="Reusable Selenium driver pool size."),
        log_level: str = typer.Option("INFO", "--log-level", help="Logging level."),
    ) -> None:
        settings = _settings_from_common_cli(
            url=url,
            selector=selector,
            table_index=table_index,
            excel=excel,
            output=output,
            selenium=selenium,
            cache=cache,
            cache_ttl_seconds=cache_ttl_seconds,
            robots=robots,
            number_locale=number_locale,
            log_level=log_level,
            async_concurrency=async_concurrency,
            selenium_pool_size=selenium_pool_size,
        )

        raise typer.Exit(run_from_settings(settings))

    try:
        app(args=argv, standalone_mode=False)
    except SystemExit as exc:
        return int(exc.code or 0)
    except Exception as exc:
        print(format_error(exc), file=sys.stderr)
        return 1

    return 0


def run_from_cli(argv: Optional[list[str]] = None) -> int:
    """Use Typer if available; otherwise fall back to argparse."""

    if typer is not None:
        return run_from_typer(argv)

    return run_from_argparse(argv)


# ============================================================
# INTERACTIVE INPUT BUILDER
# ============================================================

def ask_text(question: str, default: str) -> str:
    """Ask text input with a default value."""

    answer = input(f"{question} [{default}]: ").strip()
    return answer if answer else default


def ask_optional_text(question: str, default: Optional[str] = None) -> Optional[str]:
    """Ask optional text input. Empty keeps default / None."""

    shown_default = "None" if default is None else default
    answer = input(f"{question} [{shown_default}]: ").strip()

    if not answer:
        return default

    if answer.lower() in {"none", "null", "yok", "hayır", "hayir", "no"}:
        return None

    return answer


def ask_int(question: str, default: int, minimum: Optional[int] = None) -> int:
    """Ask integer input with validation."""

    while True:
        answer = input(f"{question} [{default}]: ").strip()

        if not answer:
            return default

        try:
            value = int(answer)
        except ValueError:
            print("Lütfen tam sayı gir. Örnek: 10")
            continue

        if minimum is not None and value < minimum:
            print(f"Değer en az {minimum} olmalı.")
            continue

        return value


def ask_optional_int(question: str, default: Optional[int] = None) -> Optional[int]:
    """Ask optional integer input. Empty keeps None/default."""

    shown_default = "None" if default is None else str(default)

    while True:
        answer = input(f"{question} [{shown_default}]: ").strip()

        if not answer:
            return default

        if answer.lower() in {"none", "null", "yok"}:
            return None

        try:
            return int(answer)
        except ValueError:
            print("Lütfen tam sayı veya boş/None gir. Örnek: 0")


def ask_float(question: str, default: float, minimum: Optional[float] = None) -> float:
    """Ask float input with validation."""

    while True:
        answer = input(f"{question} [{default}]: ").strip()

        if not answer:
            return default

        try:
            value = float(answer.replace(",", "."))
        except ValueError:
            print("Lütfen sayı gir. Örnek: 0.85")
            continue

        if minimum is not None and value < minimum:
            print(f"Değer en az {minimum} olmalı.")
            continue

        return value


def ask_choice(question: str, choices: Sequence[str], default: str) -> str:
    """Ask one value from a finite set of choices."""

    choices_set = set(choices)
    choices_text = "/".join(choices)

    while True:
        answer = input(f"{question} ({choices_text}) [{default}]: ").strip()

        if not answer:
            return default

        if answer in choices_set:
            return answer

        print(f"Geçersiz seçim. Seçenekler: {choices_text}")


def ask_path(question: str, default: Optional[Path] = None) -> Optional[Path]:
    """Ask optional filesystem path."""

    shown_default = "None" if default is None else str(default)
    answer = input(f"{question} [{shown_default}]: ").strip()

    if not answer:
        return default

    if answer.lower() in {"none", "null", "yok"}:
        return None

    return Path(answer).expanduser()


def build_settings_from_interactive_questions() -> RuntimeSettings:
    """
    Build RuntimeSettings by asking the user questions in the terminal.

    This is the beginner-friendly mode: instead of editing many variables or
    writing CLI arguments, the program asks the main choices one by one.
    """

    print_section("SEÇİMLER")
    print("Boş bırakırsan köşeli parantezdeki varsayılan değer kullanılır.")

    url = ask_text(
        "Hangi linkten veri çekilecek?",
        "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)",
    )
    css_selector = ask_text("Hangi HTML elemanları tablo olarak aransın?", "table")
    preview_rows_for_selection = ask_int("Önizlemede her tablodan kaç satır gösterilsin?", 4, minimum=0)
    table_index = ask_optional_int("Hangi tablo seçilecek? Boş bırakırsan önizleme sonrası sorulur", None)

    print_section("TEMİZLEME AYARLARI")
    clean = ask_yes_no("Veriler temizlensin mi?", default=True)
    clean_column_names = ask_yes_no("Kolon isimleri düzenlensin mi?", default=True)
    snake_case_columns = ask_yes_no("Kolon isimleri snake_case olsun mu?", default=False)
    drop_empty_rows = ask_yes_no("Boş satırlar silinsin mi?", default=True)
    drop_empty_columns = ask_yes_no("Boş kolonlar silinsin mi?", default=True)
    percent_as_decimal = ask_yes_no("Yüzdelik değerler ondalığa çevrilsin mi? Örn 12% -> 0.12", default=True)
    numeric_conversion_threshold = ask_float("Numeric conversion threshold kaç olsun?", 0.85, minimum=0.0)
    number_locale = ask_choice("Sayı formatı ne olsun?", ["auto", "en", "tr", "eu"], "auto")

    print_section("HTML / PANDAS AYARLARI")
    read_html_header_raw = ask_optional_text("pandas.read_html header değeri", "0")
    if read_html_header_raw is None or read_html_header_raw.lower() == "none":
        read_html_header: int | list[int] | None = None
    else:
        read_html_header = int(read_html_header_raw)
    read_html_displayed_only = ask_yes_no("Sadece displayed tablolar okunsun mu?", default=False)
    thousands = ask_optional_text("Binlik ayracı. Örn İngilizce için , Avrupa/TR için .", None)
    decimal = ask_optional_text("Ondalık ayracı. Örn İngilizce için . Avrupa/TR için ,", None)

    print_section("REQUEST / RETRY AYARLARI")
    timeout = ask_int("Timeout kaç saniye olsun?", 60, minimum=1)
    max_retries = ask_int("Hata olursa kaç kere denensin?", 3, minimum=1)
    retry_delay = ask_float("Retry arası kaç saniye beklensin?", 3.0, minimum=0.0)
    retry_backoff = ask_float("Retry backoff çarpanı kaç olsun?", 1.5, minimum=1.0)
    request_delay = ask_float("Her request öncesi kaç saniye beklensin?", 0.0, minimum=0.0)

    print_section("ROBOTS / CACHE AYARLARI")
    respect_robots_txt = ask_yes_no("robots.txt kontrolü yapılsın mı?", default=False)
    robots_cache_seconds = ask_int("robots.txt cache süresi kaç saniye olsun?", 3600, minimum=0)
    cache_enabled = ask_yes_no("HTML cache kullanılsın mı?", default=False)
    cache_dir = ask_text("Cache klasörü ne olsun?", ".wts_cache")
    cache_ttl_seconds = ask_int("Cache kaç saniye geçerli olsun?", 900, minimum=0)

    print_section("SELENIUM AYARLARI")
    use_selenium_first = ask_yes_no("Önce Selenium kullanılsın mı?", default=False)
    use_selenium_fallback = ask_yes_no("HTTP/cloudscraper başarısız olursa Selenium fallback kullanılsın mı?", default=True)
    selenium_headless = ask_yes_no("Selenium görünmeden/headless çalışsın mı?", default=True)
    selenium_driver_path_raw = ask_optional_text("ChromeDriver path. Boş bırakırsan Selenium Manager dener", None)
    selenium_driver_path = selenium_driver_path_raw
    selenium_pool_size = ask_int("Selenium browser pool boyutu kaç olsun?", 1, minimum=1)
    wait_time = ask_int("Selenium tabloyu kaç saniye beklesin?", 10, minimum=0)
    selenium_extra_wait = ask_float("Selenium sayfa yüklenince ekstra kaç saniye beklesin?", 0.5, minimum=0.0)

    print_section("ÇALIŞTIRMA / OUTPUT AYARLARI")
    async_concurrency = ask_int("Async/concurrent scraping için concurrency kaç olsun?", 5, minimum=1)
    refresh_seconds = ask_int("Data kaç saniyede bir güncellensin?", 10, minimum=1)
    auto_refresh = ask_yes_no("Sürekli güncellensin mi?", default=False)
    preview_rows = ask_int("Güncel datada ekrana kaç satır yazdırılsın?", 10, minimum=0)
    excel_output = ask_yes_no("Excel output istiyor musun?", default=False)
    excel_path = ask_path("Excel dosya yolu. Boş bırakırsan Desktop'a otomatik yazar", None) if excel_output else None
    parser = ask_choice("BeautifulSoup parser ne olsun?", ["lxml", "html.parser", "html5lib"], "lxml")
    log_level = ask_choice("Log seviyesi ne olsun?", ["DEBUG", "INFO", "WARNING", "ERROR"], "INFO")
    max_table_cells_warning = ask_int("Büyük tablo uyarısı kaç hücreden sonra verilsin?", 2_000_000, minimum=1)
    max_discovery_tables = ask_optional_int("Keşif aşamasında maksimum kaç tablo incelensin? Boş = hepsi", None)

    return RuntimeSettings(
        url=url,
        css_selector=css_selector,
        preview_rows_for_selection=preview_rows_for_selection,
        table_index=table_index,
        clean=clean,
        clean_column_names=clean_column_names,
        snake_case_columns=snake_case_columns,
        drop_empty_rows=drop_empty_rows,
        drop_empty_columns=drop_empty_columns,
        percent_as_decimal=percent_as_decimal,
        numeric_conversion_threshold=numeric_conversion_threshold,
        number_locale=number_locale,
        read_html_header=read_html_header,
        read_html_displayed_only=read_html_displayed_only,
        thousands=thousands,
        decimal=decimal,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        retry_backoff=retry_backoff,
        request_delay=request_delay,
        respect_robots_txt=respect_robots_txt,
        robots_cache_seconds=robots_cache_seconds,
        cache_enabled=cache_enabled,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        use_selenium_first=use_selenium_first,
        use_selenium_fallback=use_selenium_fallback,
        selenium_headless=selenium_headless,
        selenium_driver_path=selenium_driver_path,
        selenium_extra_wait=selenium_extra_wait,
        selenium_pool_size=selenium_pool_size,
        wait_time=wait_time,
        async_concurrency=async_concurrency,
        refresh_seconds=refresh_seconds,
        auto_refresh=auto_refresh,
        preview_rows=preview_rows,
        excel_output=excel_output,
        excel_path=excel_path,
        parser=parser,
        log_level=log_level,
        max_table_cells_warning=max_table_cells_warning,
        max_discovery_tables=max_discovery_tables,
    )


if __name__ == "__main__":

    # ============================================================
    # Seçimler:
    # ============================================================

    if len(sys.argv) > 1:
        # Terminal argümanı verilmişse opsiyonel CLI çalışır.
        raise SystemExit(run_from_cli(sys.argv[1:]))

    # Hangi linkten veri çekilecek?
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"

    # Hangi HTML elemanları tablo olarak aranacak?
    # Normal HTML tablolar için "table" kalsın.
    css_selector = "table"

    # Önizlemede her tablodan kaç satır gösterilsin?
    preview_rows_for_selection = 4

    # Hangi tablo seçilecek?
    # None kalırsa program bütün tabloları önizledikten sonra input() ile sorar.
    # Örnek: table_index = 2 yazarsan direkt 2. indexi seçer.
    table_index = None

    # Veriler temizlensin mi?
    clean = True

    # Kolon isimleri düzenlensin mi?
    clean_column_names = True

    # Kolon isimleri snake_case olsun mu?
    # Örnek: "Product Price" -> "product_price"
    snake_case_columns = False

    # Boş satırlar silinsin mi?
    drop_empty_rows = True

    # Boş kolonlar silinsin mi?
    drop_empty_columns = True

    # Yüzdelik değerler ondalığa çevrilsin mi?
    # Örnek: "12%" -> 0.12
    percent_as_decimal = True

    # Sayısal gibi görünen kolonları sayıya çevirme eşiği.
    # 0.85 demek: kolonun %85'i sayıya benziyorsa sayıya çevir.
    numeric_conversion_threshold = 0.85

    # Sayı formatı:
    # "auto" = otomatik seçer
    # "en"   = 1,234.56 gibi İngilizce format
    # "eu"   = 1.234,56 gibi Avrupa/Türkiye formatı
    # "tr"   = eu ile aynı çalışır
    number_locale = "auto"

    # pandas.read_html ayarları.
    # Çoğu durumda bunları değiştirme.
    read_html_header = 0
    read_html_displayed_only = False

    # Binlik ve ondalık ayracı.
    # None bırakırsan pandas tarafında zorlamaz, temizleme aşamasında number_locale devreye girer.
    # İngilizce için örnek: thousands = ",", decimal = "."
    # Avrupa/TR için örnek: thousands = ".", decimal = ","
    thousands = None
    decimal = None

    # Sayfa çekme timeout süresi.
    timeout = 60

    # Hata olursa kaç kere tekrar denensin?
    max_retries = 3

    # Tekrar denemeler arasında kaç saniye beklensin?
    retry_delay = 3.0

    # Retry bekleme süresi her denemede çarpılsın mı?
    # Örnek: 3 saniye, sonra 4.5 saniye, sonra 6.75 saniye.
    retry_backoff = 1.5

    # Her request öncesinde kaç saniye beklensin?
    # Rate limit/etik scraping için artırabilirsin.
    request_delay = 0.0

    # robots.txt kontrolü yapılsın mı?
    # True yaparsan site robots.txt ile engelliyorsa scraping yapmaz.
    respect_robots_txt = False

    # robots.txt cache süresi.
    # Aynı domain için robots.txt her seferinde yeniden okunmaz.
    robots_cache_seconds = 3600

    # HTML cache kullanılsın mı?
    # True olursa aynı URL tekrar çekilirken belirli süre cache'den okunabilir.
    # Cache zaten metadata'lıdır: HTTP status, timestamp ve response headers saklanır.
    cache_enabled = False

    # Cache klasörü.
    cache_dir = ".wts_cache"

    # Cache kaç saniye geçerli olsun?
    cache_ttl_seconds = 900

    # JavaScript ile yüklenen sitelerde Selenium gerekebilir.
    # Önce Selenium kullanılsın mı?
    use_selenium_first = False

    # HTTP/cloudscraper başarısız olursa Selenium yedek olarak kullanılsın mı?
    use_selenium_fallback = True

    # Selenium tarayıcıyı görünmeden çalıştırsın mı?
    selenium_headless = True

    # ChromeDriver path.
    # None kalırsa Selenium Manager otomatik çözmeye çalışır.
    # Örnek macOS:
    # selenium_driver_path = "/Users/username/Downloads/chromedriver"
    selenium_driver_path = None

    # Selenium browser pool boyutu.
    # Tek URL için 1 yeterli. Ağır sistemlerde dikkatli artır.
    selenium_pool_size = 1

    # Selenium sayfada tabloyu kaç saniye beklesin?
    wait_time = 10

    # Selenium sayfa yüklendikten sonra ekstra kaç saniye beklesin?
    selenium_extra_wait = 0.5

    # Async/concurrent scraping için aynı anda kaç URL çekilsin?
    # Bu SEÇİMLER akışında tek URL kullanılır; çoklu kullanım için scraper.scrape_many_urls() var.
    async_concurrency = 5

    # Sürekli güncellensin mi?
    # False: statik mod, sadece bir kere çeker ve durur.
    # True: canlı mod, belirli saniyede bir tekrar çeker.
    auto_refresh = False

    # Canlı modda data kaç saniyede bir güncellensin?
    # None: auto_refresh = True ise terminalde sorar.
    # Örnek: refresh_seconds = 10 yazarsan sormadan 10 saniyede bir günceller.
    refresh_seconds = None

    # Güncel datada ekrana kaç satır yazdırılsın?
    preview_rows = 10

    # Excel output istiyor musun?
    # None: program sana terminalde sorar.
    # True: direkt Excel yazar.
    # False: Excel yazmaz.
    excel_output = None

    # Excel dosya yolu.
    # None kalırsa Desktop'a otomatik isimle yazar.
    # Örnek:
    # excel_path = Path("/Users/username/Desktop/output.xlsx")
    excel_path = None

    # BeautifulSoup parser.
    # Genelde "lxml" kalsın.
    parser = "lxml"

    # Log seviyesi:
    # "DEBUG", "INFO", "WARNING", "ERROR" kullanılabilir.
    log_level = "INFO"

    # Büyük tablo uyarısı kaç hücreden sonra verilsin?
    max_table_cells_warning = 2_000_000

    # Keşif aşamasında maksimum kaç tablo incelensin?
    # None = hepsini incele.
    max_discovery_tables = None

    # ============================================================
    # AYARLARI OLUŞTUR VE ÇALIŞTIR
    # ============================================================
    # Bu bölümden aşağısını normalde değiştirmene gerek yok.

    settings = RuntimeSettings(
        url=url,
        css_selector=css_selector,
        preview_rows_for_selection=preview_rows_for_selection,
        table_index=table_index,
        clean=clean,
        clean_column_names=clean_column_names,
        snake_case_columns=snake_case_columns,
        drop_empty_rows=drop_empty_rows,
        drop_empty_columns=drop_empty_columns,
        percent_as_decimal=percent_as_decimal,
        numeric_conversion_threshold=numeric_conversion_threshold,
        number_locale=number_locale,
        read_html_header=read_html_header,
        read_html_displayed_only=read_html_displayed_only,
        thousands=thousands,
        decimal=decimal,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        retry_backoff=retry_backoff,
        request_delay=request_delay,
        respect_robots_txt=respect_robots_txt,
        robots_cache_seconds=robots_cache_seconds,
        cache_enabled=cache_enabled,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        use_selenium_first=use_selenium_first,
        use_selenium_fallback=use_selenium_fallback,
        selenium_headless=selenium_headless,
        selenium_driver_path=selenium_driver_path,
        selenium_extra_wait=selenium_extra_wait,
        selenium_pool_size=selenium_pool_size,
        wait_time=wait_time,
        async_concurrency=async_concurrency,
        refresh_seconds=refresh_seconds,
        auto_refresh=auto_refresh,
        preview_rows=preview_rows,
        excel_output=excel_output,
        excel_path=excel_path,
        parser=parser,
        log_level=log_level,
        max_table_cells_warning=max_table_cells_warning,
        max_discovery_tables=max_discovery_tables,
    )

    raise SystemExit(run_from_settings(settings))
