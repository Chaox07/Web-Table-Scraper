"""
Advanced Web Table Scraper
==========================
Author: C. Yildiz

Features
--------
- Scrape HTML tables from a URL or raw HTML, parsed directly into polars
  (no pandas anywhere in this file -- tables are read with a hand-rolled
  BeautifulSoup -> polars grid parser, not pandas.read_html)
- Static mode: one scrape -> DuckDB table, then exit
- Live mode: poll forever, hot SQLite (WAL) snapshot every poll, and on
  Ctrl+C the final snapshot is flushed into the DuckDB cold table -- same
  hot/cold pattern as alpaca_extractor.py

"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
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

import duckdb
import polars as pl
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


# ==========================================================
# SETTINGS -- core, edit these
# ==========================================================
URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
CSS_SELECTOR = "table"   # which HTML elements count as a "table"; "table" is almost always right
TABLE_INDEX = 0          # which table on the page (0-based). Unsure? call discover_tables() first.
MODE = "static"          # "static" (one scrape, write to duckdb, exit) or "live" (poll forever)
LIVE_POLL_SECONDS = 30.0  # live mode only

# ==========================================================
# PATHS -- explicit, not assumed relative to this file
# ==========================================================
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))  # this file's own folder

# ==========================================================
# ADVANCED -- sensible defaults, rarely need changing
# ==========================================================

# region fetch / retry
TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 3.0
RETRY_BACKOFF = 1.5
REQUEST_DELAY = 0.0
# endregion

# region robots.txt / cache
RESPECT_ROBOTS_TXT = False
ROBOTS_CACHE_SECONDS = 3600
CACHE_ENABLED = False
CACHE_DIR = os.path.join(OUTPUT_DIR, ".wts_cache")
CACHE_TTL_SECONDS = 900
# endregion

# region selenium (JS-rendered pages only)
USE_SELENIUM_FIRST = False
USE_SELENIUM_FALLBACK = True
SELENIUM_HEADLESS = True
SELENIUM_DRIVER_PATH = None   # None = Selenium Manager resolves it automatically
SELENIUM_EXTRA_WAIT = 0.5
SELENIUM_POOL_SIZE = 1
WAIT_TIME = 10
# endregion

# region cleaning / numeric parsing
CLEAN = True
CLEAN_COLUMN_NAMES = True
SNAKE_CASE_COLUMNS = False
DROP_EMPTY_ROWS = True
DROP_EMPTY_COLUMNS = True
PERCENT_AS_DECIMAL = True        # "12%" -> 0.12
NUMERIC_CONVERSION_THRESHOLD = 0.85
NUMBER_LOCALE = "auto"           # "auto" / "en" (1,234.56) / "eu" or "tr" (1.234,56)
# endregion

MAX_TABLE_CELLS_WARNING = 2_000_000
PARSER = "lxml"                  # BeautifulSoup parser: "lxml" / "html.parser" / "html5lib"
LOG_LEVEL = "INFO"

ASYNC_CONCURRENCY = 5            # only used by scrape_many_urls / async_scrape_many_urls

# ==========================================================
# RESOLVED -- derived, don't edit
# ==========================================================


def _table_name_for_url(url: str, table_index: int) -> str:
    domain = urlparse(url).netloc.replace("www.", "") or "web_table"
    slug = re.sub(r"[^A-Za-z0-9]+", "_", domain).strip("_").lower() or "web_table"
    return f"{slug}_t{table_index}"


DUCKDB_TABLE_NAME = _table_name_for_url(URL, TABLE_INDEX)
COLD_DB_PATH = os.path.join(OUTPUT_DIR, f"{DUCKDB_TABLE_NAME}.duckdb")
HOT_DB_PATH = os.path.join(OUTPUT_DIR, f"{DUCKDB_TABLE_NAME}_hot.sqlite")


__version__ = "1.0.0"

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

CURRENCY_PATTERN = r"[$€£¥₹₺₩₽]"
WHITESPACE_PATTERN = re.compile(r"\s+")
NON_SNAKE_PATTERN = re.compile(r"[^0-9a-zA-Z]+")
UNNAMED_COLUMN_PATTERN = re.compile(r"^_col\d+(_\d+)?$")


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
    dataframe: pl.DataFrame

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
    dataframe: Optional[pl.DataFrame] = None
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

    Each key is saved as a JSON file containing HTML plus status code, fetch
    timestamp, response headers and source.
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
                suggestion="Kurulum: conda install -c conda-forge selenium",
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
                    suggestion="Kurulum: conda install -c conda-forge requests cloudscraper",
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
                suggestion="Kurulum: conda install -c conda-forge selenium",
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
                suggestion="Kurulum: conda install -c conda-forge httpx",
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
# HTML -> GRID (replaces pandas.read_html)
# ============================================================

def _safe_span_int(raw: Any, default: int = 1) -> int:
    try:
        value = int(str(raw).strip())
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _html_table_to_grid(table_node: Any) -> Tuple[List[List[str]], int]:
    """
    Expand a <table> tag's rows into a rectangular grid of cell text,
    resolving colspan/rowspan the way a browser would render them. Returns
    (grid, header_row_count): header_row_count is how many leading rows are
    the header -- from <thead> if present, else however many leading rows
    are made entirely of <th> cells, else 1 (matching pandas.read_html's own
    default of header=0: the first row is always the header).
    """
    trs = table_node.find_all("tr")
    if not trs:
        return [], 0

    grid: List[List[str]] = []
    header_flags: List[bool] = []
    span_carry: Dict[int, List[Any]] = {}  # col -> [text, rows_remaining]

    thead = table_node.find("thead")
    thead_trs = {id(tr) for tr in thead.find_all("tr")} if thead else set()

    for tr in trs:
        cells = tr.find_all(["td", "th"], recursive=False)
        row: List[str] = []
        col = 0
        ci = 0
        is_header_row = id(tr) in thead_trs or (
            bool(cells) and all(c.name == "th" for c in cells)
        )

        while ci < len(cells) or col in span_carry:
            if col in span_carry:
                text, remaining = span_carry[col]
                row.append(text)
                if remaining - 1 <= 0:
                    del span_carry[col]
                else:
                    span_carry[col][1] = remaining - 1
                col += 1
                continue

            cell = cells[ci]
            ci += 1
            text = cell.get_text(" ", strip=True)
            colspan = _safe_span_int(cell.get("colspan"), 1)
            rowspan = _safe_span_int(cell.get("rowspan"), 1)
            for _ in range(colspan):
                row.append(text)
                if rowspan > 1:
                    span_carry[col] = [text, rowspan - 1]
                col += 1

        grid.append(row)
        header_flags.append(is_header_row)

    max_cols = max((len(r) for r in grid), default=0)
    grid = [r + [""] * (max_cols - len(r)) for r in grid]

    header_row_count = 0
    for flag in header_flags:
        if not flag:
            break
        header_row_count += 1
    if header_row_count == 0:
        header_row_count = 1

    return grid, header_row_count


def _grid_header_to_columns(grid: List[List[str]], header_row_count: int) -> List[str]:
    """Join a (possibly multi-row) header into one name per column -- the same
    idea as the old pandas MultiIndex tuple-join, minus pandas."""
    if not grid:
        return []
    ncols = len(grid[0])
    columns: List[str] = []
    for c in range(ncols):
        parts: List[str] = []
        for r in range(header_row_count):
            val = grid[r][c].strip() if r < len(grid) else ""
            if val and val not in parts:
                parts.append(val)
        columns.append(" ".join(parts))
    return columns


# ============================================================
# CLEANER
# ============================================================

class DataFrameCleaner:
    """Clean and normalize scraped polars DataFrames."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config

    def clean(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.width == 0:
            return df

        out = df

        if self.config.clean_column_names:
            new_names = self.clean_columns(out.columns, snake_case=self.config.snake_case_columns)
            new_names = self.dedupe_names(new_names)
            out = out.rename(dict(zip(out.columns, new_names)))

        out = self.remove_unnamed_columns(out)
        out = self.strip_text_cells(out)

        if self.config.drop_empty_rows or self.config.drop_empty_columns:
            out = self.drop_empty_rows_and_columns(out)

        out = self.convert_numeric_like_columns(out)

        if self.config.drop_empty_rows or self.config.drop_empty_columns:
            out = self.drop_empty_rows_and_columns(out)

        return out

    @classmethod
    def clean_columns(cls, columns: Any, snake_case: bool = False) -> List[str]:
        cleaned: List[str] = []

        for col in columns:
            name = str(col).strip()
            name = WHITESPACE_PATTERN.sub(" ", name)

            if UNNAMED_COLUMN_PATTERN.match(name):
                name = ""

            if snake_case and name:
                name = cls.to_snake_case(name)

            cleaned.append(name)

        return cleaned

    @staticmethod
    def to_snake_case(text: str) -> str:
        text = NON_SNAKE_PATTERN.sub("_", text.strip())
        text = re.sub(r"_+", "_", text)
        text = text.strip("_")

        return text.lower()

    @staticmethod
    def dedupe_names(names: List[str]) -> List[str]:
        """Fills blanks with a placeholder and disambiguates repeats. Polars
        (unlike pandas) refuses to construct a frame with duplicate column
        names at all, so this must run before every DataFrame construction,
        not just as a cosmetic cleanup pass afterward."""
        columns: List[str] = []
        counts: Dict[str, int] = {}

        for i, col in enumerate(names):
            base = str(col).strip() or f"_col{i}"
            counts[base] = counts.get(base, 0) + 1
            columns.append(base if counts[base] == 1 else f"{base}_{counts[base]}")

        return columns

    @staticmethod
    def remove_unnamed_columns(df: pl.DataFrame) -> pl.DataFrame:
        keep = [c for c in df.columns if not UNNAMED_COLUMN_PATTERN.match(c)]
        return df if keep == df.columns else df.select(keep)

    @staticmethod
    def strip_text_cells(df: pl.DataFrame) -> pl.DataFrame:
        if df.width == 0:
            return df

        exprs = []
        empty_values = list(EMPTY_VALUES)

        for col, dtype in zip(df.columns, df.dtypes):
            if dtype != pl.Utf8:
                exprs.append(pl.col(col))
                continue

            cleaned = pl.col(col).str.replace_all(r"\s+", " ").str.strip_chars()
            cleaned = pl.when(cleaned.is_in(empty_values)).then(None).otherwise(cleaned)
            exprs.append(cleaned.alias(col))

        return df.with_columns(exprs)

    def drop_empty_rows_and_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        out = df

        if self.config.drop_empty_rows and out.width > 0 and out.height > 0:
            blank_row = pl.all_horizontal([pl.col(c).is_null() for c in out.columns])
            out = out.filter(~blank_row)

        if self.config.drop_empty_columns and out.width > 0:
            keep = [c for c in out.columns if not out[c].is_null().all()]
            if keep != out.columns:
                out = out.select(keep)

        return out

    def convert_numeric_like_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.width == 0:
            return df

        converted_cols: Dict[str, pl.Series] = {}

        for col, dtype in zip(df.columns, df.dtypes):
            if dtype != pl.Utf8:
                continue

            series = df[col]
            non_missing = series.drop_nulls()

            if non_missing.len() == 0:
                continue

            converted = self._convert_series_by_locale(series)
            numeric_share = converted.drop_nulls().len() / non_missing.len()

            if numeric_share >= self.config.numeric_conversion_threshold:
                converted_cols[col] = converted

        if not converted_cols:
            return df

        return df.with_columns([series.alias(name) for name, series in converted_cols.items()])

    def _convert_series_by_locale(self, series: pl.Series) -> pl.Series:
        non_null = series.drop_nulls()
        has_percent = bool(non_null.str.contains("%", literal=True).any()) if non_null.len() else False

        candidate = (
            series
            .str.replace_all(CURRENCY_PATTERN, "")
            .str.replace_all("%", "", literal=True)
            .str.replace_all(r"^\((.*)\)$", r"-${1}")
            .str.replace_all(r"^\+", "")
            .str.replace_all(" ", "", literal=True)
            .str.replace_all(" ", "", literal=True)
            .str.strip_chars()
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
    def _parse_en(series: pl.Series) -> pl.Series:
        # English-style: 1,234.56 -> 1234.56
        candidate = series.str.replace_all(",", "", literal=True)
        return candidate.cast(pl.Float64, strict=False)

    @staticmethod
    def _parse_eu(series: pl.Series) -> pl.Series:
        # European/Turkish-style: 1.234,56 -> 1234.56
        candidate = (
            series
            .str.replace_all(".", "", literal=True)
            .str.replace_all(",", ".", literal=True)
        )
        return candidate.cast(pl.Float64, strict=False)

    @classmethod
    def _parse_auto(cls, series: pl.Series) -> pl.Series:
        """
        Separator-pattern confidence first, same as before: if the column is
        genuinely ambiguous ("1.234" / "1,234" without enough context),
        return all-NA so the original string column is preserved rather than
        silently misread.
        """
        non_missing = series.drop_nulls()

        if non_missing.len() == 0:
            return series.cast(pl.Float64, strict=False)

        en = cls._parse_en(series)
        eu = cls._parse_eu(series)

        comparable = en.is_not_null() & eu.is_not_null()
        if bool(comparable.any()):
            en_c = en.filter(comparable)
            eu_c = eu.filter(comparable)
            if bool((en_c == eu_c).all()):
                return en

        values = [str(v) for v in non_missing.to_list()]
        en_score = cls._locale_confidence(values, locale="en")
        eu_score = cls._locale_confidence(values, locale="eu")
        ambiguous_count = cls._ambiguous_separator_count(values)

        if en_score == 0 and eu_score == 0:
            return series.cast(pl.Float64, strict=False)

        if en_score == eu_score and ambiguous_count > 0:
            return pl.Series(series.name, [None] * series.len(), dtype=pl.Float64)

        total = max(len(values), 1)
        margin = abs(en_score - eu_score) / total

        if ambiguous_count > 0 and margin < 0.20:
            return pl.Series(series.name, [None] * series.len(), dtype=pl.Float64)

        return en if en_score > eu_score else eu

    @staticmethod
    def _locale_confidence(values: List[str], locale: str) -> int:
        score = 0

        for raw in values:
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
    def _ambiguous_separator_count(values: List[str]) -> int:
        count = 0

        for raw in values:
            value = raw.strip()

            if re.fullmatch(r"[+-]?\d{1,3}[,.]\d{3}", value):
                count += 1

        return count


# ============================================================
# PARSER
# ============================================================

class TableParser:
    """Parse HTML table nodes into polars DataFrames."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config

    def find_table_nodes(self, html: str, css_selector: str = "table") -> list[Any]:
        soup = BeautifulSoup(html, self.config.parser)
        return list(soup.select(css_selector or "table"))

    def parse_table_node(self, table_node: Any) -> pl.DataFrame:
        """Parse one already-located <table> tag directly -- no re-serializing
        to a string and re-parsing (which is what the old pandas.read_html
        path did), so this is a single HTML parse pass end to end."""

        grid, header_row_count = _html_table_to_grid(table_node)

        if not grid:
            raise ScraperError(
                code="TABLE_PARSE_EMPTY",
                message="Tablo boş veya parse edilemedi.",
            )

        raw_columns = _grid_header_to_columns(grid, header_row_count)
        raw_columns = DataFrameCleaner.dedupe_names(raw_columns)
        data_rows = grid[header_row_count:]

        columns_data = {
            name: [row[i] if i < len(row) else None for row in data_rows]
            for i, name in enumerate(raw_columns)
        }

        return pl.DataFrame(
            {name: pl.Series(name, values, dtype=pl.Utf8) for name, values in columns_data.items()}
        )

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
        df: pl.DataFrame,
        rules: TableValidationRules,
        table_index: int,
        source: str,
    ) -> None:
        if df.height < rules.min_rows:
            raise ScraperError(
                code="TOO_FEW_ROWS",
                message=f"Table {table_index} has too few rows.",
                detail=f"{df.height} < {rules.min_rows}. Source: {source}",
            )

        if df.width < rules.min_columns:
            raise ScraperError(
                code="TOO_FEW_COLUMNS",
                message=f"Table {table_index} has too few columns.",
                detail=f"{df.width} < {rules.min_columns}. Source: {source}",
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

        cell_count = df.height * df.width

        if cell_count:
            null_count = sum(df[c].null_count() for c in df.columns)
            null_ratio = null_count / cell_count

            if null_ratio > rules.max_null_ratio:
                raise ScraperError(
                    code="TOO_MANY_NULLS",
                    message=f"Table {table_index} has too many missing values.",
                    detail=f"{null_ratio:.1%} > {rules.max_null_ratio:.1%}. Source: {source}",
                )


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
    ) -> pl.DataFrame:
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
    ) -> list[pl.DataFrame]:
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
    ) -> pl.DataFrame:
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
    ) -> list[pl.DataFrame]:
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

            df = self.parser.parse_table_node(table_nodes[index])

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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
                df = self.parser.parse_table_node(node)

                if clean:
                    df = self.cleaner.clean(df)

                records.append(
                    {
                        "table_index": index,
                        "rows": df.height,
                        "columns": df.width,
                        "cell_count": int(df.height * df.width),
                        "column_names": list(df.columns),
                        "preview": str(df.head(preview_rows)),
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

        return pl.DataFrame(records, infer_schema_length=None) if records else pl.DataFrame()

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
    ) -> dict[str, pl.DataFrame]:
        """Strict compatibility helper: raises if any URL fails."""

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
        """Sync wrapper around async_scrape_many_urls()."""

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
    ) -> dict[str, pl.DataFrame]:
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
        df: pl.DataFrame,
        table_index: int,
        source: str,
    ) -> None:
        cell_count = int(df.height * df.width)

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

def require_columns(df: pl.DataFrame, columns: Sequence[str]) -> None:
    """Raise a clear error if required DataFrame columns are missing."""

    missing = [column for column in columns if column not in df.columns]

    if missing:
        raise ScraperError(
            code="MISSING_DATAFRAME_COLUMNS",
            message="Gerekli DataFrame kolonları eksik.",
            detail=f"Missing columns: {missing}",
        )


def with_columns(
    df: pl.DataFrame,
    **columns: Callable[[pl.DataFrame], Any],
) -> pl.DataFrame:
    """Return a copy of df with calculated columns added."""

    out = df
    for name, func in columns.items():
        out = out.with_columns(pl.Series(name, func(out)))
    return out


def safe_with_columns(
    df: pl.DataFrame,
    required_columns: Sequence[str] = (),
    **columns: Callable[[pl.DataFrame], Any],
) -> pl.DataFrame:
    """Return a copy of df with calculated columns added after checking required columns."""

    require_columns(df, required_columns)

    return with_columns(df, **columns)


# ============================================================
# HOT/COLD STORAGE -- same pattern as alpaca_extractor.py
# ============================================================

_DUCKDB_BLOCK_SIZE = 16384  # DuckDB's actual enforced minimum -- see connect_duckdb()


def connect_duckdb(path: str):
    """
    Connects to `path`, creating it with a small block size if it doesn't
    exist yet. DuckDB allocates storage in whole-block increments per
    table -- roughly 2 blocks minimum each, regardless of how little data
    a table holds. Its default block size (262144 bytes) means a handful
    of small tables can cost megabytes in pure block overhead with almost
    no real data in them -- verified empirically: 11 small tables -> 3.08MB
    at the default size vs 268KB at DuckDB's minimum (16384), an ~11.5x
    difference from block size alone. Block size is fixed at file creation
    and can't be changed by reattaching (DuckDB errors if you try), so an
    already-existing file (e.g. live mode appending across polls) is just
    connected to normally, keeping whatever block size it already has --
    same fix as alpaca_extractor.py's connect_duckdb.
    """
    if os.path.exists(path):
        return duckdb.connect(path)
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{path}' AS db (BLOCK_SIZE {_DUCKDB_BLOCK_SIZE})")
    conn.execute("USE db")
    return conn


def save_to_duckdb(df: pl.DataFrame, path: str, table_name: str) -> None:
    """
    Write df to a DuckDB file, overwriting the table if it already exists.
    Registers the polars DataFrame directly and does CREATE TABLE ... AS
    SELECT -- DuckDB's zero-copy Arrow path -- the same pattern
    alpaca_extractor.py's save_to_duckdb and evds_extractor.py's
    save_df_to_duckdb already use.
    """
    con = connect_duckdb(path)
    try:
        con.register("_v", df)
        try:
            con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _v')
        finally:
            con.unregister("_v")
    finally:
        con.close()


def init_hot_sqlite(path: str) -> None:
    """One-time (per file) setup: WAL journal mode so a reader process can
    read this file concurrently while the live poll loop keeps writing to
    it -- same as alpaca_extractor.py's init_hot_sqlite."""
    con = sqlite3.connect(path)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
    finally:
        con.close()


def _polars_dtype_to_sqlite(dtype: pl.DataType) -> str:
    if dtype in (pl.Float32, pl.Float64):
        return "REAL"
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return "INTEGER"
    return "TEXT"


def replace_hot_sqlite(df: pl.DataFrame, path: str, table_name: str) -> None:
    """
    Overwrites the hot table's contents with df's full current snapshot. A
    scraped table has no per-row "close" event the way a price bar does
    (see run_live's docstring), so there's no split -- every poll simply
    replaces the whole hot snapshot. Runs inside one transaction so a
    concurrent WAL reader never sees a half-emptied table.
    """
    con = sqlite3.connect(path)
    try:
        cols_sql = ", ".join(f'"{c}" {_polars_dtype_to_sqlite(df.schema[c])}' for c in df.columns)
        con.execute("BEGIN IMMEDIATE;")
        con.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        con.execute(f'CREATE TABLE "{table_name}" ({cols_sql});')
        if not df.is_empty():
            placeholders = ", ".join(["?"] * len(df.columns))
            con.executemany(
                f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                df.rows(),
            )
        con.commit()
    finally:
        con.close()


def read_hot_sqlite(path: str, table_name: str) -> pl.DataFrame:
    """Returns the hot table's current contents, or an empty DataFrame if the
    file/table doesn't exist yet."""
    if not Path(path).exists():
        return pl.DataFrame()
    con = sqlite3.connect(path)
    try:
        exists = con.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", [table_name]
        ).fetchone()[0]
        if not exists:
            return pl.DataFrame()
        return pl.read_database(query=f'SELECT * FROM "{table_name}"', connection=con)
    finally:
        con.close()


# ============================================================
# RUN -- static / live, using the SETTINGS above
# ============================================================

def _build_config() -> ScraperConfig:
    """Builds a ScraperConfig from the ADVANCED settings above."""
    return ScraperConfig(
        timeout=TIMEOUT,
        wait_time=WAIT_TIME,
        max_retries=MAX_RETRIES,
        retry_delay=RETRY_DELAY,
        retry_backoff=RETRY_BACKOFF,
        request_delay=REQUEST_DELAY,
        respect_robots_txt=RESPECT_ROBOTS_TXT,
        robots_cache_seconds=ROBOTS_CACHE_SECONDS,
        cache_enabled=CACHE_ENABLED,
        cache_dir=CACHE_DIR,
        cache_ttl_seconds=CACHE_TTL_SECONDS,
        use_selenium_first=USE_SELENIUM_FIRST,
        use_selenium_fallback=USE_SELENIUM_FALLBACK,
        selenium_headless=SELENIUM_HEADLESS,
        selenium_driver_path=SELENIUM_DRIVER_PATH,
        selenium_extra_wait=SELENIUM_EXTRA_WAIT,
        selenium_pool_size=SELENIUM_POOL_SIZE,
        async_concurrency=ASYNC_CONCURRENCY,
        numeric_conversion_threshold=NUMERIC_CONVERSION_THRESHOLD,
        percent_as_decimal=PERCENT_AS_DECIMAL,
        number_locale=NUMBER_LOCALE,
        clean_column_names=CLEAN_COLUMN_NAMES,
        snake_case_columns=SNAKE_CASE_COLUMNS,
        drop_empty_rows=DROP_EMPTY_ROWS,
        drop_empty_columns=DROP_EMPTY_COLUMNS,
        parser=PARSER,
        log_level=LOG_LEVEL,
        max_table_cells_warning=MAX_TABLE_CELLS_WARNING,
    )


def _stamp_scraped_at(df: pl.DataFrame) -> pl.DataFrame:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    return df.with_columns(pl.lit(now).alias("scraped_at"))


def run_static(
    url: str = URL,
    table_index: int = TABLE_INDEX,
    css_selector: str = CSS_SELECTOR,
    clean: bool = CLEAN,
    cold_db_path: str = COLD_DB_PATH,
    table_name: str = DUCKDB_TABLE_NAME,
) -> None:
    """One scrape -> clean -> straight into the cold DuckDB table, then exit.
    Mirrors alpaca_extractor.run()'s static-mode branch. Defaults come from
    the SETTINGS above; override per-call if you want a one-off scrape of a
    different URL/table without editing the settings."""
    config = _build_config()
    logger = setup_logger(LOG_LEVEL)

    with WebTableDataFrameScraper(config=config, logger=logger) as scraper:
        logger.info("Scraping %s (table_index=%s)...", url, table_index)
        df = scraper.scrape_table(
            url=url,
            table_index=table_index,
            css_selector=css_selector,
            clean=clean,
        )

    df = _stamp_scraped_at(df)
    save_to_duckdb(df, cold_db_path, table_name)
    logger.info(
        "Saved %d row(s), %d column(s) to: %s (table '%s')",
        df.height, df.width, cold_db_path, table_name,
    )


def run_live(
    url: str = URL,
    table_index: int = TABLE_INDEX,
    css_selector: str = CSS_SELECTOR,
    clean: bool = CLEAN,
    cold_db_path: str = COLD_DB_PATH,
    hot_db_path: str = HOT_DB_PATH,
    table_name: str = DUCKDB_TABLE_NAME,
    poll_seconds: float = LIVE_POLL_SECONDS,
) -> None:
    """
    Polls forever (Ctrl+C to stop). Each poll re-scrapes the table, cleans
    it, stamps a scraped_at timestamp, and replaces the hot SQLite (WAL)
    snapshot -- so a separate reader process can look at the live data
    while this keeps running. A transient fetch error is printed and
    retried on the next poll rather than ending the session.

    Unlike alpaca_extractor.py's run_live, there's no closed/hot split: a
    scraped table has no natural "period" that closes the way a price bar
    does, so the hot snapshot is simply the whole current table, replaced
    every poll. On Ctrl+C, that last snapshot is written into the cold
    DuckDB table -- the "stop and save as a static file" behavior.
    """
    config = _build_config()
    logger = setup_logger(LOG_LEVEL)

    init_hot_sqlite(hot_db_path)
    logger.info("Live mode: polling every %.1fs. Ctrl+C to stop.", poll_seconds)
    logger.info("Hot (live snapshot): %s (table '%s')", hot_db_path, table_name)
    logger.info("Cold (final snapshot on stop): %s (table '%s')", cold_db_path, table_name)

    last_df = read_hot_sqlite(hot_db_path, table_name)

    with WebTableDataFrameScraper(config=config, logger=logger) as scraper:
        try:
            while True:
                try:
                    df = scraper.scrape_table(
                        url=url,
                        table_index=table_index,
                        css_selector=css_selector,
                        clean=clean,
                    )
                    df = _stamp_scraped_at(df)
                    replace_hot_sqlite(df, hot_db_path, table_name)
                    last_df = df
                    logger.info("Poll OK: %d row(s), %d column(s).", df.height, df.width)
                except Exception as e:
                    logger.warning("Poll failed, will retry: %s", e)

                time.sleep(poll_seconds)
        except KeyboardInterrupt:
            if not last_df.is_empty():
                save_to_duckdb(last_df, cold_db_path, table_name)
                logger.info(
                    "\nStopped. Final snapshot (%d row(s)) saved to: %s (table '%s')",
                    last_df.height, cold_db_path, table_name,
                )
            else:
                logger.info("\nStopped. No snapshot had been captured yet -- nothing saved to cold.")


def run() -> None:
    """Reads MODE from the SETTINGS above and dispatches to run_static or
    run_live -- same shape as alpaca_extractor.run()."""
    mode = MODE.strip().lower()
    if mode not in ("static", "live"):
        raise SystemExit(f"MODE must be 'static' or 'live', got '{MODE}'.")

    if mode == "static":
        run_static()
    else:
        run_live()


if __name__ == "__main__":
    run()
