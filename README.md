# Web-Table-Scraper
# Author: C. Yildiz

"""
Advanced Web Table to DataFrame Scraper
=======================================

Scrape HTML tables from a URL or raw HTML and return clean pandas DataFrames.

Optional Excel output.
No CSV.
No Parquet.

Core workflow:
    URL / HTML -> table -> DataFrame -> add calculated columns

Dependencies:
    pip install pandas beautifulsoup4 lxml

Optional but recommended:
    pip install cloudscraper openpyxl xlsxwriter

Optional for JavaScript-heavy websites:
    pip install selenium
"""

from __future__ import annotations

import importlib.util
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import urlparse

try:
    import cloudscraper
except ImportError:  # pragma: no cover
    cloudscraper = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

import pandas as pd
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    HAVE_SELENIUM = True
except ImportError:  # pragma: no cover
    HAVE_SELENIUM = False


LOGGER_NAME = "advanced_df_scraper"

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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

    use_selenium_first: bool = False
    use_selenium_fallback: bool = True
    selenium_extra_wait: float = 0.5
    selenium_headless: bool = True
    selenium_window_size: tuple[int, int] = (1920, 1080)

    numeric_conversion_threshold: float = 0.85
    percent_as_decimal: bool = False

    clean_column_names: bool = True
    snake_case_columns: bool = False
    drop_empty_rows: bool = True
    drop_empty_columns: bool = True

    # pandas.read_html options
    read_html_header: int | list[int] | None = 0
    read_html_displayed_only: bool = False
    thousands: str | None = ","
    decimal: str = "."

    log_level: str = "INFO"
    parser: str = "lxml"

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
        if self.selenium_extra_wait < 0:
            raise ValueError("selenium_extra_wait must be >= 0")
        if not (
            isinstance(self.selenium_window_size, tuple)
            and len(self.selenium_window_size) == 2
            and all(isinstance(value, int) and value > 0 for value in self.selenium_window_size)
        ):
            raise ValueError("selenium_window_size must be a tuple of two positive integers")
        if not 0 <= self.numeric_conversion_threshold <= 1:
            raise ValueError("numeric_conversion_threshold must be between 0 and 1")
        if not self.parser:
            raise ValueError("parser cannot be empty")
        if not self.decimal:
            raise ValueError("decimal cannot be empty")


@dataclass(frozen=True)
class ScrapedTable:
    """A scraped DataFrame with source metadata."""

    source: str
    table_index: int
    dataframe: pd.DataFrame


def setup_logger(log_level: str = "INFO") -> logging.Logger:
    """Create an idempotent logger without duplicate handlers."""

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)

    for handler in logger.handlers:
        handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    return logger


class WebTableDataFrameScraper:
    """Scrape one or more HTML tables and return pandas DataFrames."""

    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or ScraperConfig()
        self.logger = setup_logger(self.config.log_level)
        self.scraper = cloudscraper.create_scraper() if cloudscraper is not None else None

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
        """Scrape selected tables from a URL. If table_indices is None, scrape all matching tables."""

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
        html = self.fetch_html(url=url, css_selector=css_selector)

        return self._extract_tables_from_html(
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

        return self._extract_tables_from_html(
            html=html,
            source=source,
            table_indices=table_indices,
            css_selector=css_selector,
            validation=validation,
            clean=clean,
        )

    def discover_tables(
        self,
        url: str,
        css_selector: str = "table",
        preview_rows: int = 5,
        clean: bool = True,
    ) -> pd.DataFrame:
        """Discover available tables on a URL."""

        self._validate_url(url)
        html = self.fetch_html(url=url, css_selector=css_selector)

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

        table_nodes = self._find_table_nodes(html=html, css_selector=css_selector)
        records: list[dict[str, Any]] = []

        for index, node in enumerate(table_nodes):
            try:
                df = self._parse_table_html(str(node))
                if clean:
                    df = self.clean_dataframe(df)

                records.append(
                    {
                        "table_index": index,
                        "rows": len(df),
                        "columns": len(df.columns),
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
                        "column_names": [],
                        "preview": "",
                        "parse_error": str(exc),
                    }
                )

        return pd.DataFrame.from_records(records)

    def fetch_html(self, url: str, css_selector: str = "table") -> str:
        """Fetch page HTML with cloudscraper, optionally falling back to Selenium."""

        self._validate_url(url)
        self._validate_css_selector(css_selector)

        if self.config.use_selenium_first:
            return self._fetch_with_selenium(url=url, css_selector=css_selector)

        last_error: Optional[BaseException] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                return self._fetch_with_cloudscraper(url)
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
                    delay = self.config.retry_delay * (self.config.retry_backoff ** (attempt - 1))
                    time.sleep(delay)

        if self.config.use_selenium_fallback and HAVE_SELENIUM:
            self.logger.info("Trying Selenium fallback for %s", url)
            return self._fetch_with_selenium(url=url, css_selector=css_selector)

        raise RuntimeError(f"Failed to fetch HTML from {url}") from last_error

    def _fetch_with_cloudscraper(self, url: str) -> str:
        """Fetch with cloudscraper if installed; otherwise fall back to requests."""

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
                raise RuntimeError(
                    "Neither cloudscraper nor requests is installed. Install one with: "
                    "pip install cloudscraper requests"
                )

            self.logger.info("cloudscraper not installed. Fetching with requests: %s", url)
            response = requests.get(
                url,
                headers=headers,
                timeout=self.config.timeout,
            )

        response.raise_for_status()

        html = response.text
        if not html or not html.strip():
            raise ValueError(f"Empty response body from {url}")

        return html

    def _fetch_with_selenium(self, url: str, css_selector: str = "table") -> str:
        if not HAVE_SELENIUM:
            raise RuntimeError("Selenium is not installed. Install it with: pip install selenium")

        self.logger.info("Fetching with Selenium: %s", url)

        options = Options()

        if self.config.selenium_headless:
            options.add_argument("--headless=new")

        width, height = self.config.selenium_window_size
        options.add_argument(f"--window-size={width},{height}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        driver = None

        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(self.config.timeout)
            driver.get(url)

            if self.config.wait_time:
                try:
                    WebDriverWait(driver, self.config.wait_time).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector or "table"))
                    )
                except Exception:
                    self.logger.warning("Selenium wait finished without detecting selector: %s", css_selector)

            if self.config.selenium_extra_wait:
                time.sleep(self.config.selenium_extra_wait)

            html = driver.page_source
            if not html or not html.strip():
                raise ValueError(f"Empty Selenium page source from {url}")

            return html

        finally:
            if driver is not None:
                driver.quit()

    def _extract_tables_from_html(
        self,
        html: str,
        source: str,
        table_indices: Optional[Sequence[int]] = None,
        css_selector: str = "table",
        validation: Optional[TableValidationRules] = None,
        clean: bool = True,
    ) -> list[ScrapedTable]:
        """Shared extraction logic for URL HTML and raw HTML."""

        self._validate_html(html)
        self._validate_css_selector(css_selector)

        rules = validation or TableValidationRules()
        table_nodes = self._find_table_nodes(html=html, css_selector=css_selector)

        if not table_nodes:
            raise ValueError(f"No tables found using selector: {css_selector!r}")

        selected_indices = self._normalize_table_indices(
            table_indices=table_indices,
            table_count=len(table_nodes),
        )

        scraped_tables: list[ScrapedTable] = []

        for index in selected_indices:
            self.logger.info("Parsing table index %s from %s", index, source)
            df = self._parse_table_html(str(table_nodes[index]))

            if clean:
                df = self.clean_dataframe(df)

            self.validate_dataframe(
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

    def _find_table_nodes(self, html: str, css_selector: str = "table") -> list[Any]:
        """Find table nodes using the configured BeautifulSoup parser."""

        soup = BeautifulSoup(html, self.config.parser)
        return list(soup.select(css_selector or "table"))

    def _parse_table_html(self, table_html: str) -> pd.DataFrame:
        """Parse one HTML table string into a DataFrame."""

        kwargs: dict[str, Any] = {
            "header": self.config.read_html_header,
            "displayed_only": self.config.read_html_displayed_only,
            "decimal": self.config.decimal,
        }

        if self.config.thousands is not None:
            kwargs["thousands"] = self.config.thousands

        try:
            tables = pd.read_html(StringIO(table_html), **kwargs)
        except ValueError as exc:
            raise ValueError("pandas could not parse this table.") from exc

        if not tables:
            raise ValueError("pandas could not parse this table.")

        return tables[0]

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean a scraped DataFrame."""

        if df.empty:
            return df.copy()

        out = df.copy()

        if self.config.clean_column_names:
            out.columns = self._clean_columns(
                out.columns,
                snake_case=self.config.snake_case_columns,
            )

        out = self._remove_unnamed_columns(out)
        out = self._deduplicate_columns(out)
        out = self._strip_text_cells(out)

        if self.config.drop_empty_rows or self.config.drop_empty_columns:
            out = self._drop_empty_rows_and_columns(out)

        out = self._convert_numeric_like_columns(out)

        if self.config.drop_empty_rows or self.config.drop_empty_columns:
            out = self._drop_empty_rows_and_columns(out)

        return out.reset_index(drop=True)

    @classmethod
    def _clean_columns(cls, columns: Any, snake_case: bool = False) -> list[str]:
        """Clean regular Index or MultiIndex columns."""

        cleaned: list[str] = []

        for col in columns:
            if isinstance(col, tuple):
                parts = [
                    str(part).strip()
                    for part in col
                    if str(part).strip() and not str(part).strip().startswith("Unnamed")
                ]
                name = " ".join(parts)
            else:
                name = str(col).strip()

            name = WHITESPACE_PATTERN.sub(" ", name)

            if snake_case:
                name = cls._to_snake_case(name)

            cleaned.append(name or "column")

        return cleaned

    @staticmethod
    def _to_snake_case(text: str) -> str:
        """Convert a column name to snake_case."""

        text = text.strip()
        text = NON_SNAKE_PATTERN.sub("_", text)
        text = re.sub(r"_+", "_", text)
        text = text.strip("_")
        return text.lower() or "column"

    @staticmethod
    def _remove_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Remove columns named like pandas placeholder columns."""

        if df.empty:
            return df.copy()

        mask = ~pd.Series(df.columns).astype(str).str.match(r"^Unnamed", na=False).to_numpy()
        return df.loc[:, mask].copy()

    @staticmethod
    def _deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Rename duplicate columns instead of dropping them."""

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
    def _strip_text_cells(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize string/object columns and replace common empty markers with NA."""

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

    def _drop_empty_rows_and_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop fully empty rows and/or fully empty columns."""

        out = df.copy()

        if self.config.drop_empty_rows:
            out = out.dropna(axis=0, how="all")

        if self.config.drop_empty_columns:
            out = out.dropna(axis=1, how="all")

        return out

    def _convert_numeric_like_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert mostly numeric-looking columns."""

        out = df.copy()

        for col in out.columns:
            if not (pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col])):
                continue

            series = out[col].astype("string")
            non_missing = int(series.notna().sum())

            if non_missing == 0:
                continue

            has_percent = series.dropna().str.contains("%", regex=False).any()

            candidate = (
                series
                .str.replace(CURRENCY_PATTERN, "", regex=True)
                .str.replace("%", "", regex=False)
                .str.replace(r"^\((.*)\)$", r"-\1", regex=True)
                .str.replace(",", "", regex=False)
                .str.replace(r"^\+", "", regex=True)
                .str.strip()
            )

            converted = pd.to_numeric(candidate, errors="coerce")
            numeric_share = converted.notna().sum() / non_missing

            if numeric_share >= self.config.numeric_conversion_threshold:
                if has_percent and self.config.percent_as_decimal:
                    converted = converted / 100
                out[col] = converted

        return out

    @staticmethod
    def validate_dataframe(
        df: pd.DataFrame,
        rules: TableValidationRules,
        table_index: int,
        source: str,
    ) -> None:
        """Validate a DataFrame against table rules."""

        if df.shape[0] < rules.min_rows:
            raise ValueError(
                f"Table {table_index} from {source} has too few rows: "
                f"{df.shape[0]} < {rules.min_rows}"
            )

        if df.shape[1] < rules.min_columns:
            raise ValueError(
                f"Table {table_index} from {source} has too few columns: "
                f"{df.shape[1]} < {rules.min_columns}"
            )

        missing_columns = [column for column in rules.required_columns if column not in df.columns]

        if missing_columns:
            raise ValueError(
                f"Table {table_index} from {source} is missing required columns: {missing_columns}"
            )

        cell_count = df.shape[0] * df.shape[1]

        if cell_count:
            null_ratio = df.isna().sum().sum() / cell_count

            if null_ratio > rules.max_null_ratio:
                raise ValueError(
                    f"Table {table_index} from {source} has too many missing values: "
                    f"{null_ratio:.1%} > {rules.max_null_ratio:.1%}"
                )

    @staticmethod
    def _validate_url(url: str) -> None:
        """Validate that the URL is absolute and uses HTTP or HTTPS."""

        if not isinstance(url, str) or not url.strip():
            raise ValueError("URL cannot be empty.")

        parsed = urlparse(url.strip())

        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(
                f"Invalid URL: {url!r}. URL must be an absolute http:// or https:// URL."
            )

    @staticmethod
    def _validate_html(html: str) -> None:
        """Validate raw HTML input."""

        if not isinstance(html, str) or not html.strip():
            raise ValueError("HTML cannot be empty.")

    @staticmethod
    def _validate_css_selector(css_selector: str) -> None:
        """Validate a CSS selector argument."""

        if css_selector is None:
            raise ValueError("css_selector cannot be None.")

        if not isinstance(css_selector, str):
            raise TypeError("css_selector must be a string.")

        if not css_selector.strip():
            raise ValueError("css_selector cannot be empty.")

    @staticmethod
    def _normalize_table_indices(
        table_indices: Optional[Sequence[int]],
        table_count: int,
    ) -> list[int]:
        """Normalize and validate requested table indices."""

        if table_count < 0:
            raise ValueError("table_count must be >= 0")

        if table_indices is None:
            return list(range(table_count))

        selected = list(table_indices)

        if not selected:
            raise ValueError("table_indices cannot be empty.")

        normalized: list[int] = []

        for index in selected:
            if isinstance(index, bool) or not isinstance(index, int):
                raise TypeError(f"Table index must be an integer, got {type(index).__name__}")

            if index < 0 or index >= table_count:
                raise IndexError(f"Table index {index} is out of range. Found {table_count} table(s).")

            normalized.append(index)

        return normalized


def scrape_table(
    url: str,
    table_index: int = 0,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    config: Optional[ScraperConfig] = None,
) -> pd.DataFrame:
    """Scrape one table from a URL."""

    return WebTableDataFrameScraper(config=config).scrape_table(
        url=url,
        table_index=table_index,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
    )


def scrape_tables(
    url: str,
    table_indices: Optional[Sequence[int]] = None,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    config: Optional[ScraperConfig] = None,
) -> list[pd.DataFrame]:
    """Scrape multiple tables from a URL."""

    return WebTableDataFrameScraper(config=config).scrape_tables(
        url=url,
        table_indices=table_indices,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
    )


def scrape_table_with_metadata(
    url: str,
    table_index: int = 0,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    config: Optional[ScraperConfig] = None,
) -> ScrapedTable:
    """Scrape one table from a URL with metadata."""

    return WebTableDataFrameScraper(config=config).scrape_table_with_metadata(
        url=url,
        table_index=table_index,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
    )


def scrape_tables_with_metadata(
    url: str,
    table_indices: Optional[Sequence[int]] = None,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    config: Optional[ScraperConfig] = None,
) -> list[ScrapedTable]:
    """Scrape multiple tables from a URL with metadata."""

    return WebTableDataFrameScraper(config=config).scrape_tables_with_metadata(
        url=url,
        table_indices=table_indices,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
    )


def discover_tables(
    url: str,
    css_selector: str = "table",
    preview_rows: int = 5,
    clean: bool = True,
    config: Optional[ScraperConfig] = None,
) -> pd.DataFrame:
    """Discover available tables before extraction."""

    return WebTableDataFrameScraper(config=config).discover_tables(
        url=url,
        css_selector=css_selector,
        preview_rows=preview_rows,
        clean=clean,
    )


def parse_table_from_html(
    html: str,
    table_index: int = 0,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    source: str = "raw_html",
    config: Optional[ScraperConfig] = None,
) -> pd.DataFrame:
    """Parse one table from raw HTML."""

    return WebTableDataFrameScraper(config=config).parse_table_from_html(
        html=html,
        table_index=table_index,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
        source=source,
    )


def parse_table_from_html_with_metadata(
    html: str,
    table_index: int = 0,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    source: str = "raw_html",
    config: Optional[ScraperConfig] = None,
) -> ScrapedTable:
    """Parse one table from raw HTML with metadata."""

    return WebTableDataFrameScraper(config=config).parse_table_from_html_with_metadata(
        html=html,
        table_index=table_index,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
        source=source,
    )


def parse_tables_from_html(
    html: str,
    table_indices: Optional[Sequence[int]] = None,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    source: str = "raw_html",
    config: Optional[ScraperConfig] = None,
) -> list[pd.DataFrame]:
    """Parse multiple tables from raw HTML."""

    return WebTableDataFrameScraper(config=config).parse_tables_from_html(
        html=html,
        table_indices=table_indices,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
        source=source,
    )


def parse_tables_from_html_with_metadata(
    html: str,
    table_indices: Optional[Sequence[int]] = None,
    css_selector: str = "table",
    validation: Optional[TableValidationRules] = None,
    clean: bool = True,
    source: str = "raw_html",
    config: Optional[ScraperConfig] = None,
) -> list[ScrapedTable]:
    """Parse multiple tables from raw HTML with metadata."""

    return WebTableDataFrameScraper(config=config).parse_tables_from_html_with_metadata(
        html=html,
        table_indices=table_indices,
        css_selector=css_selector,
        validation=validation,
        clean=clean,
        source=source,
    )


def discover_tables_from_html(
    html: str,
    css_selector: str = "table",
    preview_rows: int = 5,
    clean: bool = True,
    config: Optional[ScraperConfig] = None,
) -> pd.DataFrame:
    """Discover tables inside raw HTML."""

    return WebTableDataFrameScraper(config=config).discover_tables_from_html(
        html=html,
        css_selector=css_selector,
        preview_rows=preview_rows,
        clean=clean,
    )


def require_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Raise a clear error if required DataFrame columns are missing."""

    missing = [column for column in columns if column not in df.columns]

    if missing:
        raise KeyError(f"Missing required DataFrame columns: {missing}")


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
    """
    Return a copy of df with calculated columns added after checking required columns.

    Example:
        df = safe_with_columns(
            df,
            required_columns=["Price", "Quantity"],
            Total=lambda x: x["Price"] * x["Quantity"],
        )
    """

    require_columns(df, required_columns)
    return with_columns(df, **columns)

def print_table_discovery(discovery: pd.DataFrame) -> None:
    """Print all discovered tables with their preview rows."""

    print("\n" + "=" * 90)
    print("BULUNAN TABLOLAR")
    print("=" * 90)

    for _, row in discovery.iterrows():
        print("\n" + "-" * 90)
        print(f"TABLE INDEX: {row['table_index']}")
        print(f"SATIR SAYISI: {row['rows']}")
        print(f"KOLON SAYISI: {row['columns']}")
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


def print_current_data(df: pd.DataFrame, url: str, table_index: int, preview_rows: int) -> None:
    """Print the current scraped DataFrame and metadata."""

    print("\n" + "=" * 90)
    print("GÜNCEL DATA")
    print("=" * 90)
    print(df.head(preview_rows))

    print("\n" + "=" * 90)
    print("DATA BİLGİSİ")
    print("=" * 90)
    print(f"Kaynak link: {url}")
    print(f"Seçilen tablo index: {table_index}")
    print(f"Satır sayısı: {len(df)}")
    print(f"Kolon sayısı: {len(df.columns)}")
    print("Kolonlar / Headers:")
    for col_index, col_name in enumerate(df.columns):
        print(f"  {col_index}: {col_name}")


def ask_yes_no(question: str, default: bool = False) -> bool:
    """Ask a yes/no question in the terminal."""

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


def build_excel_output_path(url: str, table_index: int) -> Path:
    """Create a safe Excel output path on the user's Desktop."""

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

    raise RuntimeError(
        "Excel çıktısı için xlsxwriter veya openpyxl kurulu olmalı. "
        "Kurulum: pip install xlsxwriter openpyxl"
    )


def write_dataframe_to_excel(
    df: pd.DataFrame,
    output_path: Path,
    url: str,
    table_index: int,
    auto_refresh: bool,
) -> None:
    """Write the latest DataFrame to an Excel file."""

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
                "Mode",
            ],
            "Value": [
                url,
                table_index,
                scraped_at,
                len(df),
                len(df.columns),
                "Live / auto-refresh" if auto_refresh else "Static / one-time",
            ],
        }
    )

    with pd.ExcelWriter(output_path, engine=engine) as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
        metadata.to_excel(writer, sheet_name="Metadata", index=False)

        if engine == "xlsxwriter":
            workbook = writer.book
            data_sheet = writer.sheets["Data"]
            metadata_sheet = writer.sheets["Metadata"]

            header_format = workbook.add_format({"bold": True, "text_wrap": True})

            for col_num, col_name in enumerate(df.columns):
                data_sheet.write(0, col_num, col_name, header_format)
                series = df[col_name].astype("string")
                max_len = max(
                    [len(str(col_name))] + [len(value) for value in series.dropna().head(100).tolist()]
                )
                data_sheet.set_column(col_num, col_num, min(max(max_len + 2, 10), 40))

            data_sheet.freeze_panes(1, 0)
            data_sheet.autofilter(0, 0, max(len(df), 1), max(len(df.columns) - 1, 0))

            metadata_sheet.set_column(0, 0, 18)
            metadata_sheet.set_column(1, 1, 80)

        elif engine == "openpyxl":
            data_sheet = writer.sheets["Data"]
            metadata_sheet = writer.sheets["Metadata"]

            data_sheet.freeze_panes = "A2"
            if df.shape[1] > 0:
                data_sheet.auto_filter.ref = data_sheet.dimensions

            for column_cells in data_sheet.columns:
                header = column_cells[0].value
                values = [cell.value for cell in column_cells[1:101]]
                max_len = max([len(str(header))] + [len(str(value)) for value in values if value is not None])
                data_sheet.column_dimensions[column_cells[0].column_letter].width = min(max(max_len + 2, 10), 40)

            metadata_sheet.column_dimensions["A"].width = 18
            metadata_sheet.column_dimensions["B"].width = 80


if __name__ == "__main__":

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    # ============================================================
    # SEÇİMLER
    # ============================================================
    # Bu bölümde sadece istediğin ayarları değiştirmen yeterli.

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
    percent_as_decimal = True

    # Sayısal gibi görünen kolonları sayıya çevirme eşiği.
    # 0.85 demek: kolonun %85'i sayıya benziyorsa sayıya çevir.
    numeric_conversion_threshold = 0.85

    # Sayfa çekme timeout süresi.
    timeout = 60

    # Hata olursa kaç kere tekrar denensin?
    max_retries = 3

    # Tekrar denemeler arasında kaç saniye beklensin?
    retry_delay = 3.0

    # JavaScript ile yüklenen sitelerde Selenium gerekebilir.
    # Önce Selenium kullanılsın mı?
    use_selenium_first = False

    # cloudscraper başarısız olursa Selenium yedek olarak kullanılsın mı?
    use_selenium_fallback = True

    # Selenium tarayıcıyı görünmeden çalıştırsın mı?
    selenium_headless = True

    # Selenium sayfada tabloyu kaç saniye beklesin?
    wait_time = 10

    # Data kaç saniyede bir güncellensin?
    refresh_seconds = 10

    # Sürekli güncellensin mi?
    # True: belirlediğin saniyede bir tekrar çeker.
    # False: sadece bir kere çeker ve durur.
    auto_refresh = False

    # Güncel datada ekrana kaç satır yazdırılsın?
    preview_rows = 10

    # Log seviyesi:
    # "INFO", "WARNING", "ERROR" kullanılabilir.
    log_level = "INFO"

    # ============================================================
    # AYARLARI OLUŞTUR
    # ============================================================

    config = ScraperConfig(
        timeout=timeout,
        wait_time=wait_time,
        max_retries=max_retries,
        retry_delay=retry_delay,
        use_selenium_first=use_selenium_first,
        use_selenium_fallback=use_selenium_fallback,
        selenium_headless=selenium_headless,
        numeric_conversion_threshold=numeric_conversion_threshold,
        percent_as_decimal=percent_as_decimal,
        clean_column_names=clean_column_names,
        snake_case_columns=snake_case_columns,
        drop_empty_rows=drop_empty_rows,
        drop_empty_columns=drop_empty_columns,
        log_level=log_level,
    )

    # ============================================================
    # ÖNCE BÜTÜN TABLOLARI ÖNİZLE
    # ============================================================

    try:
        discovery = discover_tables(
            url=url,
            css_selector=css_selector,
            preview_rows=preview_rows_for_selection,
            clean=clean,
            config=config,
        )

        if discovery.empty:
            raise ValueError("Bu linkte tablo bulunamadı.")

        print_table_discovery(discovery)

        valid_indices = set(discovery["table_index"].dropna().astype(int).tolist())

        # ========================================================
        # TABLO SEÇİMİ
        # ========================================================
        # table_index None ise kullanıcıdan input() ile alır.
        # table_index sayı ise onu kullanır ve geçerli mi diye kontrol eder.

        if table_index is None:
            table_index = ask_table_index(valid_indices)
        else:
            if not isinstance(table_index, int):
                raise TypeError("table_index int veya None olmalı. Örnek: table_index = None")

            if table_index not in valid_indices:
                raise ValueError(
                    f"Seçilen table_index geçersiz: {table_index}. "
                    f"Seçilebilir indexler: {sorted(valid_indices)}"
                )

    except KeyboardInterrupt:
        print("\nProgram kullanıcı tarafından durduruldu.")
        raise SystemExit

    except Exception as error:
        print("\n" + "=" * 90)
        print("TABLOLAR ÖNİZLENİRKEN HATA OLUŞTU")
        print("=" * 90)
        print(error)
        raise SystemExit

    # ============================================================
    # EXCEL ÇIKTISI SEÇİMİ
    # ============================================================

    excel_output = ask_yes_no("Excel output istiyor musun?", default=False)
    excel_path: Optional[Path] = None

    if excel_output:
        excel_path = build_excel_output_path(url=url, table_index=table_index)
        print(f"Excel dosyası burada oluşturulacak: {excel_path}")

    # ============================================================
    # SEÇİLEN TABLOYU ÇALIŞTIR VE GÜNCELLE
    # ============================================================

    while True:
        try:
            df = scrape_table(
                url=url,
                table_index=table_index,
                css_selector=css_selector,
                clean=clean,
                config=config,
            )

            print_current_data(
                df=df,
                url=url,
                table_index=table_index,
                preview_rows=preview_rows,
            )

            if excel_output and excel_path is not None:
                write_dataframe_to_excel(
                    df=df,
                    output_path=excel_path,
                    url=url,
                    table_index=table_index,
                    auto_refresh=auto_refresh,
                )
                print(f"\nExcel güncellendi: {excel_path}")

        except KeyboardInterrupt:
            print("\nProgram kullanıcı tarafından durduruldu.")
            break

        except Exception as error:
            print("\n" + "=" * 90)
            print("HATA OLUŞTU")
            print("=" * 90)
            print(error)

        if not auto_refresh:
            break

        if refresh_seconds <= 0:
            print("\nrefresh_seconds 0 veya negatif olamaz. Program durduruldu.")
            break

        print(f"\n{refresh_seconds} saniye sonra data tekrar güncellenecek...\n")
        time.sleep(refresh_seconds)
