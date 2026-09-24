# Web-Table Scraper

`web_table_scraper.py` extracts an HTML table from a web page into a polars DataFrame and saves it to DuckDB. It can scrape once, or keep polling a live page, such as a page of FX quotes.

- **Static mode:** scrape once, write a DuckDB table, and exit.
- **Live mode:** poll every `LIVE_POLL_SECONDS`, keeping the latest snapshot in a SQLite "hot" file. On Ctrl+C the final snapshot is flushed into the DuckDB table.
- Tables are parsed directly from the HTML with BeautifulSoup, without pandas. Headers are cleaned, empty rows and columns dropped, and numbers, percentages and locale-formatted values (`1,234.56` / `1.234,56`) converted.
- JavaScript-rendered pages fall back to headless Chrome through Selenium.
- Retries with back-off, an optional robots.txt check, and an optional short-lived page cache.

## Install

```sh
git clone https://github.com/Chaox07/Web-Table-Scraper.git
cd Web-Table-Scraper
conda create -n scraper -c conda-forge python=3.14 requests polars pyarrow python-duckdb beautifulsoup4 lxml
```

Optional:

- `conda install -n scraper -c conda-forge httpx` for async bulk scraping.
- `conda install -n scraper -c conda-forge selenium` for JavaScript-rendered pages. It needs Chrome; Selenium Manager fetches the driver.
- `conda run -n scraper pip install cloudscraper` for Cloudflare-protected pages. It is not on conda-forge.

## Running it

The settings are at the top of `web_table_scraper.py`:

- `URL`: the page to scrape.
- `TABLE_INDEX`: which table on the page (0-based).
- `MODE`: `"static"` or `"live"`, plus `LIVE_POLL_SECONDS` for live mode.
- `OUTPUT_DIR`: where the files go. Defaults to the script's own folder.
- Advanced: timeouts and retries, `RESPECT_ROBOTS_TXT` (off by default), the cache, Selenium, the cleaning options and `NUMBER_LOCALE`.

```sh
conda activate scraper
python web_table_scraper.py
```

To find the right `TABLE_INDEX`, list the tables on a page first:

```python
import web_table_scraper as w
scraper = w.WebTableDataFrameScraper(w.ScraperConfig())
print(scraper.discover_tables("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"))
```

On the default URL, the GDP table is index 2. With the shipped `TABLE_INDEX = 0`, that page stops with "Table 0 has too few rows".

The output is `<site>_t<index>.duckdb`, which holds one table of the same name. Live mode also writes `<site>_t<index>_hot.sqlite`.

## Licence

MIT, see [LICENSE](LICENSE).
