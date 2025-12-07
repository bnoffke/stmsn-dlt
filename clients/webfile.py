"""
Web file client for URL discovery and file downloading.

Provides functionality to:
- Discover available files from web URLs (via directory listing or pattern probing)
- Download files to local temp directory
- Extract partition values from filenames
"""

import hashlib
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import requests


class DownloadError(Exception):
    """Raised when file download fails."""

    pass


class DiscoveryError(Exception):
    """Raised when URL discovery fails."""

    pass


@dataclass
class DiscoveredFile:
    """Represents a discovered file with its metadata."""

    url: str
    filename: str
    partition_values: Dict[str, str]

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        return self.url == other.url


class LinkExtractor(HTMLParser):
    """HTML parser to extract href links from anchor tags."""

    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)


class WebFileClient:
    """
    Client for discovering and downloading files from web URLs.

    Supports two discovery strategies:
    1. Directory listing: Parse HTML index page for matching files
    2. Pattern probing: Try URL patterns for different years
    """

    DEFAULT_TIMEOUT = 30
    DEFAULT_RETRIES = 3
    DEFAULT_RETRY_DELAY = 1.0
    MIN_YEAR = 2010  # Don't probe years before this

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
    ):
        """
        Initialize the web file client.

        Args:
            timeout: Request timeout in seconds
            retries: Number of retry attempts for failed requests
            retry_delay: Delay between retries in seconds
        """
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "stmsn-dlt/1.0 (Data Pipeline; +https://github.com/stmsn)"
            }
        )

    def _request_with_retry(
        self, method: str, url: str, **kwargs
    ) -> requests.Response:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, HEAD, etc.)
            url: URL to request
            **kwargs: Additional arguments for requests

        Returns:
            Response object

        Raises:
            DownloadError: If all retries fail
        """
        kwargs.setdefault("timeout", self.timeout)
        last_error = None

        for attempt in range(self.retries):
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                last_error = e
                if attempt < self.retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))

        raise DownloadError(f"Failed to {method} {url} after {self.retries} attempts: {last_error}")

    def _check_url_exists(self, url: str) -> bool:
        """
        Check if a URL exists using HEAD request.

        Args:
            url: URL to check

        Returns:
            True if URL exists (2xx response), False otherwise
        """
        try:
            response = self.session.head(url, timeout=self.timeout, allow_redirects=True)
            return response.status_code < 400
        except requests.RequestException:
            return False

    def _extract_partition_values(
        self, filename: str, partition_config: List[Dict]
    ) -> Dict[str, str]:
        """
        Extract partition values from filename using configured patterns.

        Args:
            filename: Filename to extract from
            partition_config: List of partition field configurations

        Returns:
            Dict of field name to extracted value
        """
        values = {}
        for config in partition_config:
            field = config["field"]
            pattern = config["pattern"]
            match = re.search(pattern, filename)
            if match:
                values[field] = match.group(0)
        return values

    def _discover_from_directory(
        self,
        base_url: str,
        url_patterns: List[str],
        partition_config: List[Dict],
    ) -> List[DiscoveredFile]:
        """
        Discover files by parsing HTML directory listing.

        Args:
            base_url: Base URL to fetch directory listing from
            url_patterns: Patterns to match against found links
            partition_config: Partition configuration for value extraction

        Returns:
            List of discovered files
        """
        try:
            response = self._request_with_retry("GET", base_url)
            content_type = response.headers.get("Content-Type", "")

            # Only parse HTML responses
            if "text/html" not in content_type:
                return []

            # Parse HTML for links
            parser = LinkExtractor()
            parser.feed(response.text)

            discovered = []
            seen_urls: Set[str] = set()

            # Build regex patterns from URL patterns (replace {year}, {month} with wildcards)
            regex_patterns = []
            for pattern in url_patterns:
                # Convert pattern to regex
                regex = pattern.replace("{year}", r"\d{4}").replace("{month}", r"\d{2}")
                regex_patterns.append(re.compile(regex, re.IGNORECASE))

            for link in parser.links:
                # Check if link matches any pattern
                for regex in regex_patterns:
                    if regex.search(link):
                        # Construct full URL
                        full_url = urljoin(base_url, link)
                        if full_url in seen_urls:
                            continue
                        seen_urls.add(full_url)

                        # Extract filename from URL
                        filename = link.split("/")[-1]

                        # Extract partition values
                        partition_values = self._extract_partition_values(
                            filename, partition_config
                        )

                        if partition_values:  # Only include if we extracted partitions
                            discovered.append(
                                DiscoveredFile(
                                    url=full_url,
                                    filename=filename,
                                    partition_values=partition_values,
                                )
                            )
                        break

            return discovered

        except DownloadError:
            return []

    def _discover_from_patterns(
        self,
        base_url: str,
        url_patterns: List[str],
        partition_config: List[Dict],
        start_year: Optional[int] = None,
    ) -> List[DiscoveredFile]:
        """
        Discover files by probing URL patterns for different years.

        Args:
            base_url: Base URL to append filenames to
            url_patterns: URL patterns with {year} placeholder
            partition_config: Partition configuration for value extraction
            start_year: Year to start probing from (default: current year)

        Returns:
            List of discovered files
        """
        if start_year is None:
            start_year = datetime.now().year

        discovered = []
        seen_urls: Set[str] = set()

        # Probe years from start_year backwards
        year = start_year
        consecutive_misses = 0
        max_consecutive_misses = 3  # Stop after 3 years with no files

        while year >= self.MIN_YEAR and consecutive_misses < max_consecutive_misses:
            found_for_year = False

            for pattern in url_patterns:
                filename = pattern.format(year=year, month="01")  # Default month
                url = urljoin(base_url, filename)

                if url in seen_urls:
                    continue

                if self._check_url_exists(url):
                    seen_urls.add(url)
                    partition_values = self._extract_partition_values(
                        filename, partition_config
                    )

                    discovered.append(
                        DiscoveredFile(
                            url=url,
                            filename=filename,
                            partition_values=partition_values,
                        )
                    )
                    found_for_year = True
                    break  # Found file for this year, move to next year

            if found_for_year:
                consecutive_misses = 0
            else:
                consecutive_misses += 1

            year -= 1

        return discovered

    def discover_files(
        self,
        base_url: str,
        url_patterns: List[str],
        partition_config: List[Dict],
        start_year: Optional[int] = None,
    ) -> List[DiscoveredFile]:
        """
        Discover available files using directory listing or pattern probing.

        Tries directory listing first, falls back to pattern probing.

        Args:
            base_url: Base URL for the files
            url_patterns: URL patterns with placeholders like {year}
            partition_config: Partition configuration for value extraction
            start_year: Year to start probing from (default: current year)

        Returns:
            List of discovered files, sorted by partition values (newest first)
        """
        # Try directory listing first
        discovered = self._discover_from_directory(
            base_url, url_patterns, partition_config
        )

        # Fall back to pattern probing if no files found
        if not discovered:
            discovered = self._discover_from_patterns(
                base_url, url_patterns, partition_config, start_year
            )

        # Sort by partition values (assuming year is primary partition)
        discovered.sort(
            key=lambda f: tuple(f.partition_values.values()),
            reverse=True,
        )

        return discovered

    def download_file(
        self, url: str, dest_dir: Optional[Path] = None
    ) -> tuple[Path, str]:
        """
        Download a file from URL to local filesystem.

        Args:
            url: URL to download
            dest_dir: Destination directory (default: system temp)

        Returns:
            Tuple of (local file path, MD5 hash of file)

        Raises:
            DownloadError: If download fails
        """
        if dest_dir is None:
            dest_dir = Path(tempfile.gettempdir())

        # Extract filename from URL
        filename = url.split("/")[-1]
        local_path = dest_dir / filename

        print(f"  Downloading {url}...")

        response = self._request_with_retry("GET", url, stream=True)

        # Calculate hash while writing
        hasher = hashlib.md5()
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        last_pct = -10  # Track last percentage to only print every 10%

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    hasher.update(chunk)
                    downloaded += len(chunk)

                    # Progress indication for large files (every 10%)
                    if total_size > 0:
                        pct = int((downloaded / total_size) * 100)
                        if pct >= last_pct + 10:
                            print(f"  Progress: {pct}%")
                            last_pct = pct

        if total_size > 0 and last_pct < 100:
            print("  Progress: 100%")

        file_hash = hasher.hexdigest()
        print(f"  Downloaded {local_path.name} ({downloaded:,} bytes, hash: {file_hash[:8]}...)")

        return local_path, file_hash

    def close(self):
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
