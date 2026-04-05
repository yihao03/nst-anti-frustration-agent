#!/usr/bin/env python3
"""DBS Support page scraper - recursively scrapes all help/support articles and saves as markdown files."""

import os
import re
import time
import json
import hashlib
import logging
from urllib.parse import urljoin, urlparse
from collections import deque

import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

# Configuration
BASE_URL = "https://www.dbs.com.sg/personal/support/home.html"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
DELAY_BETWEEN_REQUESTS = 1.5
REQUEST_TIMEOUT = 30
MAX_DEPTH = 5
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# State
visited_urls: dict[str, dict] = {}
url_queue: deque[dict] = deque()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Session for connection pooling
session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
})


def normalize_url(url: str) -> str:
    """Normalize URL for deduplication - strip fragments, lowercase, sort query params."""
    parsed = urlparse(url)
    query = "&".join(sorted(parsed.query.split("&"))) if parsed.query else ""
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if query:
        normalized += f"?{query}"
    return normalized.lower().rstrip("/")


def url_hash(url: str) -> str:
    """Generate a short hash for a URL for quick lookup."""
    return hashlib.md5(normalize_url(url).encode()).hexdigest()[:12]


def should_follow_url(url: str, current_depth: int) -> bool:
    """Check if a URL should be followed based on scope and depth."""
    if current_depth >= MAX_DEPTH:
        return False

    parsed = urlparse(url)
    
    # Must be same domain as base URL
    base_netloc = urlparse(BASE_URL).netloc
    if parsed.netloc != base_netloc:
        return False

    # Skip non-HTML resources
    skip_extensions = (".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".pdf", ".ico", ".woff", ".ttf")
    if parsed.path.lower().endswith(skip_extensions):
        return False

    # Skip javascript/mailto/anchors
    if parsed.scheme not in ("http", "https", ""):
        return False

    # Skip known non-content paths
    skip_patterns = [
        "/content/dam/",
        "/wps/",
        "/api/",
        ".page?",
    ]
    if any(pattern in parsed.path.lower() for pattern in skip_patterns):
        return False

    return True


def sanitize_filename(name: str) -> str:
    """Convert a title to a safe filename."""
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s]+", "-", name)
    name = name.strip("-")
    return name[:100] if name else "untitled"


def extract_links(html: str, base_url: str) -> list[dict]:
    """Extract all relevant links from a page."""
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]

        # Skip non-content links
        if any(skip in href.lower() for skip in [
            "javascript:", "mailto:", "#",
        ]):
            continue

        # Build full URL
        if href.startswith("http"):
            full_url = href
        elif href.startswith("//"):
            full_url = "https:" + href
        else:
            full_url = urljoin(base_url, href)

        # Get link text for title
        title = a_tag.get_text(strip=True)
        if not title:
            img = a_tag.find("img")
            if img and img.get("alt"):
                title = img["alt"]

        title_attr = a_tag.get("title", "")
        if title_attr and len(title_attr) > len(title):
            title = title_attr

        links.append({
            "url": full_url,
            "title": title,
        })

    return links


def fetch_page(url: str) -> str | None:
    """Fetch a page and return its HTML content."""
    try:
        logger.info(f"Fetching: {url}")
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def extract_article_content(html: str) -> dict:
    """Extract the main article content from a page."""
    soup = BeautifulSoup(html, "html.parser")

    # Try to find the main content area
    content_area = None

    selectors = [
        "article",
        "main",
        ".content",
        "#content",
        ".article-content",
        ".main-content",
        ".support-content",
        ".faq-content",
        ".page-content",
    ]

    for selector in selectors:
        content_area = soup.select_one(selector)
        if content_area and len(content_area.get_text(strip=True)) > 100:
            break

    if not content_area or len(content_area.get_text(strip=True)) < 50:
        content_area = soup.find("body")

    # Extract title
    title = None
    title_selectors = ["h1", ".page-title", ".article-title", ".title"]
    for selector in title_selectors:
        title_el = soup.select_one(selector)
        if title_el:
            title = title_el.get_text(strip=True)
            if title:
                break

    if not title:
        title = soup.title.get_text(strip=True) if soup.title else "Untitled"

    # Clean up content
    if content_area:
        for tag in content_area.find_all(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        for tag in content_area.find_all(class_=re.compile(r"nav|menu|breadcrumb|header|footer", re.I)):
            tag.decompose()

        content_html = str(content_area)
    else:
        content_html = str(html)

    # Convert to markdown
    content_md = md(
        content_html,
        heading_style="ATX",
        bullets="-",
        strip=["img"],
    )

    content_md = re.sub(r"\n{3,}", "\n\n", content_md)
    content_md = content_md.strip()

    return {
        "title": title,
        "content": content_md,
    }


def save_markdown(title: str, content: str, url: str, depth: int, output_path: str):
    """Save content as a markdown file with frontmatter."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    frontmatter = f"""---
title: "{title}"
source_url: "{url}"
scraped_date: "{time.strftime('%Y-%m-%d %H:%M:%S')}"
depth: {depth}
---

"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(frontmatter)
        f.write(content)

    logger.info(f"Saved: {output_path}")


def save_visited_urls():
    """Save visited URLs manifest for resume/debugging."""
    manifest_path = os.path.join(OUTPUT_DIR, "_visited_urls.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(visited_urls, f, indent=2)
    logger.info(f"Saved visited URLs manifest: {manifest_path}")


def main():
    """Main recursive scraper function."""
    logger.info("Starting DBS Support recursive scraper...")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Max depth: {MAX_DEPTH}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Seed queue with base URL
    url_queue.append({
        "url": BASE_URL,
        "title": "DBS Help & Support",
        "depth": 0,
    })

    success_count = 0
    fail_count = 0
    skipped_count = 0
    processed = 0

    while url_queue:
        item = url_queue.popleft()
        url = item["url"]
        depth = item["depth"]
        title_hint = item.get("title", "")

        # Check if already visited
        normalized = normalize_url(url)
        uhash = url_hash(url)

        if uhash in visited_urls:
            skipped_count += 1
            continue

        # Mark as visited
        visited_urls[uhash] = {
            "url": url,
            "normalized": normalized,
            "depth": depth,
            "title": title_hint,
            "status": "pending",
        }

        # Fetch page
        html = fetch_page(url)
        if not html:
            visited_urls[uhash]["status"] = "failed"
            fail_count += 1
            continue

        processed += 1
        logger.info(f"Processing [{processed}] (depth={depth}): {title_hint or url}")

        # Extract content
        article = extract_article_content(html)

        # Generate filename
        filename = sanitize_filename(article["title"]) + ".md"
        output_path = os.path.join(OUTPUT_DIR, filename)

        # Handle duplicate filenames
        counter = 1
        while os.path.exists(output_path):
            filename = f"{sanitize_filename(article['title'])}-{counter}.md"
            output_path = os.path.join(OUTPUT_DIR, filename)
            counter += 1

        # Save
        save_markdown(
            title=article["title"],
            content=article["content"],
            url=url,
            depth=depth,
            output_path=output_path,
        )

        visited_urls[uhash]["status"] = "success"
        visited_urls[uhash]["title"] = article["title"]
        visited_urls[uhash]["output"] = output_path
        success_count += 1

        # Extract and queue new links
        if depth < MAX_DEPTH:
            links = extract_links(html, url)
            new_links = 0

            for link in links:
                link_url = link["url"]

                if not should_follow_url(link_url, depth):
                    continue

                link_normalized = normalize_url(link_url)
                link_hash = url_hash(link_url)

                if link_hash not in visited_urls:
                    url_queue.append({
                        "url": link_url,
                        "title": link["title"],
                        "depth": depth + 1,
                    })
                    new_links += 1

            if new_links > 0:
                logger.info(f"  Found {new_links} new links at depth {depth + 1}")

        # Save manifest periodically
        if processed % 10 == 0:
            save_visited_urls()

        # Be polite
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Final manifest
    save_visited_urls()

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info(f"Scraping complete!")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {fail_count}")
    logger.info(f"  Skipped (duplicates): {skipped_count}")
    logger.info(f"  Total unique URLs visited: {len(visited_urls)}")
    logger.info(f"  Output directory: {OUTPUT_DIR}")
    logger.info(f"{'='*50}")


if __name__ == "__main__":
    main()
