#!/usr/bin/env python3
"""
AASLD Practice Guidelines Scraper - Complete Workflow
HTML + PDF Processing with Full Content in JSON
"""

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict

import requests
from bs4 import BeautifulSoup, Tag, NavigableString
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urljoin, urlparse

# PDF processing
try:
    import PyPDF2
    PDF_SUPPORT = True
except ImportError:
    print("⚠ PyPDF2 not installed. Run: pip install PyPDF2")
    PDF_SUPPORT = False

# Configuration
MAIN_URL = "https://www.aasld.org/practice-guidelines"
RATE_LIMIT_REQUESTS = 1.5
RATE_LIMIT_SELENIUM = 5.0
CLOUDFLARE_WAIT = 15
SELENIUM_TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

DATA_DIR = Path("data")
META_DIR = DATA_DIR / "metadata"
JSON_DIR = DATA_DIR / "json"
TEXT_DIR = DATA_DIR / "text_content"
PDF_DIR = DATA_DIR / "pdfs"

for d in [META_DIR, JSON_DIR, TEXT_DIR, PDF_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LINKS_FILE = META_DIR / "second_level_links.txt"
HEADING_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6"]

REJECT_PATTERNS = [
    "/forums", "/home", "/about", "/contact", "/subscribe",
    "facebook.com", "twitter.com", "linkedin.com", "youtube.com"
]

DRIVER = None

# ============================================================================
# SELENIUM DRIVER MANAGEMENT
# ============================================================================

def init_selenium_driver():
    """Initialize Selenium driver"""
    global DRIVER
    
    if DRIVER is None:
        try:
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            service = Service(ChromeDriverManager().install())
            DRIVER = webdriver.Chrome(service=service, options=options)
            DRIVER.set_page_load_timeout(SELENIUM_TIMEOUT)
            DRIVER.set_script_timeout(SELENIUM_TIMEOUT)
            
            DRIVER.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            print("✓ Selenium driver initialized")
        except Exception as e:
            print(f"✗ Failed to initialize driver: {e}")
            DRIVER = None
            raise
    
    return DRIVER

def close_selenium_driver():
    """Close Selenium driver"""
    global DRIVER
    if DRIVER:
        try:
            DRIVER.quit()
        except:
            pass
        DRIVER = None

def reset_selenium_driver():
    """Reset driver if it crashes"""
    global DRIVER
    close_selenium_driver()
    time.sleep(2)
    init_selenium_driver()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_cloudflare_challenge(html: str) -> bool:
    """Check if page is Cloudflare challenge"""
    return "Just a moment" in html or "Verify you are human" in html or "Checking your browser" in html

def clean_url(u: str) -> str:
    return u.split("#")[0].rstrip("/")

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()

def sha256_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def heading_level(tag_name: Optional[str]) -> int:
    try:
        if tag_name and tag_name.lower().startswith("h") and tag_name[1].isdigit():
            return int(tag_name[1])
    except Exception:
        pass
    return 7

# ============================================================================
# PDF PROCESSING
# ============================================================================

def download_pdf(url: str) -> Optional[Path]:
    """Download PDF from URL"""
    try:
        print(f"    → Downloading PDF...")
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            url_hash = sha256_hash(url)
            pdf_path = PDF_DIR / f"{url_hash}.pdf"
            
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            
            print(f"    ✓ PDF downloaded ({len(response.content)} bytes)")
            return pdf_path
        else:
            print(f"    ✗ Failed to download PDF (status: {response.status_code})")
            return None
            
    except Exception as e:
        print(f"    ✗ Download error: {str(e)[:40]}")
        return None

def parse_pdf_into_paragraphs(full_text: str) -> List[str]:
    """Split PDF text into paragraphs"""
    paragraphs = []
    
    # Clean the text
    text = re.sub(r'\s+', ' ', full_text)
    
    # Split by sentence endings followed by capital letters
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
    
    current_para = []
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) > 20:
            current_para.append(sentence)
            
            # Group 3-5 sentences as one paragraph
            if len(current_para) >= 3:
                paragraphs.append(' '.join(current_para))
                current_para = []
    
    # Add remaining
    if current_para:
        paragraphs.append(' '.join(current_para))
    
    return paragraphs

def extract_text_from_pdf(pdf_path: Path) -> Dict:
    """Extract text from PDF"""
    if not PDF_SUPPORT:
        return {
            "full_text": "",
            "page_count": 0,
            "error": "PyPDF2 not installed"
        }
    
    try:
        print(f"    → Extracting text from PDF...")
        
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            page_count = len(pdf_reader.pages)
            
            text_pages = []
            for page_num in range(page_count):
                try:
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    if text:
                        text_pages.append(text)
                except Exception as e:
                    print(f"    ⚠ Error on page {page_num + 1}")
                    continue
            
            full_text = "\n\n".join(text_pages)
            full_text_clean = re.sub(r'\s+', ' ', full_text)
            
            paragraphs = parse_pdf_into_paragraphs(full_text_clean)
            
            print(f"    ✓ Extracted {len(full_text_clean)} characters, {len(paragraphs)} paragraphs")
            
            return {
                "full_text": full_text_clean,
                "page_count": page_count,
                "word_count": len(full_text_clean.split()),
                "char_count": len(full_text_clean),
                "paragraphs": paragraphs,
                "paragraph_count": len(paragraphs)
            }
            
    except Exception as e:
        print(f"    ✗ PDF extraction error: {str(e)[:40]}")
        return {
            "full_text": "",
            "page_count": 0,
            "error": str(e)
        }

def process_pdf(url: str) -> Optional[Dict]:
    """Download and process PDF"""
    pdf_path = download_pdf(url)
    
    if not pdf_path:
        return None
    
    extracted_data = extract_text_from_pdf(pdf_path)
    
    if not extracted_data["full_text"]:
        print(f"    ⚠ No text extracted from PDF")
        return None
    
    title = url.split("/")[-1].replace(".pdf", "").replace("_", " ").replace("-", " ")
    
    return {
        "title": title,
        "full_text": extracted_data["full_text"],
        "full_text_length": extracted_data["char_count"],
        "word_count": extracted_data["word_count"],
        "page_count": extracted_data["page_count"],
        "paragraphs": extracted_data.get("paragraphs", []),
        "paragraph_count": extracted_data.get("paragraph_count", 0),
        "pdf_path": str(pdf_path)
    }

def save_pdf_data(url: str, extracted_data: Dict) -> tuple:
    """Save PDF data to JSON"""
    url_hash = sha256_hash(url)
    
    json_record = {
        "page_url": url,
        "page_title": extracted_data["title"],
        "content_type": "pdf",
        "crawled_at": datetime.now(timezone.utc).isoformat(),
        "content": {
            "full_text": extracted_data["full_text"],
            "full_text_length": extracted_data["full_text_length"],
            "word_count": extracted_data["word_count"],
            "page_count": extracted_data["page_count"],
            "paragraph_count": extracted_data["paragraph_count"],
            "paragraphs": extracted_data["paragraphs"],
            "pdf_path": extracted_data["pdf_path"]
        },
        "accessible": True
    }
    
    json_file = JSON_DIR / f"{url_hash}.json"
    json_file.write_text(json.dumps(json_record, indent=2, ensure_ascii=False), encoding="utf-8")
    
    text_file = TEXT_DIR / f"{url_hash}.txt"
    text_file.write_text(extracted_data["full_text"], encoding="utf-8")
    
    return json_file, text_file

# ============================================================================
# FETCH FUNCTIONS
# ============================================================================

def fetch_with_selenium(url: str, retry_count: int = 0) -> Optional[str]:
    """Fetch URL using Selenium"""
    try:
        driver = init_selenium_driver()
        
        time.sleep(RATE_LIMIT_SELENIUM)
        print(f"    → Loading with Selenium...")
        driver.get(url)
        
        time.sleep(3)
        
        if is_cloudflare_challenge(driver.page_source):
            print(f"    → Cloudflare detected, waiting {CLOUDFLARE_WAIT}s...")
            time.sleep(CLOUDFLARE_WAIT)
            
            if is_cloudflare_challenge(driver.page_source):
                print(f"    ⚠ Cloudflare still blocking...")
                
                if retry_count < 1:
                    print(f"    → Retrying...")
                    time.sleep(10)
                    return fetch_with_selenium(url, retry_count + 1)
                else:
                    return None
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except:
            pass
        
        html = driver.page_source
        
        if is_cloudflare_challenge(html):
            print(f"    ✗ Cloudflare blocking persists")
            return None
        
        return html
        
    except Exception as e:
        print(f"    ✗ Selenium error: {str(e)[:60]}")
        reset_selenium_driver()
        return None

def fetch_with_requests(url: str) -> Optional[str]:
    """Fetch URL using requests"""
    try:
        time.sleep(RATE_LIMIT_REQUESTS)
        session = requests.Session()
        session.headers.update(HEADERS)
        resp = session.get(url, timeout=30, allow_redirects=True)
        return resp.text if resp.status_code == 200 else None
    except Exception:
        return None

def fetch(url: str, force_selenium: bool = False) -> Optional[str]:
    """Fetch URL with appropriate method"""
    if "journals.lww.com" in url or force_selenium:
        return fetch_with_selenium(url)
    
    html = fetch_with_requests(url)
    if html and not is_cloudflare_challenge(html):
        return html
    
    print("    → Selenium fallback...")
    return fetch_with_selenium(url)

# ============================================================================
# LINK EXTRACTION
# ============================================================================

def get_disease_links() -> List[str]:
    """Extract disease links from main AASLD page"""
    print(f"Fetching main page: {MAIN_URL}")
    html = fetch(MAIN_URL)
    if not html:
        print("✗ Failed to fetch main page")
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    disease_section = soup.find(
        lambda tag: tag.name in ["h2", "h3"] and 
        "guidelines and guidance by disease" in normalize_text(tag.get_text())
    )
    
    if not disease_section:
        print("✗ Could not find disease section")
        return []
    
    links = []
    for sibling in disease_section.find_next_siblings():
        if sibling.name in ["h1", "h2", "h3"]:
            break
        for a in sibling.find_all("a", href=True):
            href = a.get("href")
            if href and "/practice-guidelines/" in href:
                full_url = clean_url(urljoin(MAIN_URL, href))
                if full_url not in links and full_url != clean_url(MAIN_URL):
                    links.append(full_url)
    
    print(f"  ✓ Found {len(links)} disease pages\n")
    return links

def match_target_heading(txt: str) -> bool:
    """Check if heading matches target patterns"""
    t = normalize_text(txt)
    return (
        ("practice" in t and "guid" in t) or
        ("supplement" in t and "material" in t)
    )

def extract_section_using_next_elements(heading: Tag) -> BeautifulSoup:
    """Extract content section after a heading"""
    parts = []
    cur_level = heading_level(heading.name)
    started = False
    
    for el in heading.next_elements:
        if not started:
            if el is heading:
                continue
            started = True
        
        if isinstance(el, Tag) and el.name in HEADING_TAGS:
            if heading_level(el.name) <= cur_level:
                break
        
        if isinstance(el, (Tag, NavigableString)):
            if isinstance(el, Tag) and el.name in ["script", "style", "noscript"]:
                continue
            parts.append(str(el))
    
    return BeautifulSoup("".join(parts), "html.parser")

def is_valid_content_link(url: str, base_url: str) -> bool:
    """Check if URL is a valid guideline link"""
    if not url or url == base_url:
        return False
    
    if any(pattern in url.lower() for pattern in REJECT_PATTERNS):
        return False
    
    parsed = urlparse(url)
    
    if any(domain in parsed.netloc for domain in ["journals.lww.com", "doi.org", "pubmed"]):
        return True
    
    if url.endswith(".pdf") or "/sites/default/files/" in url:
        return True
    
    if "aasld.org" in parsed.netloc and "/practice-guidelines/" in url:
        base_path = urlparse(base_url).path.rstrip("/")
        url_path = parsed.path.rstrip("/")
        if url_path != base_path and url_path.startswith(base_path):
            return True
    
    return False

def get_links_under_target_headings(page_url: str) -> List[str]:
    """Extract guideline links from a disease page"""
    html = fetch(page_url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    collected_links = []
    
    for heading in soup.find_all(HEADING_TAGS):
        if match_target_heading(heading.get_text(strip=True)):
            section = extract_section_using_next_elements(heading)
            
            for a in section.find_all("a", href=True):
                href = a.get("href")
                if not href or href.startswith("#"):
                    continue
                
                full_url = clean_url(urljoin(page_url, href))
                
                if is_valid_content_link(full_url, page_url) and full_url not in collected_links:
                    collected_links.append(full_url)
    
    return collected_links

# ============================================================================
# HTML DATA EXTRACTION
# ============================================================================

def extract_all_text_with_structure(html: str) -> Dict:
    """Extract ALL text with structure preserved"""
    soup = BeautifulSoup(html, "html.parser")
    
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    
    sections = []
    current_section = None
    
    for element in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'section']):
        if element.find_parent(['script', 'style']):
            continue
        
        text = element.get_text(strip=True)
        if not text or len(text) < 5:
            continue
        
        if element.name in ['h1', 'h2', 'h3']:
            if current_section:
                sections.append(current_section)
            current_section = {
                'heading': text,
                'level': int(element.name[1]),
                'content': []
            }
        elif current_section:
            current_section['content'].append(text)
    
    if current_section:
        sections.append(current_section)
    
    all_paragraphs = []
    for p in soup.find_all(['p', 'li', 'div']):
        text = p.get_text(strip=True)
        if text and len(text) > 10:
            all_paragraphs.append(text)
    
    tables = extract_all_tables(html)
    
    title = soup.title.string if soup.title else "No title"
    if soup.find("h1"):
        title = soup.find("h1").get_text(strip=True)
    
    links = []
    for a in soup.find_all('a', href=True):
        link_text = a.get_text(strip=True)
        href = a.get('href')
        if link_text and href:
            links.append({"text": link_text, "url": href})
    
    full_text = "\n\n".join(all_paragraphs)
    
    return {
        "title": title,
        "full_text": full_text,
        "full_text_length": len(full_text),
        "paragraph_count": len(all_paragraphs),
        "sections": sections,
        "section_count": len(sections),
        "tables": tables,
        "table_count": len(tables),
        "links_count": len(links),
        "links": links[:20],
        "word_count": len(full_text.split()),
        "char_count": len(full_text)
    }

def extract_all_tables(html: str) -> List[Dict]:
    """Extract ALL tables"""
    soup = BeautifulSoup(html, "html.parser")
    tables = []
    
    for table_idx, table in enumerate(soup.find_all("table")):
        headers = []
        rows = []
        
        thead = table.find('thead')
        if thead:
            for tr in thead.find_all('tr'):
                header_cells = [td.get_text(strip=True) for td in tr.find_all(['th', 'td'])]
                if header_cells:
                    headers = header_cells
                    break
        
        tbody = table.find('tbody')
        table_body = tbody if tbody else table
        
        for tr in table_body.find_all('tr'):
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if cells and cells != headers:
                rows.append(cells)
        
        if rows or headers:
            tables.append({
                "table_index": table_idx + 1,
                "headers": headers,
                "rows": rows,
                "row_count": len(rows),
                "column_count": len(headers) if headers else (len(rows[0]) if rows else 0)
            })
    
    return tables

def save_complete_data(url: str, extracted_data: Dict) -> tuple:
    """Save all extracted data"""
    url_hash = sha256_hash(url)
    
    json_record = {
        "page_url": url,
        "page_title": extracted_data["title"],
        "content_type": "html",
        "crawled_at": datetime.now(timezone.utc).isoformat(),
        "content": {
            "full_text": extracted_data["full_text"],
            "full_text_length": extracted_data["full_text_length"],
            "paragraph_count": extracted_data["paragraph_count"],
            "word_count": extracted_data["word_count"],
            "section_count": extracted_data["section_count"],
            "table_count": extracted_data["table_count"],
            "sections": extracted_data["sections"],
            "tables": extracted_data["tables"],
            "links": extracted_data["links"]
        },
        "accessible": True
    }
    
    json_file = JSON_DIR / f"{url_hash}.json"
    json_file.write_text(json.dumps(json_record, indent=2, ensure_ascii=False), encoding="utf-8")
    
    text_file = TEXT_DIR / f"{url_hash}.txt"
    text_file.write_text(extracted_data["full_text"], encoding="utf-8")
    
    sections_file = TEXT_DIR / f"{url_hash}_sections.json"
    sections_file.write_text(json.dumps(extracted_data["sections"], indent=2, ensure_ascii=False), encoding="utf-8")
    
    if extracted_data["tables"]:
        tables_file = TEXT_DIR / f"{url_hash}_tables.json"
        tables_file.write_text(json.dumps(extracted_data["tables"], indent=2, ensure_ascii=False), encoding="utf-8")
    
    return json_file, text_file

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*70)
    print("AASLD Complete Workflow: HTML + PDF Processing")
    print("="*70 + "\n")
    
    if not PDF_SUPPORT:
        print("⚠ Warning: PyPDF2 not installed - PDFs will be skipped")
        print("  Install with: pip install PyPDF2\n")
    
    try:
        # STEP 1 & 2: Extract all links
        print("== STEP 1: Extracting disease links ==\n")
        disease_pages = get_disease_links()
        
        if not disease_pages:
            print("✗ No disease pages found")
            return
        
        print("== STEP 2: Extracting guideline links from disease pages ==\n")
        all_links = []
        for i, page_url in enumerate(disease_pages, 1):
            disease_name = page_url.split('/')[-1]
            print(f"[{i}/{len(disease_pages)}] {disease_name}")
            links = get_links_under_target_headings(page_url)
            if links:
                all_links.extend(links)
                print(f"  ✓ {len(links)} link(s)\n")
        
        unique_links = list(dict.fromkeys(all_links))
        LINKS_FILE.write_text("\n".join(unique_links), encoding="utf-8")
        print(f"✓ Total unique links: {len(unique_links)}")
        print(f"  Saved to: {LINKS_FILE}\n")
        
        # STEP 3: Process all links
        print("== STEP 3: Processing all guideline links ==\n")
        
        stats = {
            "total": len(unique_links),
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "pdf_success": 0,
            "pdf_failed": 0,
            "insufficient": 0
        }
        
        results = []
        
        for i, url in enumerate(unique_links, 1):
            print(f"[{i}/{len(unique_links)}] {url[:60]}...")
            
            # Handle PDFs
            if url.endswith(".pdf"):
                if not PDF_SUPPORT:
                    print(f"    ✗ PDF skipped (PyPDF2 not installed)\n")
                    stats["pdf_failed"] += 1
                    continue
                
                pdf_data = process_pdf(url)
                
                if pdf_data:
                    json_file, text_file = save_pdf_data(url, pdf_data)
                    print(f"    ✓ PDF Success - {pdf_data['word_count']:,} words\n")
                    stats["pdf_success"] += 1
                    stats["success"] += 1
                    results.append({
                        "url": url,
                        "status": "success",
                        "type": "pdf",
                        "title": pdf_data["title"],
                        "word_count": pdf_data["word_count"]
                    })
                else:
                    print(f"    ✗ PDF processing failed\n")
                    stats["pdf_failed"] += 1
                    stats["failed"] += 1
                    results.append({"url": url, "status": "failed", "type": "pdf"})
                
                continue
            
            # Handle HTML pages
            html = fetch(url)
            
            if not html:
                print(f"    ✗ Failed to fetch\n")
                stats["failed"] += 1
                results.append({"url": url, "status": "failed", "type": "html"})
                continue
            
            if is_cloudflare_challenge(html):
                print(f"    ✗ Blocked by Cloudflare\n")
                stats["blocked"] += 1
                results.append({"url": url, "status": "blocked", "type": "html"})
                continue
            
            try:
                extracted_data = extract_all_text_with_structure(html)
                
                if not extracted_data["full_text"] or len(extracted_data["full_text"]) < 100:
                    print(f"    ✗ Insufficient content\n")
                    stats["insufficient"] += 1
                    results.append({"url": url, "status": "insufficient", "type": "html"})
                    continue
                
                json_file, text_file = save_complete_data(url, extracted_data)
                
                print(f"    ✓ HTML Success - {extracted_data['word_count']:,} words, {extracted_data['section_count']} sections\n")
                stats["success"] += 1
                results.append({
                    "url": url,
                    "status": "success",
                    "type": "html",
                    "title": extracted_data["title"],
                    "word_count": extracted_data["word_count"],
                    "sections": extracted_data["section_count"]
                })
                
            except Exception as e:
                print(f"    ✗ Error: {str(e)[:40]}\n")
                stats["failed"] += 1
                results.append({"url": url, "status": "error", "type": "html"})
        
        # SUMMARY
        results_file = META_DIR / "processing_results.json"
        results_file.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        
        print("\n" + "="*70)
        print("PROCESSING SUMMARY")
        print("="*70)
        print(f"Total links: {stats['total']}")
        print(f"✓ Successful: {stats['success']}")
        print(f"  - HTML: {stats['success'] - stats['pdf_success']}")
        print(f"  - PDF: {stats['pdf_success']}")
        print(f"✗ Failed: {stats['failed']}")
        print(f"  - PDF Failed: {stats['pdf_failed']}")
        print(f"⊘ Blocked: {stats['blocked']}")
        print(f"⊘ Insufficient: {stats['insufficient']}")
        print(f"\nSuccess rate: {(stats['success'] / stats['total'] * 100):.1f}%")
        print("="*70)
        print(f"\nData saved to:")
        print(f"  - Metadata JSON: {JSON_DIR}")
        print(f"  - Full text: {TEXT_DIR}")
        print(f"  - Downloaded PDFs: {PDF_DIR}")
        print(f"  - Results: {results_file}")
        print("="*70 + "\n")
        
    finally:
        close_selenium_driver()

if __name__ == "__main__":
    main()
