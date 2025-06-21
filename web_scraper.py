"""
Web scraping module for discovering venue websites and technical specification PDFs.
"""
import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import logging
from typing import List, Dict, Set, Optional
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from config import *

class VenueWebScraper:
    """Handles venue website discovery and PDF location."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS)
        })
        self.found_pdfs = set()
        self.visited_urls = set()
        
    def search_venues(self, venue_names: List[str]) -> Dict[str, List[str]]:
        """
        Search for official websites of venues.
        
        Args:
            venue_names: List of venue names to search for
            
        Returns:
            Dictionary mapping venue names to their potential website URLs
        """
        venue_websites = {}
        
        for venue_name in venue_names:
            logging.info(f"Searching for websites for venue: {venue_name}")
            websites = self._search_venue_websites(venue_name)
            venue_websites[venue_name] = websites
            time.sleep(REQUEST_DELAY_SECONDS)
            
        return venue_websites
    
    def _search_venue_websites(self, venue_name: str) -> List[str]:
        """
        Search for a specific venue's official website using Serper API.
        Fails if Serper API is not available or returns no results.
        """
        if not SERPER_API_KEY:
            raise Exception("Serper API key is required for venue website search")
        
        websites = []
        serper_results = self._serper_search(venue_name)
        if isinstance(serper_results, str) and serper_results == "error":
            raise Exception(f"Serper API search failed for venue: {venue_name}")
        
        websites.extend(serper_results)
        
        if not websites:
            raise Exception(f"No websites found for venue: {venue_name}")
        
        # Remove duplicates and validate
        unique_websites = list(set(websites))
        validated_websites = self._validate_websites(unique_websites)
        
        if not validated_websites:
            raise Exception(f"No valid websites found for venue: {venue_name}")
        
        return validated_websites
    
    def _serper_search(self, venue_name: str):
        """
        Search using Serper API for the official site and technical specification PDF,
        excluding Wikipedia and focusing on artist/event venues.
        Returns "error" if the request fails.
        """
        try:
            search_url = "https://google.serper.dev/search"
            
            # First search for the venue's official website
            query = f'"{venue_name}" official website -wikipedia -tripadvisor'
            headers = {
                "X-API-KEY": SERPER_API_KEY,
                "Content-Type": "application/json"
            }
            payload = {"q": query}
            
            # Add timeout and retry logic
            response = requests.post(search_url, headers=headers, json=payload, timeout=15)
            if response.status_code != 200:
                logging.error(f"Serper search failed for {venue_name}: {response.status_code} {response.text}")
                return "error"
                
            results = response.json()
            websites = []
            
            # Get organic results
            for item in results.get('organic', []):
                link = item.get('link')
                if link and self._is_likely_venue_website(link, venue_name):
                    websites.append(link)
            
            # If we found websites, also search specifically for PDFs
            if websites:
                time.sleep(1)  # Rate limiting
                pdf_query = f'"{venue_name}" technical specifications OR equipment list filetype:pdf'
                pdf_payload = {"q": pdf_query}
                
                try:
                    pdf_response = requests.post(search_url, headers=headers, json=pdf_payload, timeout=15)
                    if pdf_response.status_code == 200:
                        pdf_results = pdf_response.json()
                        for item in pdf_results.get('organic', []):
                            link = item.get('link')
                            if link and link.endswith('.pdf'):
                                websites.append(link)
                except Exception as e:
                    logging.debug(f"PDF search failed for {venue_name}: {e}")
            
            return websites
            
        except requests.exceptions.Timeout:
            logging.error(f"Serper search timed out for {venue_name}")
            return "error"
        except Exception as e:
            logging.error(f"Serper search failed for {venue_name}: {e}")
            return "error"
    
    def _is_likely_venue_website(self, url: str, venue_name: str) -> bool:
        """Check if a URL is likely to be the venue's official website."""
        domain = urlparse(url).netloc.lower()
        venue_words = venue_name.lower().split()
        
        # Check if venue name appears in domain
        for word in venue_words:
            if len(word) > 3 and word in domain:
                return True
        
        # Check for venue-related keywords in domain
        venue_keywords = ['center', 'centre', 'theatre', 'theater', 'hall', 'arts', 'opera', 'auditorium']
        if any(keyword in domain for keyword in venue_keywords):
            return True
            
        return False
    
    # Domain guessing removed as per user feedback.
    
    def _validate_websites(self, websites: List[str]) -> List[str]:
        """Validate that websites are accessible and likely to be official venue sites."""
        validated = []
        venue_keywords = [
            "center", "centre", "theatre", "theater", "hall", "arts", "opera", "auditorium", "stadium", "arena", "philharmonic", "orchestra"
        ]
        exclude_domains = [
            "dictionary.cambridge.org", "thesaurus", "wikipedia.org", "wikidata.org", "wikimedia.org", "youtube.com", "facebook.com", "twitter.com", "linkedin.com", "tripadvisor.com"
        ]
        for website in websites:
            try:
                domain = urlparse(website).netloc.lower()
                if any(ex in domain for ex in exclude_domains):
                    continue
                response = self.session.get(website, timeout=10)
                if response.status_code == 200:
                    # Decode with errors="replace" to avoid decoding warnings
                    try:
                        content = response.content.decode(response.encoding or "utf-8", errors="replace").lower()
                    except Exception:
                        content = response.text.lower()
                    # Must contain venue keywords in domain or content
                    if (
                        any(kw in domain for kw in venue_keywords)
                        or any(kw in content for kw in venue_keywords)
                    ):
                        validated.append(website)
            except Exception as e:
                logging.debug(f"Website validation failed for {website}: {e}")
                continue
        return validated
    
    def find_pdfs_on_websites(self, venue_websites: Dict[str, List[str]]) -> Dict[str, List[Dict]]:
        """
        Find technical specification PDFs on venue websites.
        
        Args:
            venue_websites: Dictionary mapping venue names to website URLs
            
        Returns:
            Dictionary mapping venue names to lists of PDF information
        """
        venue_pdfs = {}
        
        for venue_name, websites in venue_websites.items():
            logging.info(f"Searching for PDFs on websites for venue: {venue_name}")
            pdfs = []
            
            for website in websites:
                try:
                    site_pdfs = self._find_pdfs_on_site(website, venue_name)
                    pdfs.extend(site_pdfs)
                    time.sleep(REQUEST_DELAY_SECONDS)
                    
                except Exception as e:
                    logging.error(f"Error searching PDFs on {website}: {e}")
                    continue
            
            venue_pdfs[venue_name] = pdfs
            
        return venue_pdfs
    
    def _find_pdfs_on_site(self, base_url: str, venue_name: str) -> List[Dict]:
        """Find PDFs on a specific website."""
        if base_url in self.visited_urls:
            return []
            
        self.visited_urls.add(base_url)
        pdfs = []
        
        try:
            # Get main page
            response = self.session.get(base_url, timeout=10)
            response.raise_for_status()

            # Decode with errors="replace" to avoid decoding warnings
            try:
                html = response.content.decode(response.encoding or "utf-8", errors="replace")
            except Exception:
                html = response.text
            soup = BeautifulSoup(html, 'html.parser')

            # Find direct PDF links
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
            
            for link in pdf_links:
                href = link.get('href')
                if href:
                    full_url = urljoin(base_url, href)
                    if self._is_relevant_pdf(link, full_url):
                        pdfs.append({
                            'url': full_url,
                            'title': link.get_text(strip=True) or 'Unknown',
                            'venue': venue_name,
                            'source_page': base_url
                        })
            
            # Look for pages that might contain PDFs
            relevant_pages = self._find_relevant_pages(soup, base_url)
            
            for page_url in relevant_pages[:5]:  # Limit depth
                if page_url not in self.visited_urls:
                    try:
                        page_pdfs = self._find_pdfs_on_site(page_url, venue_name)
                        pdfs.extend(page_pdfs)
                    except Exception as e:
                        logging.debug(f"Error searching page {page_url}: {e}")
                        continue
                        
        except Exception as e:
            logging.error(f"Error processing site {base_url}: {e}")
            
        return pdfs
    
    def _is_relevant_pdf(self, link_element, pdf_url: str) -> bool:
        """Check if a PDF link is likely to contain technical specifications."""
        link_text = link_element.get_text(strip=True).lower()
        href = link_element.get('href', '').lower()
        
        # Check for relevant keywords
        relevant_keywords = PDF_KEYWORDS + ['spec', 'technical', 'equipment', 'av', 'audio', 'visual']
        
        for keyword in relevant_keywords:
            if keyword in link_text or keyword in href:
                return True
                
        return False
    
    def _find_relevant_pages(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find pages that might contain technical specifications."""
        relevant_pages = []
        
        # Look for navigation links with relevant keywords
        nav_keywords = ['technical', 'specs', 'specifications', 'equipment', 'av', 'audio', 'visual', 'production']
        
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            href = link.get('href').lower()
            
            if any(keyword in link_text or keyword in href for keyword in nav_keywords):
                full_url = urljoin(base_url, link.get('href'))
                if self._is_same_domain(base_url, full_url):
                    relevant_pages.append(full_url)
                    
        return relevant_pages
    
    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        try:
            domain1 = urlparse(url1).netloc
            domain2 = urlparse(url2).netloc
            return domain1 == domain2
        except:
            return False
    
    async def download_pdfs(self, venue_pdfs: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Download PDFs asynchronously.
        
        Args:
            venue_pdfs: Dictionary mapping venue names to PDF information
            
        Returns:
            Updated dictionary with local file paths
        """
        os.makedirs(PDF_CACHE_DIR, exist_ok=True)
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            for venue_name, pdfs in venue_pdfs.items():
                for pdf_info in pdfs:
                    task = self._download_pdf(session, pdf_info, venue_name)
                    tasks.append(task)
            
            # Limit concurrent downloads
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            
            async def bounded_download(task):
                async with semaphore:
                    return await task
            
            results = await asyncio.gather(*[bounded_download(task) for task in tasks], return_exceptions=True)
            
            # Update PDF info with download results
            result_index = 0
            for venue_name, pdfs in venue_pdfs.items():
                for pdf_info in pdfs:
                    result = results[result_index]
                    if isinstance(result, str):  # Success - file path returned
                        pdf_info['local_path'] = result
                        pdf_info['downloaded'] = True
                    else:  # Exception or failure
                        pdf_info['downloaded'] = False
                        pdf_info['error'] = str(result) if result else 'Download failed'
                    result_index += 1
        
        return venue_pdfs
    
    async def _download_pdf(self, session: aiohttp.ClientSession, pdf_info: Dict, venue_name: str) -> Optional[str]:
        """Download a single PDF file."""
        try:
            url = pdf_info['url']
            
            # Generate filename
            filename = self._generate_pdf_filename(pdf_info, venue_name)
            filepath = os.path.join(PDF_CACHE_DIR, filename)
            
            # Skip if already downloaded
            if os.path.exists(filepath):
                return filepath
            
            async with session.get(url, timeout=PDF_TIMEOUT_SECONDS) as response:
                if response.status == 200:
                    content_length = response.headers.get('content-length')
                    if content_length and int(content_length) > MAX_PDF_SIZE_MB * 1024 * 1024:
                        raise Exception(f"PDF too large: {content_length} bytes")
                    
                    content = await response.read()
                    
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    
                    logging.info(f"Downloaded PDF: {filename}")
                    return filepath
                else:
                    raise Exception(f"HTTP {response.status}")
                    
        except Exception as e:
            logging.error(f"Failed to download PDF {pdf_info['url']}: {e}")
            return None
    
    def _generate_pdf_filename(self, pdf_info: Dict, venue_name: str) -> str:
        """Generate a safe filename for the PDF."""
        # Clean venue name and title
        clean_venue = re.sub(r'[^a-zA-Z0-9\s]', '', venue_name)
        clean_venue = re.sub(r'\s+', '_', clean_venue.strip())
        
        clean_title = re.sub(r'[^a-zA-Z0-9\s]', '', pdf_info.get('title', 'spec'))
        clean_title = re.sub(r'\s+', '_', clean_title.strip())
        
        # Create filename
        filename = f"{clean_venue}_{clean_title}.pdf"
        
        # Ensure unique filename
        counter = 1
        base_filename = filename
        while os.path.exists(os.path.join(PDF_CACHE_DIR, filename)):
            name, ext = os.path.splitext(base_filename)
            filename = f"{name}_{counter}{ext}"
            counter += 1
            
        return filename
    
    def close(self):
        """Clean up resources."""
        self.session.close()
