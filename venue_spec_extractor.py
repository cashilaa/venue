import asyncio
import logging
import os
import sys
from typing import List, Dict, Optional
from datetime import datetime
import argparse

from web_scraper import VenueWebScraper
from pdf_processor import PDFProcessor
from data_extractor import DataExtractor
from data_standardizer import DataStandardizer
from data_exporter import DataExporter
from config import *

class VenueSpecExtractor:
    
    def __init__(self):
        # Create necessary directories BEFORE logging setup
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(PDF_CACHE_DIR, exist_ok=True)
        os.makedirs(LOGS_DIR, exist_ok=True)
        self.setup_logging()
        self.web_scraper = VenueWebScraper()
        self.pdf_processor = PDFProcessor()
        self.data_extractor = DataExtractor()
        self.data_standardizer = DataStandardizer()
        self.data_exporter = DataExporter()
    
    def setup_logging(self):
        log_filename = f"venue_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = os.path.join(LOGS_DIR, log_filename)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logging.info("Venue Specification Extractor initialized")
    
    async def extract_venue_specifications(self, venue_names: List[str], 
                                         skip_web_search: bool = False,
                                         pdf_paths: Optional[List[str]] = None) -> Dict[str, str]:
        
        logging.info(f"Starting extraction process for {len(venue_names)} venues")
        
        try:
            # Step 1: Discover venue websites (unless skipped)
            if skip_web_search and pdf_paths:
                logging.info("Skipping web search, using provided PDF paths")
                venue_pdfs = self._create_pdf_info_from_paths(venue_names, pdf_paths)
            else:
                logging.info("Step 1: Discovering venue websites...")
                venue_websites = self.web_scraper.search_venues(venue_names)
                
                # Step 2: Find PDFs on websites
                logging.info("Step 2: Locating technical specification PDFs...")
                venue_pdfs = self.web_scraper.find_pdfs_on_websites(venue_websites)
                
                # Step 3: Download PDFs
                logging.info("Step 3: Downloading PDFs...")
                venue_pdfs = await self.web_scraper.download_pdfs(venue_pdfs)
            
            # Step 4: Process PDFs
            logging.info("Step 4: Processing PDFs and extracting content...")
            processed_content = self.pdf_processor.process_pdfs(venue_pdfs)
            
            # Step 5: Extract equipment data using AI
            logging.info("Step 5: Extracting equipment data using AI...")
            extracted_data = self.data_extractor.extract_equipment_data(processed_content)
            
            # Step 6: Standardize and normalize data
            logging.info("Step 6: Standardizing and normalizing data...")
            standardized_data = self.data_standardizer.standardize_equipment_data(extracted_data)
            
            # Step 7: Validate data
            logging.info("Step 7: Validating standardized data...")
            validation_report = self.data_standardizer.validate_standardized_data(standardized_data)
            
            # Step 8: Export data in all formats
            logging.info("Step 8: Exporting data to structured formats...")
            exported_files = self.data_exporter.export_all_formats(standardized_data, validation_report)
            
            # Log summary
            total_equipment = sum(len(items) for items in standardized_data.values())
            total_issues = sum(len(issues) for issues in validation_report.values())
            
            logging.info(f"Extraction completed successfully!")
            logging.info(f"Total venues processed: {len(standardized_data)}")
            logging.info(f"Total equipment items extracted: {total_equipment}")
            logging.info(f"Total validation issues: {total_issues}")
            logging.info(f"Exported files: {list(exported_files.keys())}")
            
            return exported_files
            
        except Exception as e:
            logging.error(f"Error during extraction process: {e}")
            raise
        
        finally:
            # Cleanup
            self.web_scraper.close()
    
    def _create_pdf_info_from_paths(self, venue_names: List[str], pdf_paths: List[str]) -> Dict[str, List[Dict]]:
        venue_pdfs = {}
        
        # If we have equal numbers of venues and PDFs, map them 1:1
        if len(venue_names) == len(pdf_paths):
            for venue_name, pdf_path in zip(venue_names, pdf_paths):
                if os.path.exists(pdf_path):
                    venue_pdfs[venue_name] = [{
                        'url': f'file://{pdf_path}',
                        'title': os.path.basename(pdf_path),
                        'venue': venue_name,
                        'source_page': 'local_file',
                        'local_path': pdf_path,
                        'downloaded': True
                    }]
                else:
                    logging.warning(f"PDF file not found: {pdf_path}")
                    venue_pdfs[venue_name] = []
        else:
            # Assign all PDFs to the first venue or distribute evenly
            for venue_name in venue_names:
                venue_pdfs[venue_name] = []
            
            for pdf_path in pdf_paths:
                if os.path.exists(pdf_path):
                    # Assign to first venue for simplicity
                    first_venue = venue_names[0]
                    venue_pdfs[first_venue].append({
                        'url': f'file://{pdf_path}',
                        'title': os.path.basename(pdf_path),
                        'venue': first_venue,
                        'source_page': 'local_file',
                        'local_path': pdf_path,
                        'downloaded': True
                    })
                else:
                    logging.warning(f"PDF file not found: {pdf_path}")
        
        return venue_pdfs
    
    def search_equipment(self, database_path: str, manufacturer: str = None, 
                        model: str = None, category: str = None) -> List[Dict]:
       
        return self.data_exporter.search_equipment_database(
            database_path, manufacturer, model, category
        )
    
    def get_processing_summary(self) -> Dict:
        """Get a summary of the last processing run."""
        # This would typically read from a status file or database
        # For now, return basic info
        return {
            "last_run": "Not available",
            "status": "Ready",
            "cache_size": len(os.listdir(PDF_CACHE_DIR)) if os.path.exists(PDF_CACHE_DIR) else 0
        }

def auto_discover_venues():
    """
    Automatically discover a list of real venue names by scraping a Wikipedia list page.
    Only returns actual venues, not categories, lists, or meta pages.
    """
    import requests
    from bs4 import BeautifulSoup

    # Example: Wikipedia list of concert halls in the United States
    wiki_url = "https://en.wikipedia.org/wiki/List_of_concert_halls"

    try:
        response = requests.get(wiki_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        venue_names = set()

        # Exclude links/names with these substrings (case-insensitive)
        EXCLUDE_SUBSTRINGS = [
            "list", "category", "portal", "article", "commons", "main page", "current events",
            "random article", "disambiguation", "template", "help:", "special:", "wikipedia:", "file:"
        ]

        # Prefer extracting from the main venue table if present
        tables = soup.find_all("table", class_="wikitable")
        if tables:
            for table in tables:
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if cells:
                        a = cells[0].find("a")
                        if a and a.get("href", "").startswith("/wiki/"):
                            href = a.get("href", "").lower()
                            name = a.get_text(strip=True)
                            lname = name.lower()
                            if (
                                len(name) > 4
                                and not any(ex in href for ex in EXCLUDE_SUBSTRINGS)
                                and not any(ex in lname for ex in EXCLUDE_SUBSTRINGS)
                                and not href.startswith("/wiki/list_of")
                                and not href.startswith("/wiki/category:")
                                and not href.startswith("/wiki/portal:")
                                and not href.startswith("/wiki/template:")
                                and not href.startswith("/wiki/special:")
                                and not href.startswith("/wiki/wikipedia:")
                                and not href.startswith("/wiki/file:")
                            ):
                                venue_names.add(name)
        else:
            # Fallback: old <ul> logic if no table found
            for ul in soup.find_all("ul"):
                for li in ul.find_all("li"):
                    a = li.find("a")
                    if a and a.get("href", "").startswith("/wiki/"):
                        href = a.get("href", "").lower()
                        name = a.get_text(strip=True)
                        lname = name.lower()
                        if (
                            len(name) > 4
                            and not any(ex in href for ex in EXCLUDE_SUBSTRINGS)
                            and not any(ex in lname for ex in EXCLUDE_SUBSTRINGS)
                            and not href.startswith("/wiki/list_of")
                            and not href.startswith("/wiki/category:")
                            and not href.startswith("/wiki/portal:")
                            and not href.startswith("/wiki/template:")
                            and not href.startswith("/wiki/special:")
                            and not href.startswith("/wiki/wikipedia:")
                            and not href.startswith("/wiki/file:")
                        ):
                            venue_names.add(name)

        # Post-process: Exclude names with meta/list/category/portal/template/special/file substrings
        EXCLUDE_SUBSTRINGS = [
            "list", "category", "portal", "article", "commons", "main page", "current events",
            "random article", "disambiguation", "template", "help:", "special:", "wikipedia:", "file:"
        ]
        filtered_venue_names = [
            name for name in venue_names
            if not any(ex in name.lower() for ex in EXCLUDE_SUBSTRINGS)
        ]
        filtered_venue_names = sorted(filtered_venue_names)
        if not filtered_venue_names:
            logging.warning("No venues found from Wikipedia list after filtering.")
        return list(filtered_venue_names)

    except Exception as e:
        logging.error(f"Error during venue auto-discovery from Wikipedia: {e}")
        return []

def filter_artist_venues(venue_names):
    """
    Filter venue names to only include those likely to be artist/event venues.
    """
    keywords = [
        "theatre", "theater", "hall", "center", "centre", "auditorium", "music", "arts", "arena", "stadium",
        "opera", "concert", "performing", "club", "jazz", "philharmonic", "orchestra", "cultural", "amphitheatre",
        "amphitheater", "recital", "auditorium", "event space", "event hall", "event center", "event centre"
    ]
    filtered = []
    for name in venue_names:
        lname = name.lower()
        if any(kw in lname for kw in keywords):
            filtered.append(name)
    return filtered

MAX_VENUES_PER_RUN = 20  # Limit the number of venues processed per run

def main():
    parser = argparse.ArgumentParser(description='AI-powered venue specification extractor')
    
    parser.add_argument('venues', nargs='*', help='Venue names to process')
    parser.add_argument('--skip-web-search', action='store_true', 
                       help='Skip web search and use provided PDF paths')
    parser.add_argument('--pdf-paths', nargs='*', 
                       help='PDF file paths to process directly')
    parser.add_argument('--search-equipment', action='store_true',
                       help='Search equipment database instead of extracting')
    parser.add_argument('--database-path', type=str,
                       help='Path to equipment database for searching')
    parser.add_argument('--manufacturer', type=str,
                       help='Manufacturer to search for')
    parser.add_argument('--model', type=str,
                       help='Model to search for')
    parser.add_argument('--category', type=str,
                       help='Category to filter by (lighting, sound, video)')

    args = parser.parse_args()

    # If no venues provided, auto-discover
    if not args.venues or len(args.venues) == 0:
        print("No venues provided. Automatically discovering venues...")
        args.venues = auto_discover_venues()
        print(f"Discovered venues: {args.venues}")

    # Filter for artist/event venues only
    filtered_venues = filter_artist_venues(args.venues)
    if not filtered_venues:
        print("No artist/event venues found after filtering.")
        return
    # Limit the number of venues processed per run
    limited_venues = filtered_venues[:MAX_VENUES_PER_RUN]
    if len(filtered_venues) > MAX_VENUES_PER_RUN:
        print(f"Limiting to first {MAX_VENUES_PER_RUN} venues for speed.")
    args.venues = limited_venues
    print(f"Filtered venues: {args.venues}")

    extractor = VenueSpecExtractor()
    
    if args.search_equipment:
        if not args.database_path:
            print("Error: --database-path required for equipment search")
            return
        
        results = extractor.search_equipment(
            args.database_path, 
            args.manufacturer, 
            args.model, 
            args.category
        )
        
        print(f"Found {len(results)} matching equipment items:")
        for result in results:
            print(f"- {result['manufacturer']} {result['model']} ({result['category']})")
            print(f"  Found in venues: {', '.join(result['venues_found_in'])}")
            print(f"  Total quantity: {result['total_quantity_across_venues']}")
            print()
    
    else:
        # Run extraction
        try:
            exported_files = asyncio.run(
                extractor.extract_venue_specifications(
                    args.venues,
                    args.skip_web_search,
                    args.pdf_paths
                )
            )
            
            print("\nExtraction completed successfully!")
            print("Exported files:")
            for format_name, file_path in exported_files.items():
                print(f"  {format_name.upper()}: {file_path}")
                
        except Exception as e:
            print(f"Error during extraction: {e}")
            logging.error(f"Extraction failed: {e}")

if __name__ == "__main__":
    main()
