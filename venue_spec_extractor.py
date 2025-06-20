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
        self.setup_logging()
        self.web_scraper = VenueWebScraper()
        self.pdf_processor = PDFProcessor()
        self.data_extractor = DataExtractor()
        self.data_standardizer = DataStandardizer()
        self.data_exporter = DataExporter()
        
        # Create necessary directories
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(PDF_CACHE_DIR, exist_ok=True)
        os.makedirs(LOGS_DIR, exist_ok=True)
    
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

def main():
    parser = argparse.ArgumentParser(description='AI-powered venue specification extractor')
    
    parser.add_argument('venues', nargs='+', help='Venue names to process')
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

    # Show help if no arguments are provided
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    args = parser.parse_args()
    
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
