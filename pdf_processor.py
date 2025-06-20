"""
PDF processing module for extracting text and structure from technical specification PDFs.
"""
import PyPDF2
import pdfplumber
import logging
import re
from typing import List, Dict, Optional, Tuple
import os
from dataclasses import dataclass
try:
    import fitz  # PyMuPDF for better text extraction
except ImportError:
    fitz = None

@dataclass
class PDFContent:
    """Structure to hold extracted PDF content."""
    text: str
    pages: List[str]
    tables: List[List[List[str]]]
    metadata: Dict
    file_path: str
    venue: str

class PDFProcessor:
    """Handles PDF text extraction and preprocessing."""
    
    def __init__(self):
        self.supported_formats = ['.pdf']
        
    def process_pdfs(self, venue_pdfs: Dict[str, List[Dict]]) -> Dict[str, List[PDFContent]]:
        """
        Process all downloaded PDFs and extract content.
        
        Args:
            venue_pdfs: Dictionary mapping venue names to PDF information
            
        Returns:
            Dictionary mapping venue names to extracted PDF content
        """
        processed_content = {}
        
        for venue_name, pdfs in venue_pdfs.items():
            logging.info(f"Processing PDFs for venue: {venue_name}")
            venue_content = []
            
            for pdf_info in pdfs:
                if pdf_info.get('downloaded', False) and pdf_info.get('local_path'):
                    try:
                        content = self._process_single_pdf(pdf_info['local_path'], venue_name)
                        if content:
                            venue_content.append(content)
                    except Exception as e:
                        logging.error(f"Error processing PDF {pdf_info['local_path']}: {e}")
                        continue
            
            processed_content[venue_name] = venue_content
            
        return processed_content
    
    def _process_single_pdf(self, file_path: str, venue_name: str) -> Optional[PDFContent]:
        """Process a single PDF file and extract all relevant content."""
        if not os.path.exists(file_path):
            logging.error(f"PDF file not found: {file_path}")
            return None
        
        try:
            # Try multiple extraction methods for best results
            content = self._extract_with_pdfplumber(file_path)
            if not content or len(content.text.strip()) < 100:
                content = self._extract_with_pymupdf(file_path)
            if not content or len(content.text.strip()) < 100:
                content = self._extract_with_pypdf2(file_path)
            
            if content:
                content.venue = venue_name
                content.file_path = file_path
                
                # Post-process the extracted content
                content = self._post_process_content(content)
                
                logging.info(f"Successfully processed PDF: {os.path.basename(file_path)}")
                return content
            else:
                logging.warning(f"No content extracted from PDF: {file_path}")
                return None
                
        except Exception as e:
            logging.error(f"Error processing PDF {file_path}: {e}")
            return None
    
    def _extract_with_pdfplumber(self, file_path: str) -> Optional[PDFContent]:
        """Extract content using pdfplumber (best for tables and structured content)."""
        try:
            with pdfplumber.open(file_path) as pdf:
                pages = []
                tables = []
                full_text = ""
                
                for page_num, page in enumerate(pdf.pages):
                    # Extract text
                    page_text = page.extract_text() or ""
                    pages.append(page_text)
                    full_text += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
                    
                    # Extract tables
                    page_tables = page.extract_tables()
                    if page_tables:
                        for table in page_tables:
                            if table and len(table) > 1:  # Valid table with header and data
                                tables.append(table)
                
                metadata = pdf.metadata or {}
                
                return PDFContent(
                    text=full_text,
                    pages=pages,
                    tables=tables,
                    metadata=metadata,
                    file_path=file_path,
                    venue=""
                )
                
        except Exception as e:
            logging.debug(f"pdfplumber extraction failed for {file_path}: {e}")
            return None
    
    def _extract_with_pymupdf(self, file_path: str) -> Optional[PDFContent]:
        """Extract content using PyMuPDF (good for complex layouts)."""
        if not fitz:
            logging.debug("PyMuPDF not available, skipping")
            return None
            
        try:
            doc = fitz.open(file_path)
            pages = []
            full_text = ""
            tables = []
            
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                
                # Extract text
                page_text = page.get_text()
                pages.append(page_text)
                full_text += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
                
                # Try to extract tables (basic approach)
                page_tables = self._extract_tables_from_text(page_text)
                tables.extend(page_tables)
            
            doc.close()
            
            return PDFContent(
                text=full_text,
                pages=pages,
                tables=tables,
                metadata={},
                file_path=file_path,
                venue=""
            )
            
        except Exception as e:
            logging.debug(f"PyMuPDF extraction failed for {file_path}: {e}")
            return None
    
    def _extract_with_pypdf2(self, file_path: str) -> Optional[PDFContent]:
        """Extract content using PyPDF2 (fallback method)."""
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                pages = []
                full_text = ""
                
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    page_text = page.extract_text()
                    pages.append(page_text)
                    full_text += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
                
                metadata = pdf_reader.metadata or {}
                
                # Try to extract tables from text
                tables = self._extract_tables_from_text(full_text)
                
                return PDFContent(
                    text=full_text,
                    pages=pages,
                    tables=tables,
                    metadata=dict(metadata),
                    file_path=file_path,
                    venue=""
                )
                
        except Exception as e:
            logging.debug(f"PyPDF2 extraction failed for {file_path}: {e}")
            return None
    
    def _extract_tables_from_text(self, text: str) -> List[List[List[str]]]:
        """Extract table-like structures from plain text."""
        tables = []
        lines = text.split('\n')
        
        current_table = []
        in_table = False
        
        for line in lines:
            line = line.strip()
            if not line:
                if in_table and current_table:
                    tables.append(current_table)
                    current_table = []
                    in_table = False
                continue
            
            # Detect table-like patterns (multiple columns separated by spaces/tabs)
            if self._looks_like_table_row(line):
                row = self._parse_table_row(line)
                if row and len(row) > 1:
                    current_table.append(row)
                    in_table = True
            else:
                if in_table and current_table:
                    tables.append(current_table)
                    current_table = []
                    in_table = False
        
        # Add final table if exists
        if current_table:
            tables.append(current_table)
        
        # Filter out tables that are too small or don't look like equipment lists
        filtered_tables = []
        for table in tables:
            if len(table) >= 2 and self._looks_like_equipment_table(table):
                filtered_tables.append(table)
        
        return filtered_tables
    
    def _looks_like_table_row(self, line: str) -> bool:
        """Check if a line looks like a table row."""
        # Look for multiple columns separated by whitespace
        parts = re.split(r'\s{2,}|\t', line)
        return len(parts) >= 2
    
    def _parse_table_row(self, line: str) -> List[str]:
        """Parse a line into table columns."""
        # Split by multiple spaces or tabs
        parts = re.split(r'\s{2,}|\t', line)
        return [part.strip() for part in parts if part.strip()]
    
    def _looks_like_equipment_table(self, table: List[List[str]]) -> bool:
        """Check if a table looks like it contains equipment information."""
        if len(table) < 2:
            return False
        
        # Check header row for equipment-related keywords
        header = ' '.join(table[0]).lower()
        equipment_keywords = [
            'equipment', 'model', 'manufacturer', 'quantity', 'type', 'brand',
            'description', 'specs', 'specifications', 'audio', 'video', 'lighting'
        ]
        
        return any(keyword in header for keyword in equipment_keywords)
    
    def _post_process_content(self, content: PDFContent) -> PDFContent:
        """Post-process extracted content to improve quality."""
        # Clean up text
        content.text = self._clean_text(content.text)
        
        # Clean up pages
        content.pages = [self._clean_text(page) for page in content.pages]
        
        # Clean up tables
        cleaned_tables = []
        for table in content.tables:
            cleaned_table = []
            for row in table:
                cleaned_row = [self._clean_cell_text(cell) for cell in row]
                if any(cell.strip() for cell in cleaned_row):  # Skip empty rows
                    cleaned_table.append(cleaned_row)
            if cleaned_table:
                cleaned_tables.append(cleaned_table)
        content.tables = cleaned_tables
        
        return content
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize extracted text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove page headers/footers (common patterns)
        text = re.sub(r'Page \d+ of \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'^\d+\s*$', '', text, flags=re.MULTILINE)
        
        # Fix common OCR errors
        text = text.replace('|', 'I')  # Common OCR mistake
        text = text.replace('0', 'O')  # In some contexts
        
        # Normalize line breaks
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        
        return text.strip()
    
    def _clean_cell_text(self, cell: str) -> str:
        """Clean text in table cells."""
        if not cell:
            return ""
        
        # Remove extra whitespace
        cell = re.sub(r'\s+', ' ', cell.strip())
        
        # Remove common artifacts
        cell = re.sub(r'^[-•·]+\s*', '', cell)  # Remove bullet points
        cell = re.sub(r'\s*[-•·]+$', '', cell)  # Remove trailing bullets
        
        return cell
    
    def get_content_summary(self, content: PDFContent) -> Dict:
        """Generate a summary of the extracted content."""
        return {
            'file_path': content.file_path,
            'venue': content.venue,
            'total_pages': len(content.pages),
            'total_text_length': len(content.text),
            'tables_found': len(content.tables),
            'has_equipment_keywords': self._has_equipment_keywords(content.text),
            'metadata': content.metadata
        }
    
    def _has_equipment_keywords(self, text: str) -> bool:
        """Check if text contains equipment-related keywords."""
        text_lower = text.lower()
        equipment_keywords = [
            'audio', 'video', 'lighting', 'sound', 'microphone', 'speaker',
            'projector', 'screen', 'mixer', 'amplifier', 'led', 'fixture'
        ]
        
        return any(keyword in text_lower for keyword in equipment_keywords)