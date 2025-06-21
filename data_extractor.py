"""
AI-powered data extraction module for extracting equipment information from PDF content.
"""
import openai
import json
import re
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
import time
from pdf_processor import PDFContent
from config import *

@dataclass
class EquipmentItem:
    """Structure for individual equipment items."""
    manufacturer: str
    model: str
    quantity: int
    equipment_type: str
    category: str  # lighting, sound, video
    venue: str
    specifications: Dict[str, Any]
    source_document: str
    confidence_score: float

class DataExtractor:
    """AI-powered equipment data extraction from PDF content."""
    
    def __init__(self):
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI API key is required for data extraction")
        
        openai.api_key = OPENAI_API_KEY
        self.client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
    def extract_equipment_data(self, processed_content: Dict[str, List[PDFContent]]) -> Dict[str, List[EquipmentItem]]:
        """
        Extract equipment data from processed PDF content.
        
        Args:
            processed_content: Dictionary mapping venue names to PDF content
            
        Returns:
            Dictionary mapping venue names to extracted equipment items
        """
        extracted_data = {}
        
        for venue_name, pdf_contents in processed_content.items():
            logging.info(f"Extracting equipment data for venue: {venue_name}")
            venue_equipment = []
            
            for pdf_content in pdf_contents:
                try:
                    equipment_items = self._extract_from_pdf_content(pdf_content)
                    venue_equipment.extend(equipment_items)
                    
                    # Rate limiting for API calls
                    time.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Error extracting data from {pdf_content.file_path}: {e}")
                    continue
            
            extracted_data[venue_name] = venue_equipment
            
        return extracted_data
    
    def _extract_from_pdf_content(self, pdf_content: PDFContent) -> List[EquipmentItem]:
        """Extract equipment data from a single PDF content."""
        equipment_items = []
        
        # Extract from tables first (most structured data)
        if pdf_content.tables:
            table_equipment = self._extract_from_tables(pdf_content)
            equipment_items.extend(table_equipment)
        
        # Extract from text content
        text_equipment = self._extract_from_text(pdf_content)
        equipment_items.extend(text_equipment)
        
        # Remove duplicates and merge similar items
        equipment_items = self._deduplicate_equipment(equipment_items)
        
        return equipment_items
    
    def _extract_from_tables(self, pdf_content: PDFContent) -> List[EquipmentItem]:
        """Extract equipment data from tables using AI."""
        equipment_items = []
        
        for table_idx, table in enumerate(pdf_content.tables):
            try:
                # Convert table to a more readable format
                table_text = self._table_to_text(table)
                
                # Use AI to extract structured data
                extracted_items = self._ai_extract_from_table(table_text, pdf_content, table_idx)
                equipment_items.extend(extracted_items)
                
            except Exception as e:
                logging.error(f"Error extracting from table {table_idx}: {e}")
                continue
        
        return equipment_items
    
    def _extract_from_text(self, pdf_content: PDFContent) -> List[EquipmentItem]:
        """Extract equipment data from unstructured text using AI."""
        # Split text into manageable chunks
        text_chunks = self._split_text_into_chunks(pdf_content.text, max_tokens=3000)
        
        equipment_items = []
        
        for chunk_idx, chunk in enumerate(text_chunks):
            try:
                # Only process chunks that likely contain equipment information
                if self._chunk_contains_equipment_info(chunk):
                    extracted_items = self._ai_extract_from_text(chunk, pdf_content, chunk_idx)
                    equipment_items.extend(extracted_items)
                    
            except Exception as e:
                logging.error(f"Error extracting from text chunk {chunk_idx}: {e}")
                continue
        
        return equipment_items
    
    def _ai_extract_from_table(self, table_text: str, pdf_content: PDFContent, table_idx: int) -> List[EquipmentItem]:
        """Use AI to extract equipment data from table text."""
        prompt = f"""
        Extract audio-visual equipment information from the following table data. 
        Return a JSON array of equipment items with the following structure:
        
        {{
            "manufacturer": "string",
            "model": "string", 
            "quantity": number,
            "equipment_type": "string",
            "category": "lighting|sound|video",
            "specifications": {{"key": "value"}},
            "confidence_score": 0.0-1.0
        }}
        
        Guidelines:
        - Only extract actual equipment items (not headers, totals, or non-equipment text)
        - Normalize manufacturer names (e.g., "Shure Inc." -> "Shure")
        - Categorize equipment as lighting, sound, or video
        - Include quantity as a number (default to 1 if not specified)
        - Extract any technical specifications mentioned
        - Assign confidence score based on data completeness and clarity
        
        Table data:
        {table_text}
        
        Return only valid JSON array:
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert at extracting structured equipment data from technical documents. Always return valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            response_text = response.choices[0].message.content.strip()
            
            # Clean up response to ensure valid JSON
            response_text = self._clean_json_response(response_text)
            
            # Parse JSON response
            equipment_data = json.loads(response_text)
            
            # Convert to EquipmentItem objects
            equipment_items = []
            for item_data in equipment_data:
                equipment_item = EquipmentItem(
                    manufacturer=item_data.get('manufacturer', ''),
                    model=item_data.get('model', ''),
                    quantity=item_data.get('quantity', 1),
                    equipment_type=item_data.get('equipment_type', ''),
                    category=item_data.get('category', ''),
                    venue=pdf_content.venue,
                    specifications=item_data.get('specifications', {}),
                    source_document=pdf_content.file_path,
                    confidence_score=item_data.get('confidence_score', 0.5)
                )
                equipment_items.append(equipment_item)
            
            return equipment_items
            
        except Exception as e:
            logging.error(f"AI extraction from table failed: {e}")
            raise Exception(f"Failed to extract equipment data from table: {e}")
    
    def _ai_extract_from_text(self, text_chunk: str, pdf_content: PDFContent, chunk_idx: int) -> List[EquipmentItem]:
        """Use AI to extract equipment data from text chunk."""
        prompt = f"""
        Extract audio-visual equipment information from the following text. 
        Look for mentions of specific equipment with manufacturer, model, and quantities.
        
        Return a JSON array of equipment items with this structure:
        {{
            "manufacturer": "string",
            "model": "string",
            "quantity": number,
            "equipment_type": "string", 
            "category": "lighting|sound|video",
            "specifications": {{"key": "value"}},
            "confidence_score": 0.0-1.0
        }}
        
        Guidelines:
        - Only extract clear equipment mentions with identifiable manufacturer/model
        - Normalize manufacturer names
        - Categorize as lighting, sound, or video equipment
        - Extract quantities mentioned in text
        - Include any specifications or features mentioned
        - Higher confidence for complete information
        
        Text:
        {text_chunk}
        
        Return only valid JSON array:
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert at extracting equipment information from technical documents. Always return valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1500
            )
            
            response_text = response.choices[0].message.content.strip()
            response_text = self._clean_json_response(response_text)
            
            equipment_data = json.loads(response_text)
            
            equipment_items = []
            for item_data in equipment_data:
                equipment_item = EquipmentItem(
                    manufacturer=item_data.get('manufacturer', ''),
                    model=item_data.get('model', ''),
                    quantity=item_data.get('quantity', 1),
                    equipment_type=item_data.get('equipment_type', ''),
                    category=item_data.get('category', ''),
                    venue=pdf_content.venue,
                    specifications=item_data.get('specifications', {}),
                    source_document=pdf_content.file_path,
                    confidence_score=item_data.get('confidence_score', 0.5)
                )
                equipment_items.append(equipment_item)
            
            return equipment_items
            
        except Exception as e:
            logging.error(f"AI extraction from text failed: {e}")
            raise Exception(f"Failed to extract equipment data from text: {e}")
    
    def _table_to_text(self, table: List[List[str]]) -> str:
        """Convert table structure to readable text format."""
        if not table:
            return ""
        
        # Create formatted table text
        table_text = ""
        for row_idx, row in enumerate(table):
            if row_idx == 0:
                table_text += "Headers: " + " | ".join(row) + "\n"
                table_text += "-" * 50 + "\n"
            else:
                table_text += "Row: " + " | ".join(row) + "\n"
        
        return table_text
    
    def _split_text_into_chunks(self, text: str, max_tokens: int = 3000) -> List[str]:
        """Split text into chunks that fit within token limits."""
        # Rough estimation: 1 token ≈ 4 characters
        max_chars = max_tokens * 4
        
        if len(text) <= max_chars:
            return [text]
        
        chunks = []
        current_chunk = ""
        
        # Split by paragraphs first
        paragraphs = text.split('\n\n')
        
        for paragraph in paragraphs:
            if len(current_chunk) + len(paragraph) <= max_chars:
                current_chunk += paragraph + "\n\n"
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = paragraph + "\n\n"
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _chunk_contains_equipment_info(self, chunk: str) -> bool:
        """Check if a text chunk likely contains equipment information."""
        chunk_lower = chunk.lower()
        
        # Check for equipment-related keywords
        equipment_indicators = [
            'manufacturer', 'model', 'quantity', 'specifications',
            'audio', 'video', 'lighting', 'sound', 'microphone', 'speaker',
            'projector', 'screen', 'mixer', 'amplifier', 'led', 'fixture',
            'console', 'system', 'equipment', 'device'
        ]
        
        # Check for manufacturer names
        manufacturer_indicators = list(MANUFACTURER_MAPPINGS.keys())
        
        all_indicators = equipment_indicators + manufacturer_indicators
        
        return any(indicator in chunk_lower for indicator in all_indicators)
    
    def _clean_json_response(self, response_text: str) -> str:
        """Clean AI response to ensure valid JSON."""
        # Remove markdown code blocks
        response_text = re.sub(r'```json\s*', '', response_text)
        response_text = re.sub(r'```\s*$', '', response_text)
        
        # Remove any text before the first [ or {
        match = re.search(r'[\[\{]', response_text)
        if match:
            response_text = response_text[match.start():]
        
        # Remove any text after the last ] or }
        for i in range(len(response_text) - 1, -1, -1):
            if response_text[i] in ']}':
                response_text = response_text[:i + 1]
                break
        
        return response_text.strip()
    
    def _deduplicate_equipment(self, equipment_items: List[EquipmentItem]) -> List[EquipmentItem]:
        """Remove duplicate equipment items and merge similar ones."""
        if not equipment_items:
            return []
        
        # Group similar items
        grouped_items = {}
        
        for item in equipment_items:
            # Create a key for grouping similar items
            key = f"{item.manufacturer.lower()}_{item.model.lower()}_{item.category.lower()}"
            
            if key in grouped_items:
                # Merge with existing item
                existing_item = grouped_items[key]
                existing_item.quantity += item.quantity
                
                # Merge specifications
                existing_item.specifications.update(item.specifications)
                
                # Use higher confidence score
                existing_item.confidence_score = max(existing_item.confidence_score, item.confidence_score)
                
            else:
                grouped_items[key] = item
        
        return list(grouped_items.values())
    
    def get_equipment_features(self, manufacturer: str, model: str) -> Dict[str, Any]:
        """
        Get detailed features for a specific equipment model using AI.
        
        Args:
            manufacturer: Equipment manufacturer
            model: Equipment model
            
        Returns:
            Dictionary of equipment features and specifications
        """
        prompt = f"""
        Provide detailed technical specifications and features for the following audio-visual equipment:
        
        Manufacturer: {manufacturer}
        Model: {model}
        
        Return a JSON object with the following structure:
        {{
            "category": "lighting|sound|video",
            "type": "specific equipment type",
            "features": ["list", "of", "key", "features"],
            "specifications": {{
                "power": "power consumption",
                "dimensions": "physical dimensions", 
                "weight": "weight",
                "connectivity": ["connection", "types"],
                "other_specs": "any other relevant specs"
            }},
            "typical_applications": ["list", "of", "typical", "uses"],
            "compatibility": ["compatible", "systems", "or", "standards"]
        }}
        
        If you don't have specific information about this model, provide general information for similar equipment from this manufacturer.
        
        Return only valid JSON:
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert in audio-visual equipment specifications. Always return valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            response_text = response.choices[0].message.content.strip()
            response_text = self._clean_json_response(response_text)
            
            features = json.loads(response_text)
            return features
            
        except Exception as e:
            logging.error(f"Error getting equipment features for {manufacturer} {model}: {e}")
            raise Exception(f"Failed to get equipment features for {manufacturer} {model}: {e}")
