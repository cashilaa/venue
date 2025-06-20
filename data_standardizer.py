"""
Data standardization module for normalizing and mapping equipment data.
"""
import re
import logging
from typing import List, Dict, Optional, Set, Tuple
from fuzzywuzzy import fuzz, process
from dataclasses import dataclass, asdict
import json
from data_extractor import EquipmentItem
from config import MANUFACTURER_MAPPINGS, EQUIPMENT_CATEGORIES

@dataclass
class StandardizedEquipment:
    """Standardized equipment item structure."""
    id: str
    manufacturer: str
    model: str
    quantity: int
    equipment_type: str
    category: str
    venue: str
    specifications: Dict
    features: List[str]
    applications: List[str]
    compatibility: List[str]
    source_documents: List[str]
    confidence_score: float
    standardization_notes: List[str]

class DataStandardizer:
    """Handles data normalization and standardization."""
    
    def __init__(self):
        self.manufacturer_variations = self._build_manufacturer_variations()
        self.equipment_type_mappings = self._build_equipment_type_mappings()
        self.standardized_categories = set(EQUIPMENT_CATEGORIES.keys())
        
    def standardize_equipment_data(self, extracted_data: Dict[str, List[EquipmentItem]]) -> Dict[str, List[StandardizedEquipment]]:
        """
        Standardize extracted equipment data.
        
        Args:
            extracted_data: Dictionary mapping venue names to equipment items
            
        Returns:
            Dictionary mapping venue names to standardized equipment items
        """
        standardized_data = {}
        
        for venue_name, equipment_items in extracted_data.items():
            logging.info(f"Standardizing data for venue: {venue_name}")
            
            standardized_items = []
            for item in equipment_items:
                try:
                    standardized_item = self._standardize_single_item(item)
                    if standardized_item:
                        standardized_items.append(standardized_item)
                except Exception as e:
                    logging.error(f"Error standardizing item {item.manufacturer} {item.model}: {e}")
                    continue
            
            # Merge duplicate items after standardization
            merged_items = self._merge_duplicate_items(standardized_items)
            standardized_data[venue_name] = merged_items
            
        return standardized_data
    
    def _standardize_single_item(self, item: EquipmentItem) -> Optional[StandardizedEquipment]:
        """Standardize a single equipment item."""
        notes = []
        
        # Standardize manufacturer
        original_manufacturer = item.manufacturer
        standardized_manufacturer = self._standardize_manufacturer(item.manufacturer)
        if standardized_manufacturer != original_manufacturer:
            notes.append(f"Manufacturer normalized: {original_manufacturer} -> {standardized_manufacturer}")
        
        # Standardize model
        original_model = item.model
        standardized_model = self._standardize_model(item.model)
        if standardized_model != original_model:
            notes.append(f"Model normalized: {original_model} -> {standardized_model}")
        
        # Standardize category
        original_category = item.category
        standardized_category = self._standardize_category(item.category, item.equipment_type)
        if standardized_category != original_category:
            notes.append(f"Category normalized: {original_category} -> {standardized_category}")
        
        # Standardize equipment type
        original_type = item.equipment_type
        standardized_type = self._standardize_equipment_type(item.equipment_type, standardized_category)
        if standardized_type != original_type:
            notes.append(f"Equipment type normalized: {original_type} -> {standardized_type}")
        
        # Generate unique ID
        item_id = self._generate_item_id(standardized_manufacturer, standardized_model, item.venue)
        
        # Create standardized item
        standardized_item = StandardizedEquipment(
            id=item_id,
            manufacturer=standardized_manufacturer,
            model=standardized_model,
            quantity=item.quantity,
            equipment_type=standardized_type,
            category=standardized_category,
            venue=item.venue,
            specifications=self._standardize_specifications(item.specifications),
            features=[],  # Will be populated later
            applications=[],  # Will be populated later
            compatibility=[],  # Will be populated later
            source_documents=[item.source_document],
            confidence_score=item.confidence_score,
            standardization_notes=notes
        )
        
        return standardized_item
    
    def _standardize_manufacturer(self, manufacturer: str) -> str:
        """Standardize manufacturer name."""
        if not manufacturer:
            return "Unknown"
        
        manufacturer_clean = manufacturer.strip().lower()
        
        # Direct mapping check
        for standard_name, variations in self.manufacturer_variations.items():
            if manufacturer_clean in variations:
                return standard_name.title()
        
        # Fuzzy matching for close matches
        all_standard_names = list(self.manufacturer_variations.keys())
        match = process.extractOne(manufacturer_clean, all_standard_names, scorer=fuzz.ratio)
        
        if match and match[1] >= 80:  # 80% similarity threshold
            return match[0].title()
        
        # Clean up the original name
        cleaned = re.sub(r'\b(inc|incorporated|corp|corporation|ltd|limited|llc)\b', '', manufacturer_clean, flags=re.IGNORECASE)
        cleaned = re.sub(r'[^\w\s]', '', cleaned).strip()
        
        return cleaned.title() if cleaned else "Unknown"
    
    def _standardize_model(self, model: str) -> str:
        """Standardize model name."""
        if not model:
            return "Unknown"
        
        # Clean up model name
        model_clean = model.strip()
        
        # Remove common prefixes/suffixes
        model_clean = re.sub(r'^(model\s+|mod\s+)', '', model_clean, flags=re.IGNORECASE)
        model_clean = re.sub(r'\s+(series|ser)$', '', model_clean, flags=re.IGNORECASE)
        
        # Normalize spacing and special characters
        model_clean = re.sub(r'\s+', ' ', model_clean)
        model_clean = re.sub(r'[^\w\s\-/]', '', model_clean)
        
        return model_clean.strip() if model_clean else "Unknown"
    
    def _standardize_category(self, category: str, equipment_type: str) -> str:
        """Standardize equipment category."""
        if not category:
            category = ""
        
        category_clean = category.strip().lower()
        
        # Direct match
        if category_clean in self.standardized_categories:
            return category_clean
        
        # Try to infer from equipment type
        if equipment_type:
            inferred_category = self._infer_category_from_type(equipment_type)
            if inferred_category:
                return inferred_category
        
        # Fuzzy matching
        match = process.extractOne(category_clean, list(self.standardized_categories), scorer=fuzz.ratio)
        if match and match[1] >= 70:
            return match[0]
        
        return "other"
    
    def _standardize_equipment_type(self, equipment_type: str, category: str) -> str:
        """Standardize equipment type."""
        if not equipment_type:
            return "Unknown"
        
        type_clean = equipment_type.strip().lower()
        
        # Check predefined mappings
        if category in self.equipment_type_mappings:
            category_mappings = self.equipment_type_mappings[category]
            
            # Direct match
            if type_clean in category_mappings:
                return category_mappings[type_clean]
            
            # Fuzzy match within category
            match = process.extractOne(type_clean, list(category_mappings.keys()), scorer=fuzz.ratio)
            if match and match[1] >= 75:
                return category_mappings[match[0]]
        
        # Clean up the original type
        type_clean = re.sub(r'\b(system|device|unit|equipment)\b', '', type_clean)
        type_clean = re.sub(r'[^\w\s]', ' ', type_clean)
        type_clean = re.sub(r'\s+', ' ', type_clean).strip()
        
        return type_clean.title() if type_clean else "Unknown"
    
    def _standardize_specifications(self, specifications: Dict) -> Dict:
        """Standardize specification keys and values."""
        if not specifications:
            return {}
        
        standardized_specs = {}
        
        for key, value in specifications.items():
            # Standardize key
            std_key = self._standardize_spec_key(key)
            
            # Standardize value
            std_value = self._standardize_spec_value(value, std_key)
            
            standardized_specs[std_key] = std_value
        
        return standardized_specs
    
    def _standardize_spec_key(self, key: str) -> str:
        """Standardize specification key names."""
        if not key:
            return "unknown"
        
        key_mappings = {
            'power': ['power', 'wattage', 'watts', 'power consumption', 'power draw'],
            'dimensions': ['dimensions', 'size', 'measurements', 'dims'],
            'weight': ['weight', 'mass'],
            'frequency_response': ['frequency response', 'freq response', 'frequency range'],
            'impedance': ['impedance', 'ohms'],
            'spl': ['spl', 'sound pressure level', 'max spl'],
            'throw_distance': ['throw distance', 'projection distance'],
            'resolution': ['resolution', 'native resolution'],
            'brightness': ['brightness', 'lumens', 'ansi lumens'],
            'contrast_ratio': ['contrast ratio', 'contrast'],
            'connectivity': ['connectivity', 'connections', 'inputs', 'outputs']
        }
        
        key_clean = key.strip().lower()
        
        for standard_key, variations in key_mappings.items():
            if key_clean in variations:
                return standard_key
        
        # Clean up original key
        key_clean = re.sub(r'[^\w\s]', '_', key_clean)
        key_clean = re.sub(r'\s+', '_', key_clean)
        
        return key_clean
    
    def _standardize_spec_value(self, value: str, key: str) -> str:
        """Standardize specification values."""
        if not value:
            return ""
        
        value_str = str(value).strip()
        
        # Power standardization
        if key == 'power':
            power_match = re.search(r'(\d+(?:\.\d+)?)\s*(w|watts?|kw)', value_str, re.IGNORECASE)
            if power_match:
                power_val = float(power_match.group(1))
                unit = power_match.group(2).lower()
                if unit.startswith('kw'):
                    power_val *= 1000
                return f"{power_val}W"
        
        # Weight standardization
        elif key == 'weight':
            weight_match = re.search(r'(\d+(?:\.\d+)?)\s*(kg|lbs?|pounds?)', value_str, re.IGNORECASE)
            if weight_match:
                weight_val = float(weight_match.group(1))
                unit = weight_match.group(2).lower()
                if unit.startswith('lb') or unit.startswith('pound'):
                    weight_val *= 0.453592  # Convert to kg
                return f"{weight_val:.1f}kg"
        
        # Frequency standardization
        elif key == 'frequency_response':
            freq_match = re.search(r'(\d+)\s*hz?\s*-\s*(\d+(?:\.\d+)?)\s*(khz?|hz)', value_str, re.IGNORECASE)
            if freq_match:
                low_freq = int(freq_match.group(1))
                high_freq = float(freq_match.group(2))
                unit = freq_match.group(3).lower()
                if unit.startswith('k'):
                    high_freq *= 1000
                return f"{low_freq}Hz - {int(high_freq)}Hz"
        
        return value_str
    
    def _infer_category_from_type(self, equipment_type: str) -> Optional[str]:
        """Infer category from equipment type."""
        type_lower = equipment_type.lower()
        
        for category, keywords in EQUIPMENT_CATEGORIES.items():
            if any(keyword in type_lower for keyword in keywords):
                return category
        
        return None
    
    def _generate_item_id(self, manufacturer: str, model: str, venue: str) -> str:
        """Generate unique ID for equipment item."""
        # Create a hash-like ID from manufacturer, model, and venue
        id_string = f"{manufacturer}_{model}_{venue}".lower()
        id_string = re.sub(r'[^\w]', '_', id_string)
        id_string = re.sub(r'_+', '_', id_string)
        
        return id_string.strip('_')
    
    def _merge_duplicate_items(self, items: List[StandardizedEquipment]) -> List[StandardizedEquipment]:
        """Merge duplicate standardized items."""
        if not items:
            return []
        
        merged_items = {}
        
        for item in items:
            # Create merge key
            merge_key = f"{item.manufacturer}_{item.model}_{item.venue}".lower()
            
            if merge_key in merged_items:
                # Merge with existing item
                existing = merged_items[merge_key]
                existing.quantity += item.quantity
                
                # Merge specifications
                existing.specifications.update(item.specifications)
                
                # Merge source documents
                existing.source_documents.extend(item.source_documents)
                existing.source_documents = list(set(existing.source_documents))
                
                # Merge notes
                existing.standardization_notes.extend(item.standardization_notes)
                
                # Use higher confidence score
                existing.confidence_score = max(existing.confidence_score, item.confidence_score)
                
            else:
                merged_items[merge_key] = item
        
        return list(merged_items.values())
    
    def _build_manufacturer_variations(self) -> Dict[str, List[str]]:
        """Build comprehensive manufacturer variations mapping."""
        variations = {}
        
        for standard_name, name_list in MANUFACTURER_MAPPINGS.items():
            variations[standard_name] = [name.lower() for name in name_list]
        
        return variations
    
    def _build_equipment_type_mappings(self) -> Dict[str, Dict[str, str]]:
        """Build equipment type standardization mappings."""
        return {
            'lighting': {
                'led fixture': 'LED Fixture',
                'led light': 'LED Fixture',
                'moving head': 'Moving Head',
                'moving light': 'Moving Head',
                'par can': 'PAR Can',
                'par light': 'PAR Can',
                'spotlight': 'Spotlight',
                'floodlight': 'Floodlight',
                'fresnel': 'Fresnel',
                'wash light': 'Wash Light',
                'beam light': 'Beam Light',
                'strobe': 'Strobe',
                'dimmer': 'Dimmer',
                'lighting console': 'Lighting Console',
                'dmx controller': 'DMX Controller'
            },
            'sound': {
                'speaker': 'Speaker',
                'loudspeaker': 'Speaker',
                'microphone': 'Microphone',
                'mic': 'Microphone',
                'wireless microphone': 'Wireless Microphone',
                'wireless mic': 'Wireless Microphone',
                'mixer': 'Audio Mixer',
                'mixing console': 'Audio Mixer',
                'amplifier': 'Amplifier',
                'power amplifier': 'Power Amplifier',
                'subwoofer': 'Subwoofer',
                'monitor': 'Monitor Speaker',
                'di box': 'DI Box',
                'compressor': 'Compressor',
                'equalizer': 'Equalizer',
                'reverb': 'Reverb Unit'
            },
            'video': {
                'projector': 'Projector',
                'lcd projector': 'LCD Projector',
                'led projector': 'LED Projector',
                'screen': 'Projection Screen',
                'projection screen': 'Projection Screen',
                'display': 'Display',
                'monitor': 'Video Monitor',
                'led wall': 'LED Wall',
                'led panel': 'LED Panel',
                'camera': 'Camera',
                'video camera': 'Video Camera',
                'switcher': 'Video Switcher',
                'video mixer': 'Video Switcher',
                'scaler': 'Video Scaler',
                'converter': 'Video Converter',
                'splitter': 'Video Splitter',
                'matrix': 'Video Matrix'
            }
        }
    
    def validate_standardized_data(self, standardized_data: Dict[str, List[StandardizedEquipment]]) -> Dict[str, List[str]]:
        """Validate standardized data and return validation report."""
        validation_report = {}
        
        for venue_name, items in standardized_data.items():
            venue_issues = []
            
            for item in items:
                # Check required fields
                if not item.manufacturer or item.manufacturer == "Unknown":
                    venue_issues.append(f"Missing manufacturer for item: {item.id}")
                
                if not item.model or item.model == "Unknown":
                    venue_issues.append(f"Missing model for item: {item.id}")
                
                if item.quantity <= 0:
                    venue_issues.append(f"Invalid quantity for item: {item.id}")
                
                if item.category not in self.standardized_categories and item.category != "other":
                    venue_issues.append(f"Invalid category '{item.category}' for item: {item.id}")
                
                if item.confidence_score < 0.3:
                    venue_issues.append(f"Low confidence score ({item.confidence_score}) for item: {item.id}")
            
            validation_report[venue_name] = venue_issues
        
        return validation_report