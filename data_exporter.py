"""
Data export module for generating structured JSON output and reports.
"""
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import asdict
import pandas as pd
from data_standardizer import StandardizedEquipment
from data_extractor import DataExtractor
from config import OUTPUT_DIR

class DataExporter:
    """Handles data export and report generation."""
    
    def __init__(self):
        self.data_extractor = DataExtractor()
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
    def export_to_json(self, standardized_data: Dict[str, List[StandardizedEquipment]], 
                      filename: Optional[str] = None) -> str:
        """
        Export standardized data to JSON format.
        
        Args:
            standardized_data: Dictionary mapping venue names to standardized equipment
            filename: Optional custom filename
            
        Returns:
            Path to the exported JSON file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"venue_equipment_data_{timestamp}.json"
        
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # Convert to JSON-serializable format
        export_data = {
            "metadata": {
                "export_timestamp": datetime.now().isoformat(),
                "total_venues": len(standardized_data),
                "total_equipment_items": sum(len(items) for items in standardized_data.values()),
                "data_version": "1.0"
            },
            "venues": {}
        }
        
        for venue_name, equipment_items in standardized_data.items():
            venue_data = {
                "venue_name": venue_name,
                "equipment_count": len(equipment_items),
                "equipment": []
            }
            
            for item in equipment_items:
                item_dict = asdict(item)
                venue_data["equipment"].append(item_dict)
            
            export_data["venues"][venue_name] = venue_data
        
        # Write to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Data exported to JSON: {filepath}")
        return filepath
    
    def export_to_csv(self, standardized_data: Dict[str, List[StandardizedEquipment]], 
                     filename: Optional[str] = None) -> str:
        """
        Export standardized data to CSV format.
        
        Args:
            standardized_data: Dictionary mapping venue names to standardized equipment
            filename: Optional custom filename
            
        Returns:
            Path to the exported CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"venue_equipment_data_{timestamp}.csv"
        
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # Flatten data for CSV
        flattened_data = []
        
        for venue_name, equipment_items in standardized_data.items():
            for item in equipment_items:
                row = {
                    'venue': item.venue,
                    'manufacturer': item.manufacturer,
                    'model': item.model,
                    'quantity': item.quantity,
                    'equipment_type': item.equipment_type,
                    'category': item.category,
                    'confidence_score': item.confidence_score,
                    'source_documents': '; '.join(item.source_documents),
                    'specifications': json.dumps(item.specifications),
                    'features': '; '.join(item.features),
                    'applications': '; '.join(item.applications),
                    'compatibility': '; '.join(item.compatibility),
                    'standardization_notes': '; '.join(item.standardization_notes)
                }
                flattened_data.append(row)
        
        # Create DataFrame and export
        df = pd.DataFrame(flattened_data)
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        logging.info(f"Data exported to CSV: {filepath}")
        return filepath
    
    def generate_summary_report(self, standardized_data: Dict[str, List[StandardizedEquipment]], 
                               validation_report: Dict[str, List[str]]) -> str:
        """
        Generate a comprehensive summary report.
        
        Args:
            standardized_data: Standardized equipment data
            validation_report: Validation issues report
            
        Returns:
            Path to the generated report file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"venue_equipment_report_{timestamp}.json"
        report_filepath = os.path.join(OUTPUT_DIR, report_filename)
        
        # Calculate statistics
        total_venues = len(standardized_data)
        total_equipment = sum(len(items) for items in standardized_data.values())
        
        # Category breakdown
        category_stats = {}
        manufacturer_stats = {}
        venue_stats = {}
        
        for venue_name, equipment_items in standardized_data.items():
            venue_stats[venue_name] = {
                'total_items': len(equipment_items),
                'categories': {},
                'manufacturers': {},
                'avg_confidence': 0
            }
            
            confidence_scores = []
            
            for item in equipment_items:
                # Category stats
                if item.category not in category_stats:
                    category_stats[item.category] = 0
                category_stats[item.category] += item.quantity
                
                if item.category not in venue_stats[venue_name]['categories']:
                    venue_stats[venue_name]['categories'][item.category] = 0
                venue_stats[venue_name]['categories'][item.category] += item.quantity
                
                # Manufacturer stats
                if item.manufacturer not in manufacturer_stats:
                    manufacturer_stats[item.manufacturer] = 0
                manufacturer_stats[item.manufacturer] += item.quantity
                
                if item.manufacturer not in venue_stats[venue_name]['manufacturers']:
                    venue_stats[venue_name]['manufacturers'][item.manufacturer] = 0
                venue_stats[venue_name]['manufacturers'][item.manufacturer] += item.quantity
                
                confidence_scores.append(item.confidence_score)
            
            # Calculate average confidence for venue
            if confidence_scores:
                venue_stats[venue_name]['avg_confidence'] = sum(confidence_scores) / len(confidence_scores)
        
        # Generate report
        report = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "report_version": "1.0"
            },
            "summary": {
                "total_venues": total_venues,
                "total_equipment_items": total_equipment,
                "categories_found": len(category_stats),
                "manufacturers_found": len(manufacturer_stats)
            },
            "category_breakdown": category_stats,
            "manufacturer_breakdown": manufacturer_stats,
            "venue_statistics": venue_stats,
            "data_quality": {
                "validation_issues": validation_report,
                "total_validation_issues": sum(len(issues) for issues in validation_report.values())
            },
            "recommendations": self._generate_recommendations(standardized_data, validation_report)
        }
        
        # Write report
        with open(report_filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Summary report generated: {report_filepath}")
        return report_filepath
    
    def export_equipment_database(self, standardized_data: Dict[str, List[StandardizedEquipment]]) -> str:
        """
        Export equipment database with enhanced features for each device.
        
        Args:
            standardized_data: Standardized equipment data
            
        Returns:
            Path to the equipment database file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_filename = f"equipment_database_{timestamp}.json"
        db_filepath = os.path.join(OUTPUT_DIR, db_filename)
        
        equipment_database = {
            "database_metadata": {
                "created_at": datetime.now().isoformat(),
                "version": "1.0",
                "description": "Comprehensive equipment database with features and specifications"
            },
            "equipment": {}
        }
        
        # Create unique equipment entries
        unique_equipment = {}
        
        for venue_name, equipment_items in standardized_data.items():
            for item in equipment_items:
                # Create unique key for equipment model
                equipment_key = f"{item.manufacturer}_{item.model}".lower().replace(' ', '_')
                
                if equipment_key not in unique_equipment:
                    # Get enhanced features for this equipment
                    try:
                        features = self.data_extractor.get_equipment_features(
                            item.manufacturer, item.model
                        )
                    except Exception as e:
                        logging.error(f"Error getting features for {item.manufacturer} {item.model}: {e}")
                        features = {}
                    
                    unique_equipment[equipment_key] = {
                        "manufacturer": item.manufacturer,
                        "model": item.model,
                        "category": item.category,
                        "equipment_type": item.equipment_type,
                        "specifications": item.specifications,
                        "features": features.get('features', []),
                        "typical_applications": features.get('typical_applications', []),
                        "compatibility": features.get('compatibility', []),
                        "venues_found_in": [venue_name],
                        "total_quantity_across_venues": item.quantity,
                        "confidence_score": item.confidence_score
                    }
                else:
                    # Update existing entry
                    existing = unique_equipment[equipment_key]
                    if venue_name not in existing["venues_found_in"]:
                        existing["venues_found_in"].append(venue_name)
                    existing["total_quantity_across_venues"] += item.quantity
                    
                    # Merge specifications
                    existing["specifications"].update(item.specifications)
                    
                    # Update confidence score (use maximum)
                    existing["confidence_score"] = max(existing["confidence_score"], item.confidence_score)
        
        equipment_database["equipment"] = unique_equipment
        
        # Write database
        with open(db_filepath, 'w', encoding='utf-8') as f:
            json.dump(equipment_database, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Equipment database exported: {db_filepath}")
        return db_filepath
    
    def search_equipment_database(self, database_path: str, manufacturer: str = None, 
                                 model: str = None, category: str = None) -> List[Dict]:
        """
        Search the equipment database for specific equipment.
        
        Args:
            database_path: Path to the equipment database JSON file
            manufacturer: Manufacturer to search for
            model: Model to search for
            category: Category to filter by
            
        Returns:
            List of matching equipment entries
        """
        try:
            with open(database_path, 'r', encoding='utf-8') as f:
                database = json.load(f)
            
            equipment = database.get('equipment', {})
            results = []
            
            for equipment_key, equipment_data in equipment.items():
                match = True
                
                if manufacturer and manufacturer.lower() not in equipment_data['manufacturer'].lower():
                    match = False
                
                if model and model.lower() not in equipment_data['model'].lower():
                    match = False
                
                if category and category.lower() != equipment_data['category'].lower():
                    match = False
                
                if match:
                    results.append(equipment_data)
            
            return results
            
        except Exception as e:
            logging.error(f"Error searching equipment database: {e}")
            return []
    
    def _generate_recommendations(self, standardized_data: Dict[str, List[StandardizedEquipment]], 
                                 validation_report: Dict[str, List[str]]) -> List[str]:
        """Generate recommendations based on the data analysis."""
        recommendations = []
        
        # Data quality recommendations
        total_issues = sum(len(issues) for issues in validation_report.values())
        if total_issues > 0:
            recommendations.append(f"Address {total_issues} data quality issues identified in validation report")
        
        # Coverage recommendations
        venues_with_no_data = [venue for venue, items in standardized_data.items() if not items]
        if venues_with_no_data:
            recommendations.append(f"No equipment data found for {len(venues_with_no_data)} venues: {', '.join(venues_with_no_data)}")
        
        # Confidence score recommendations
        low_confidence_items = []
        for venue_name, items in standardized_data.items():
            for item in items:
                if item.confidence_score < 0.5:
                    low_confidence_items.append(f"{venue_name}: {item.manufacturer} {item.model}")
        
        if low_confidence_items:
            recommendations.append(f"Review {len(low_confidence_items)} items with low confidence scores")
        
        # Standardization recommendations
        unknown_manufacturers = set()
        unknown_models = set()
        
        for venue_name, items in standardized_data.items():
            for item in items:
                if item.manufacturer == "Unknown":
                    unknown_manufacturers.add(f"{venue_name}: {item.equipment_type}")
                if item.model == "Unknown":
                    unknown_models.add(f"{venue_name}: {item.manufacturer}")
        
        if unknown_manufacturers:
            recommendations.append(f"Identify manufacturers for {len(unknown_manufacturers)} equipment items")
        
        if unknown_models:
            recommendations.append(f"Identify models for {len(unknown_models)} equipment items")
        
        return recommendations
    
    def export_all_formats(self, standardized_data: Dict[str, List[StandardizedEquipment]], 
                          validation_report: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Export data in all available formats.
        
        Args:
            standardized_data: Standardized equipment data
            validation_report: Validation report
            
        Returns:
            Dictionary mapping format names to file paths
        """
        exported_files = {}
        
        try:
            # JSON export
            json_path = self.export_to_json(standardized_data)
            exported_files['json'] = json_path
            
            # CSV export
            csv_path = self.export_to_csv(standardized_data)
            exported_files['csv'] = csv_path
            
            # Summary report
            report_path = self.generate_summary_report(standardized_data, validation_report)
            exported_files['report'] = report_path
            
            # Equipment database
            db_path = self.export_equipment_database(standardized_data)
            exported_files['database'] = db_path
            
            logging.info(f"All formats exported successfully: {list(exported_files.keys())}")
            
        except Exception as e:
            logging.error(f"Error during export: {e}")
        
        return exported_files