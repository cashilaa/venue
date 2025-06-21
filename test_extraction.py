#!/usr/bin/env python3
"""
Simple test script for the venue specification extraction tool.
"""
import asyncio
import os
from venue_spec_extractor import VenueSpecExtractor

def test_basic_functionality():
    """Test basic functionality with a small venue list."""
    print("Testing Venue Specification Extractor...")
    
    # Test venues - small list for quick testing
    test_venues = [
        "Lincoln Center",
        "Kennedy Center"
    ]
    
    try:
        # Initialize extractor
        extractor = VenueSpecExtractor()
        print(f"✓ Extractor initialized successfully")
        
        # Test extraction process
        print(f"Testing with venues: {test_venues}")
        
        # Run extraction
        exported_files = asyncio.run(
            extractor.extract_venue_specifications(test_venues)
        )
        
        print("✓ Extraction completed!")
        print("Exported files:")
        for format_name, file_path in exported_files.items():
            if os.path.exists(file_path):
                print(f"  ✓ {format_name.upper()}: {file_path}")
            else:
                print(f"  ✗ {format_name.upper()}: {file_path} (not found)")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_basic_functionality()
    if success:
        print("\n🎉 Test completed successfully!")
    else:
        print("\n❌ Test failed!")