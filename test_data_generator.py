"""
Test data generator for creating sample venue specification PDFs when real ones aren't available.
"""
import os
import logging
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from typing import Dict, List

class TestDataGenerator:
    """Generate sample venue specification PDFs for testing."""
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        
    def generate_sample_pdfs(self, venue_names: List[str], output_dir: str = 'pdf_cache') -> Dict[str, List[Dict]]:
        """
        Generate sample PDFs for venues when real ones aren't available.
        
        Args:
            venue_names: List of venue names to generate PDFs for
            output_dir: Directory to save PDFs
            
        Returns:
            Dictionary mapping venue names to PDF information
        """
        os.makedirs(output_dir, exist_ok=True)
        venue_pdfs = {}
        
        for venue_name in venue_names:
            pdf_path = self._generate_venue_pdf(venue_name, output_dir)
            if pdf_path:
                venue_pdfs[venue_name] = [{
                    'url': f'file://{pdf_path}',
                    'title': f'{venue_name} Technical Specifications',
                    'venue': venue_name,
                    'source_page': 'generated_test_data',
                    'local_path': pdf_path,
                    'downloaded': True
                }]
                logging.info(f"Generated sample PDF for {venue_name}: {pdf_path}")
            else:
                venue_pdfs[venue_name] = []
                
        return venue_pdfs
    
    def _generate_venue_pdf(self, venue_name: str, output_dir: str) -> str:
        """Generate a sample PDF for a specific venue."""
        try:
            # Create filename
            safe_name = venue_name.replace(' ', '_').replace('/', '_')
            filename = f"{safe_name}_technical_specs.pdf"
            filepath = os.path.join(output_dir, filename)
            
            # Create PDF document
            doc = SimpleDocTemplate(filepath, pagesize=letter)
            story = []
            
            # Title
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=self.styles['Heading1'],
                fontSize=18,
                spaceAfter=30,
                alignment=1  # Center alignment
            )
            story.append(Paragraph(f"{venue_name} - Technical Specifications", title_style))
            story.append(Spacer(1, 20))
            
            # Introduction
            intro_text = f"""
            This document contains the technical specifications and equipment inventory for {venue_name}.
            The venue is equipped with state-of-the-art audio, video, and lighting systems to support
            various types of performances and events.
            """
            story.append(Paragraph(intro_text, self.styles['Normal']))
            story.append(Spacer(1, 20))
            
            # Audio Equipment Section
            story.append(Paragraph("Audio Equipment", self.styles['Heading2']))
            audio_data = self._get_sample_audio_equipment()
            audio_table = Table(audio_data)
            audio_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(audio_table)
            story.append(Spacer(1, 20))
            
            # Video Equipment Section
            story.append(Paragraph("Video Equipment", self.styles['Heading2']))
            video_data = self._get_sample_video_equipment()
            video_table = Table(video_data)
            video_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(video_table)
            story.append(Spacer(1, 20))
            
            # Lighting Equipment Section
            story.append(Paragraph("Lighting Equipment", self.styles['Heading2']))
            lighting_data = self._get_sample_lighting_equipment()
            lighting_table = Table(lighting_data)
            lighting_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(lighting_table)
            
            # Build PDF
            doc.build(story)
            return filepath
            
        except Exception as e:
            logging.error(f"Error generating PDF for {venue_name}: {e}")
            return None
    
    def _get_sample_audio_equipment(self) -> List[List[str]]:
        """Get sample audio equipment data."""
        return [
            ['Manufacturer', 'Model', 'Type', 'Quantity', 'Specifications'],
            ['Shure', 'SM58', 'Dynamic Microphone', '12', 'Frequency Response: 50-15,000 Hz'],
            ['Yamaha', 'QL5', 'Digital Mixing Console', '1', '32 Input Channels, 16 Mix Buses'],
            ['QSC', 'K12.2', 'Powered Speaker', '8', '2000W, 12" Woofer, 1.75" Tweeter'],
            ['Sennheiser', 'EW 100 G4', 'Wireless Microphone System', '6', 'UHF Band, 42 MHz Bandwidth'],
            ['JBL', 'VTX V25', 'Line Array Speaker', '16', '3-way, Neodymium Drivers'],
            ['Yamaha', 'DM2000', 'Digital Mixer', '1', '96 Input Channels, 24-bit/96kHz'],
            ['Crown', 'XTi 6002', 'Power Amplifier', '4', '2100W @ 4Ω, DSP Processing'],
            ['Shure', 'ULXD4', 'Wireless Receiver', '8', 'Digital, AES256 Encryption']
        ]
    
    def _get_sample_video_equipment(self) -> List[List[str]]:
        """Get sample video equipment data."""
        return [
            ['Manufacturer', 'Model', 'Type', 'Quantity', 'Specifications'],
            ['Sony', 'PXW-Z750', 'Professional Camera', '3', '4K, 3x 2/3" CMOS Sensors'],
            ['Barco', 'UDX-4K32', 'Projector', '2', '31,000 Lumens, 4K Resolution'],
            ['Blackmagic', 'ATEM 2 M/E', 'Video Switcher', '1', '20 Inputs, 4K Support'],
            ['Christie', 'D13WU-H', 'Projector', '1', '13,000 Lumens, WUXGA'],
            ['Panasonic', 'AW-UE150', 'PTZ Camera', '4', '4K, 20x Optical Zoom'],
            ['Roland', 'V-60HD', 'Video Switcher', '1', '6 HDMI Inputs, Full HD'],
            ['Da-Lite', 'Fast-Fold Deluxe', 'Projection Screen', '2', '16:9 Aspect Ratio, 20ft Wide'],
            ['AJA', 'KONA 5', 'Video I/O Card', '2', '4K/UltraHD, 12G-SDI']
        ]
    
    def _get_sample_lighting_equipment(self) -> List[List[str]]:
        """Get sample lighting equipment data."""
        return [
            ['Manufacturer', 'Model', 'Type', 'Quantity', 'Specifications'],
            ['ETC', 'ColorSource PAR', 'LED Par Light', '24', 'RGBA-Lime, 7-color LED Array'],
            ['Martin', 'MAC Quantum Wash', 'Moving Head Wash', '12', 'RGBW LED, 19° - 54° Zoom'],
            ['Chamsys', 'MagicQ MQ500', 'Lighting Console', '1', '202 Universes, Touch Screen'],
            ['Robe', 'Pointe', 'Moving Head Spot', '8', '280W Lamp, Prism, Gobos'],
            ['ETC', 'Ion Xe 20', 'Lighting Console', '1', '20 Faders, 40,960 Outputs'],
            ['Clay Paky', 'Mythos 2', 'Hybrid Moving Light', '6', '470W LED, Beam/Spot/Wash'],
            ['Avolites', 'Tiger Touch II', 'Lighting Console', '1', '20 Playback Faders'],
            ['High End', 'SolaFrame 750', 'LED Framing Spot', '16', '26,000 Lumens, Framing System'],
            ['MA Lighting', 'grandMA3 Light', 'Lighting Console', '1', '8,192 Parameters']
        ]
