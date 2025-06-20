"""
Configuration settings for the venue specification extraction tool.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
GOOGLE_SEARCH_API_KEY = os.getenv('GOOGLE_SEARCH_API_KEY')
GOOGLE_SEARCH_ENGINE_ID = os.getenv('GOOGLE_SEARCH_ENGINE_ID')

# Rate limiting
MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', 5))
REQUEST_DELAY_SECONDS = float(os.getenv('REQUEST_DELAY_SECONDS', 1.0))

# PDF processing
MAX_PDF_SIZE_MB = int(os.getenv('MAX_PDF_SIZE_MB', 50))
PDF_TIMEOUT_SECONDS = int(os.getenv('PDF_TIMEOUT_SECONDS', 30))

# File paths
OUTPUT_DIR = 'output'
PDF_CACHE_DIR = 'pdf_cache'
LOGS_DIR = 'logs'

# Equipment categories and keywords
EQUIPMENT_CATEGORIES = {
    'lighting': [
        'light', 'lighting', 'led', 'fixture', 'dimmer', 'console', 'spotlight',
        'floodlight', 'par', 'fresnel', 'moving head', 'wash', 'beam', 'strobe',
        'haze', 'fog', 'smoke', 'gobo', 'color changer', 'scroller'
    ],
    'sound': [
        'audio', 'sound', 'speaker', 'microphone', 'mic', 'mixer', 'amplifier',
        'amp', 'subwoofer', 'monitor', 'pa', 'system', 'wireless', 'di box',
        'compressor', 'equalizer', 'reverb', 'delay', 'console', 'board'
    ],
    'video': [
        'video', 'projector', 'screen', 'display', 'monitor', 'camera', 'switcher',
        'scaler', 'converter', 'splitter', 'matrix', 'recorder', 'player',
        'led wall', 'panel', 'processor', 'hdmi', 'sdi', 'streaming'
    ]
}

# Common manufacturer variations
MANUFACTURER_MAPPINGS = {
    'shure': ['shure', 'shure inc', 'shure incorporated'],
    'yamaha': ['yamaha', 'yamaha corporation', 'yamaha music'],
    'qsc': ['qsc', 'qsc audio', 'qsc llc'],
    'jbl': ['jbl', 'jbl professional', 'harman jbl'],
    'martin': ['martin', 'martin professional', 'martin lighting'],
    'chamsys': ['chamsys', 'chamsys ltd'],
    'etc': ['etc', 'electronic theatre controls', 'electronic theater controls'],
    'sony': ['sony', 'sony corporation', 'sony professional'],
    'panasonic': ['panasonic', 'panasonic corporation'],
    'barco': ['barco', 'barco nv'],
    'christie': ['christie', 'christie digital'],
    'epson': ['epson', 'seiko epson'],
    'avolites': ['avolites', 'avolites ltd'],
    'robe': ['robe', 'robe lighting'],
    'clay paky': ['clay paky', 'claypaky', 'clay-paky']
}

# PDF search keywords
PDF_KEYWORDS = [
    'technical specifications', 'tech specs', 'equipment list', 'inventory',
    'audio visual', 'av specs', 'production guide', 'technical rider',
    'equipment specifications', 'venue specifications', 'technical information'
]

# User agents for web scraping
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]