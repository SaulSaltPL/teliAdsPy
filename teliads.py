import os
import json
import requests
from flask import Flask, jsonify
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from dateutil import parser as date_parser
import logging
import time
from typing import Dict, List, Optional, Any
from tenacity import retry, stop_after_attempt, wait_exponential

app = Flask(__name__)


@app.route('/_ah/warmup')
def warmup():
    """Warmup endpoint for Cloud Run"""
    logger.info("Warmup endpoint called")
    return '', 200

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
YESTERDAY_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
CONFIG_FILE = os.getenv('CONFIG_FILE', 'passkeys.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '1Pl24edGAhoovXPtHTTugsb3QM4YbcBsMjg6lBk9BXOs')
CUTOFF_DATE = datetime(2024, 9, 1)
API_VERSION = "v17.0"
MAX_RETRIES = 3

class FacebookAdsError(Exception):
    """Custom exception for Facebook Ads API errors"""
    pass

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def fetch_ad_creation_time(ad_id: str, access_token: str) -> Optional[datetime]:
    """Fetch ad creation time with retry logic"""
    logging.info(f"Fetching creation time for ad_id: {ad_id}")
    url = f"https://graph.facebook.com/{API_VERSION}/{ad_id}"
    params = {
        "fields": "created_time",
        "access_token": access_token
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            logging.error(f"Facebook API error for ad_id={ad_id}: {data['error']}")
            return None
            
        created_str = data.get("created_time")
        if not created_str:
            return None
            
        dt = date_parser.isoparse(created_str)
        logging.info(f"Successfully fetched creation time for ad_id: {ad_id}")
        return dt.replace(tzinfo=None)
    except Exception as e:
        logging.error(f"Error fetching creation time for {ad_id}: {e}")
        raise

def load_config() -> Dict[str, str]:
    """Load configuration from JSON file"""
    logging.info("Loading configuration...")
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Config file {CONFIG_FILE} not found")
        
    try:
        with open(CONFIG_FILE, "r") as file:
            config = json.load(file)
            
        required_keys = ["accessToken", "adAccountId"]
        missing_keys = [key for key in required_keys if key not in config]
        
        if missing_keys:
            raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")
            
        logging.info("Configuration loaded successfully")
        return config
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        raise

def init_sheets_api():
    """Initialize Google Sheets API"""
    logging.info("Initializing Google Sheets API...")
    creds_file = 'zeta-environs-448616-m0-cb4f0707f662.json'
    
    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"Google Sheets credentials file {creds_file} not found")
        
    try:
        credentials = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        logging.info("Google Sheets API initialized successfully")
        return service.spreadsheets()
    except Exception as e:
        logging.error(f"Failed to initialize Google Sheets API: {e}")
        raise

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def fetch_api_data(config: Dict[str, str]) -> List[Dict[str, Any]]:
    """Fetch Facebook Ads data"""
    logging.info("Fetching Facebook Ads data...")
    access_token = config["accessToken"]
    ad_account_id = config["adAccountId"]
    
    api_url = f"https://graph.facebook.com/{API_VERSION}/act_{ad_account_id}/insights"
    params = {
        "fields": "campaign_name,ad_name,spend,ad_id",
        "access_token": access_token,
        "level": "ad",
        "time_range": json.dumps({"since": YESTERDAY_DATE, "until": YESTERDAY_DATE}),
        "limit": 5000
    }
    
    all_data = []
    next_page = api_url
    
    while next_page:
        try:
            logging.info(f"Fetching data from: {next_page}")
            response = requests.get(next_page, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                raise FacebookAdsError(f"Facebook API error: {data['error']}")
                
            page_data = data.get("data", [])
            logging.info(f"Retrieved {len(page_data)} ads from current page")
            
            for ad in page_data:
                ad_id = ad.get("ad_id")
                if ad_id:
                    created_dt = fetch_ad_creation_time(ad_id, access_token)
                    if created_dt and created_dt >= CUTOFF_DATE:
                        all_data.append(ad)
                    else:
                        logging.info(f"Skipping ad {ad_id} created before cutoff date")
            
            next_page = data.get("paging", {}).get("next")
            if next_page:
                params = {}
                logging.info("Moving to next page...")
            
        except Exception as e:
            logging.error(f"Error fetching ads data: {e}")
            raise
    
    logging.info(f"Total ads fetched: {len(all_data)}")
    return all_data

def process_daily_data(api_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Process API data"""
    logging.info("Processing daily data...")
    daily_data = {}
    
    for entry in api_data:
        date = YESTERDAY_DATE  # Use yesterday's date for all entries
        
        if not daily_data.get(date):
            daily_data[date] = []
        
        try:
            spend = float(entry.get("spend", 0))
        except (TypeError, ValueError):
            logging.warning(f"Invalid spend value for entry: {entry}")
            spend = 0
        
        daily_data[date].append({
            "campaign_name": entry.get("campaign_name", "Not Available"),
            "ad_name": entry.get("ad_name", "Not Available"),
            "spend": spend,
            "date_start": date,
            "date_stop": date
        })
    
    logging.info(f"Processed {sum(len(entries) for entries in daily_data.values())} entries")
    return daily_data

def write_to_sheets(sheets, data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Write data to Google Sheets"""
    logging.info("Writing to Google Sheets...")
    try:
        rows = []
        for date, entries in data.items():
            for entry in entries:
                row = [
                    date,
                    entry['campaign_name'],
                    entry['ad_name'],
                    entry['spend'],
                    entry['date_start'],
                    entry['date_stop']
                ]
                rows.append(row)
        
        if not rows:
            logging.warning("No data to write to sheets")
            return
        
        tab_name = 'Sheet1'
        next_row = get_next_empty_row(sheets, SPREADSHEET_ID, tab_name)
        range_name = f'{tab_name}!A{next_row}:F{next_row + len(rows)}'
        
        logging.info(f"Writing {len(rows)} rows to range: {range_name}")
        body = {'values': rows}
        
        result = sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logging.info(f"Successfully wrote {len(rows)} rows to Google Sheets")
        logging.info(f"Sheets API response: {result}")
    except Exception as e:
        logging.error(f"Failed to write to Google Sheets: {e}")
        raise

def get_next_empty_row(sheets, spreadsheet_id: str, tab_name: str) -> int:
    """Get next empty row in sheet"""
    logging.info("Getting next empty row...")
    try:
        range_name = f"{tab_name}!A:A"
        result = sheets.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        values = result.get("values", [])
        next_row = len(values) + 1
        logging.info(f"Next empty row: {next_row}")
        return next_row
    except Exception as e:
        logging.error(f"Failed to get next empty row: {e}")
        raise

@app.route('/')
def home():
    """Root endpoint for Cloud Run health checks"""
    logger.info("Health check endpoint called")
    return jsonify({"status": "healthy"}), 200

@app.route('/sync')
def sync_data():
    """Endpoint to trigger sync"""
    logger.info("Starting sync process...")
    try:
        config = load_config()
        sheets = init_sheets_api()
        api_data = fetch_api_data(config)
        daily_data = process_daily_data(api_data)
        write_to_sheets(sheets, daily_data)
        logger.info("Sync completed successfully")
        return jsonify({"status": "success", "message": "Data sync completed"}), 200
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Get port from environment variable
    port = int(os.environ.get('PORT', '8080'))
    logger.info(f"Starting application on port {port}")
    app.run(host='0.0.0.0', port=port)