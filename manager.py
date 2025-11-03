import cdsapi
import os
import time
import re
import sqlite3
import logging
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from cdsapi.api import Result # <-- Import Result for API client

# --- Configuration ---
load_dotenv()
CDS_USERNAME = os.getenv("CDS_USERNAME")
CDS_PASSWORD = os.getenv("CDS_PASSWORD")

DB_NAME = "requests.db"
LOG_FILE = "manager.log"
MAX_ACTIVE_REQUESTS = 8
output_dir = "era5_data"
LOOP_SLEEP_SECONDS = 3600  # 1 hour

years_to_download = [str(year) for year in range(2019, 2025)]
variables_to_download = [
    "10m_u_component_of_wind", "10m_v_component_of_wind", "2m_dewpoint_temperature",
    "2m_temperature", "mean_sea_level_pressure", "sea_surface_temperature",
    "surface_pressure", "total_precipitation"
]
bounding_boxes = {
    'AL': [35.008323, -88.474595, 30.222501, -84.89248], 'AK': [71.365162, -179.148909, 51.214183, 179.77847],
    'AZ': [37.00426, -114.81651, 31.332177, -109.045223], 'AR': [36.49965, -94.619466, 33.004136, -89.655473],
    'CA': [42.009659, -124.410607, 32.534156, -114.131211], 'CO': [41.003444, -109.060253, 36.992426, -102.041524],
    'CT': [42.050894, -73.727775, 40.980144, -71.786994], 'DE': [39.839007, -75.79003, 38.451013, -75.048939],
    'DC': [38.99511, -77.119759, 38.791645, -76.909395], 'FL': [31.000888, -87.634938, 24.514909, -80.031362],
    'GA': [35.000659, -85.606749, 30.357851, -80.839729], 'HI': [28.402123, -178.334698, 18.910361, -154.806773],
    'ID': [49.001146, -117.243027, 41.988057, -111.043564], 'IL': [42.508773, -91.514727, 36.970298, -87.494718],
    'IN': [41.760592, -88.09776, 37.771742, -84.784579], 'IA': [43.501196, -96.639704, 40.375501, -90.140028],
    'KS': [40.003162, -102.052894, 36.992751, -94.588413], 'KY': [39.147458, -89.572919, 36.497073, -81.964971],
    'LA': [33.019599, -94.043147, 28.928609, -88.815578], 'ME': [47.459686, -71.084466, 42.977764, -66.949895],
    'MD': [39.723043, -79.487651, 37.911717, -75.048939], 'MA': [42.886759, -73.508142, 41.237964, -69.928393],
    'MI': [48.2388, -90.418136, 41.696102, -82.413474], 'MN': [49.384687, -97.239651, 43.499269, -89.490365],
    'MS': [34.996052, -91.655009, 30.173943, -88.097888], 'MO': [40.613687, -95.774704, 35.995382, -89.098843],
    'MT': [49.001546, -116.051141, 44.358221, -104.039138], 'NE': [43.001708, -104.053514, 39.999998, -95.30829],
    'NV': [42.002207, -120.006543, 35.00145, -114.039648], 'NH': [45.305871, -72.557247, 42.696907, -70.610621],
    'NJ': [41.357633, -75.560315, 38.928212, -73.893979], 'NM': [37.000482, -109.050173, 31.332301, -103.000468],
    'NY': [45.01585, -79.763379, 40.496103, -71.856164], 'NC': [36.588133, -84.321869, 33.842316, -75.459815],
    'ND': [49.000687, -104.0489, 45.934703, -96.554507], 'OH': [41.977874, -84.820694, 38.403202, -80.518693],
    'OK': [37.002206, -103.004057, 33.615833, -94.430662], 'OR': [46.292035, -124.566244, 41.991619, -116.463504],
    'PA': [42.269954, -80.52072, 39.7198, -74.689516], 'PR': [18.516095, -67.945404, 17.88328, -65.220703],
    'RI': [42.019109, -71.862772, 41.146339, -71.120359], 'SC': [35.215402, -83.35391, 32.034258, -78.539429],
    'SD': [45.94545, -104.05931, 42.479635, -96.435649], 'TN': [36.678335, -90.310298, 34.982551, -81.6469],
    'TX': [36.500704, -106.647191, 25.837377, -93.508292], 'UT': [42.001928, -114.052962, 36.997905, -109.041058],
    'VT': [45.016659, -73.439043, 42.726853, -71.464555], 'VA': [39.466012, -83.675709, 36.540738, -75.240868],
    'WA': [49.002494, -124.763068, 45.543541, -116.915989], 'WV': [40.638801, -82.644739, 37.201483, -77.719519],
    'WI': [47.080621, -92.889427, 42.491592, -86.805415], 'WY': [45.006059, -111.058433, 40.994746, -104.052131]
}
states_to_download = list(bounding_boxes.keys())

# --- Logging Setup ---
def setup_logging():
    """Sets up a logger that writes to file and console."""
    logger = logging.getLogger('cds_manager')
    logger.setLevel(logging.DEBUG) 
    
    # File handler
    fh = logging.FileHandler(LOG_FILE, mode='a') # 'a' to append
    fh.setLevel(logging.DEBUG)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(file_formatter)
    ch.setFormatter(console_formatter)

    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)
        
    return logger

# --- Database Functions ---
def setup_database(logger):
    """Creates the database table if it doesn't exist."""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        # Add the 'download' column from your add_column script
        c.execute("""
        CREATE TABLE IF NOT EXISTS requests (
            request_id TEXT PRIMARY KEY,
            state_abbr TEXT NOT NULL,
            year TEXT NOT NULL,
            output_filename TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            location TEXT,
            content_length INTEGER,
            download BOOLEAN DEFAULT 0, 
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """)
        conn.commit()
        conn.close()
        logger.info("Database setup complete.")
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")

def get_all_filenames_in_db(logger):
    """Gets a set of all filenames in the DB to prevent re-submission."""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT output_filename FROM requests")
        filenames = set(row[0] for row in c.fetchall())
        conn.close()
        return filenames
    except Exception as e:
        logger.error(f"Failed to get filenames from DB: {e}")
        return set()

def parse_size_to_bytes(size_str):
    if not size_str: return None
    size_str = size_str.strip()
    match = re.match(r'([\d.]+)\s*(\w+)', size_str)
    if not match: return None
    try:
        value = float(match.group(1))
        unit = match.group(2).upper()
        if unit == 'KB': return int(value * 1024)
        elif unit == 'MB': return int(value * 1024 * 1024)
        elif unit == 'GB': return int(value * 1024 * 1024 * 1024)
        elif unit == 'TB': return int(value * 1024 * 1024 * 1024 * 1024)
        elif unit == 'B': return int(value)
        else: return None
    except Exception: return None

# --- Selenium Functions (from update_status.py) ---
def selenium_login(driver, logger):
    """Performs the initial login and cookie banner."""
    logger.info("Attempting login...")
    driver.get("https://cds.climate.copernicus.eu/")

    # Action 2: Handle Cookie Banner
    try:
        logger.info("Waiting for cookie banner...")
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Deny all']"))
        ).click()
        logger.info("Clicked 'Deny all' on cookie banner.")
    except Exception:
        logger.info("Cookie banner not found or 'Deny all' not clickable. Continuing...")

    # Action 3: Click "Login - Register"
    logger.info("Waiting for Login button...")
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[.//p[text()='Login - Register']]"))
    ).click()

    # Action 4: Input Username and Password
    logger.info("Waiting for login form...")
    username_field = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "username"))
    )
    
    logger.info("Entering credentials...")
    username_field.send_keys(CDS_USERNAME)
    driver.find_element(By.ID, "password").send_keys(CDS_PASSWORD)

    # Action 5: Hit Enter
    logger.info("Logging in...")
    driver.find_element(By.ID, "password").send_keys(Keys.RETURN)

    # Action 6: Click "Your requests"
    logger.info("Waiting for 'Your requests' link...")
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Your requests"))
    ).click()
    
    # Wait for the page to load
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-requid]"))
    )
    logger.info("Login successful. On 'Your requests' page.")


def update_status_via_selenium(driver, logger):
    """
    Uses the Selenium browser to scrape the true status of all requests
    and updates the local database.
    Returns the count of currently active (queued/running) requests.
    """
    logger.info("--- Starting status update via Selenium ---")
    active_count = 0
    
    try:
        # Go to the requests page (or refresh it)
        driver.get("https://cds.climate.copernicus.eu/requests?tab=all")
        
        # Wait for the first request row to be visible
        logger.info("Waiting for request list to load...")
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-requid]"))
        )
        logger.info("Scraping request IDs and statuses...")
        
        request_rows = driver.find_elements(By.CSS_SELECTOR, "div[data-requid]")
        logger.info(f"Found {len(request_rows)} requests on page.")
        
        scraped_data = []
        for row in request_rows:
            try:
                request_id = row.get_attribute("data-requid")
                status_element = row.find_element(By.CSS_SELECTOR, 'span[class^="sc-d2474931-"]')
                status_text = status_element.text
                location = None
                content_length_str = None
                
                if status_text == 'Complete':
                    try:
                        link_element = row.find_element(By.LINK_TEXT, "Download")
                        location = link_element.get_attribute('href')
                    except NoSuchElementException:
                        logger.warning(f"Warning: 'Complete' request {request_id} has no Download link.")
                    try:
                        size_element = row.find_element(By.CSS_SELECTOR, 'p[class^="sc-d5be8ee9-8"]')
                        content_length_str = size_element.text
                    except NoSuchElementException:
                        logger.warning(f"Warning: 'Complete' request {request_id} has no file size.")
                
                if request_id and status_text:
                    scraped_data.append({
                        "id": request_id, 
                        "status": status_text,
                        "location": location,
                        "content_length_str": content_length_str
                    })
            except Exception as e:
                logger.warning(f"Could not parse a row. Error: {e}")
        
        # Now, update the database
        if scraped_data:
            logger.info(f"--- Updating local database with {len(scraped_data)} scraped items ---")
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            
            status_map = {
                'Rejected': 'failed',
                'Queued': 'queued',
                'In progress': 'running',
                'Complete': 'completed'
            }
            
            updated_count = 0
            for item in scraped_data:
                db_status = status_map.get(item['status'])
                if not db_status:
                    logger.warning(f"Skipping unknown status: {item['status']} for ID {item['id']}")
                    continue
                
                if db_status in ('queued', 'running'):
                    active_count += 1
                    
                request_id = item['id']
                location = item['location']
                content_length = parse_size_to_bytes(item['content_length_str'])
                now_time = datetime.now()
                
                try:
                    c.execute(
                        """
                        UPDATE requests 
                        SET status = ?, location = ?, content_length = ?, updated_at = ?
                        WHERE request_id = ?
                        """,
                        (db_status, location, content_length, now_time, request_id)
                    )
                    if c.rowcount > 0:
                        updated_count += 1
                except Exception as e:
                    logger.error(f"Error updating DB for {request_id}: {e}")
                    
            conn.commit()
            conn.close()
            logger.info(f"Database update complete. {updated_count} rows updated.")
            
    except TimeoutException:
        logger.error("Timed out waiting for requests page. Session may be invalid.")
        raise Exception("Session timeout") # Will be caught by main loop
    except Exception as e:
        logger.error(f"Error during Selenium scrape: {e}")
        driver.save_screenshot("manager_error.png")
        raise e # Will be caught by main loop
        
    logger.info(f"--- Status update finished. Active jobs: {active_count} ---")
    return active_count

# --- API Functions (from submit.py) ---
def submit_new_requests(api_client, logger, current_active_count):
    """
    Submits new requests via the API if there are available slots.
    """
    logger.info("--- Starting new request submission ---")
    available_slots = MAX_ACTIVE_REQUESTS - current_active_count
    
    if available_slots <= 0:
        logger.info(f"Max active request limit ({MAX_ACTIVE_REQUESTS}) reached. No new requests will be submitted.")
        return

    logger.info(f"Have {available_slots} available slots. Starting new request submissions...")

    db_filenames = get_all_filenames_in_db(logger)
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    submitted_count = 0
    stop_submission = False

    three_month_chunks = [
        {'label': 'Jan-Mar', 'months': ['01', '02', '03']},
        {'label': 'Apr-Jun', 'months': ['04', '05', '06']},
        {'label': 'Jul-Sep', 'months': ['07', '08', '09']},
        {'label': 'Oct-Dec', 'months': ['10', '11', '12']}
    ]

    for state_abbr in states_to_download:
        if stop_submission: break
        if state_abbr not in bounding_boxes:
            logger.warning(f"Bounding box for state '{state_abbr}' not found. Skipping.")
            continue
        
        state_area = bounding_boxes[state_abbr]
        
        for year in years_to_download:
            if stop_submission: break
            
            for chunk in three_month_chunks:
                if stop_submission: break
                
                target_filename = f"ERA5_hourly_multivariable_{state_abbr}_{year}_{chunk['label']}.nc"
                target_path = os.path.join(output_dir, target_filename)

                # --- Idempotency Checks ---
                if target_filename in db_filenames:
                    continue # Already in DB, skip
                
                if os.path.exists(target_path):
                    logger.warning(f"Skipping {target_filename}: File already exists on disk.")
                    continue
                # --- End Checks ---

                logger.info(f"Submitting request for: {target_filename}")

                try:
                    result = api_client.retrieve(
                        'reanalysis-era5-single-levels',
                        {
                            'product_type': ['reanalysis'],
                            'variable': variables_to_download,
                            'year': [year],
                            'month': chunk['months'],
                            'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
                                    '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
                                    '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
                            'time': ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                                     '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                                     '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                                     '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'],
                            'format': 'netcdf', 
                            'area': state_area,
                        }
                    )
                    
                    # Request submitted, add to DB
                    now_time = datetime.now()
                    request_id = result.reply['request_id']
                    status = result.reply['state']
                    
                    # Note: We add the 'download' column here, it gets the default value of 0
                    c.execute(
                        "INSERT INTO requests (request_id, state_abbr, year, output_filename, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (request_id, state_abbr, year, target_filename, status, now_time, now_time)
                    )
                    conn.commit()
                    db_filenames.add(target_filename)
                    
                    logger.info(f"  > Submitted. ID: {request_id}, Status: {status}")
                    submitted_count += 1

                except Exception as e:
                    logger.error(f"ERROR: Request submission failed for {target_filename}.")
                    logger.error(f"Details: {e}")
                
                finally:
                    if submitted_count >= available_slots:
                        logger.info("Reached max active request limit. Stopping submissions for this cycle.")
                        stop_submission = True
                    
                    if not stop_submission:
                        logger.debug("Waiting 15s before next request...")
                        time.sleep(15)

    conn.close()
    logger.info(f"--- Request submission finished. Submitted {submitted_count} new requests. ---")

# --- Main Execution ---
def main():
    logger = setup_logging()
    logger.info("====== Starting CDS Manager Script ======")
    
    if not CDS_USERNAME or not CDS_PASSWORD:
        logger.error("Error: CDS_USERNAME or CDS_PASSWORD not found in .env file.")
        logger.error("Please create a .env file with your credentials.")
        exit()
        
    setup_database(logger)
    
    # Initialize API client (for submitting)
    api_client = cdsapi.Client(wait_until_complete=False)
    
    # Initialize Selenium driver (for status checking)
    logger.info("Setting up Selenium Chrome driver...")
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    try:
        # Initial login
        selenium_login(driver, logger)
        
        while True:
            try:
                logger.info("--- Starting new cycle ---")
                
                # 1. Update status via Selenium & get active count
                active_count = update_status_via_selenium(driver, logger)
                
                # 2. Submit new requests via API
                submit_new_requests(api_client, logger, active_count)
                
                # 3. Sleep for 1 hour
                logger.info(f"--- Cycle complete. Sleeping for {LOOP_SLEEP_SECONDS / 3600} hour(s) ---")
                time.sleep(LOOP_SLEEP_SECONDS)
                
            except Exception as e:
                logger.error(f"A non-fatal error occurred in the main loop: {e}")
                logger.warning("Attempting to re-login and continue cycle in 5 minutes...")
                driver.save_screenshot("manager_loop_error.png")
                try:
                    # Try to re-login
                    selenium_login(driver, logger)
                except Exception as login_e:
                    logger.critical(f"Re-login failed: {login_e}. Sleeping for 1 hour.")
                    time.sleep(3600) # Sleep long to avoid spamming
                    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Shutting down...")
    except Exception as e:
        logger.critical(f"A fatal error occurred: {e}")
        driver.save_screenshot("manager_fatal_error.png")
    finally:
        logger.info("====== Shutting down CDS Manager ======")
        driver.quit()

if __name__ == '__main__':
    main()
