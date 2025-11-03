import os
import time
import re
import sqlite3
import zipfile
from datetime import datetime
from dotenv import load_dotenv
import requests  # <-- ADDED for direct downloading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# 1. Load credentials from .env file
load_dotenv()
CDS_USERNAME = os.getenv("CDS_USERNAME")
CDS_PASSWORD = os.getenv("CDS_PASSWORD")
DB_NAME = "requests.db"
output_dir = "era5_data"
# Set full path for download directory
DOWNLOAD_DIR = os.path.join(os.getcwd(), output_dir)

if not CDS_USERNAME or not CDS_PASSWORD:
    print("Error: CDS_USERNAME or CDS_PASSWORD not found in .env file.")
    print("Please create a .env file with your credentials.")
    exit()

def setup_database():
    """Creates the database table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # UPDATED TABLE to include 'download' column
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

# --- Helper function to handle unzip and rename ---
def process_downloaded_file(zip_path, target_nc_filename):
    """
    Unzips the downloaded file(s), renames them logically, and cleans up.
    Handles single files (data.nc) and multiple files (instant.nc, accum.nc).
    """
    print(f"  > Unzipping {os.path.basename(zip_path)}...")
    
    # Get the base filename without the .nc extension
    # e.g., "ERA5_hourly_multivariable_AL_2019_Jan-Mar"
    base_target_name = target_nc_filename.replace(".nc", "")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_contents = zip_ref.namelist()
        nc_files = [f for f in zip_contents if f.endswith('.nc')]
        
        if not nc_files:
            raise Exception(f"No .nc file found in {zip_path}")
            
        print(f"  > Found {len(nc_files)} .nc file(s): {', '.join(nc_files)}")

        for extracted_file_name in nc_files:
            # Determine the new filename
            if len(nc_files) == 1:
                # Only one file, use the original target name
                final_nc_filename = target_nc_filename
            else:
                # --- MODIFIED LOGIC ---
                # Check if file *contains* instant or accum
                if 'instant' in extracted_file_name:
                    final_nc_filename = f"{base_target_name}_instant.nc"
                elif 'accum' in extracted_file_name:
                    final_nc_filename = f"{base_target_name}_accum.nc"
                # --- END MODIFICATION ---
                else:
                    # Handle other cases, e.g., data1.nc, data2.nc
                    name_part = extracted_file_name.replace(".nc", "")
                    final_nc_filename = f"{base_target_name}_{name_part}.nc"
            
            final_nc_path = os.path.join(DOWNLOAD_DIR, final_nc_filename)
            
            # Extract the file
            zip_ref.extract(extracted_file_name, path=DOWNLOAD_DIR)
            extracted_file_path = os.path.join(DOWNLOAD_DIR, extracted_file_name)
            
            # Rename the extracted file
            if os.path.exists(final_nc_path):
                 print(f"Warning: Target file {final_nc_filename} already exists. Overwriting.")
                 os.remove(final_nc_path)
                 
            os.rename(extracted_file_path, final_nc_path)
            print(f"  > Renamed {extracted_file_name} to {final_nc_filename}")

    os.remove(zip_path)
    print(f"  > Removed temporary {os.path.basename(zip_path)}")

# --- Helper function to download file with requests ---
def download_file_with_session(url, target_zip_path, driver_cookies):
    """
    Downloads a file from a URL using a requests session
    and the browser's login cookies.
    """
    print(f"  > Downloading from {url[:50]}...")
    
    # 1. Create a requests session
    s = requests.Session()
    
    # 2. Load Selenium's cookies into the requests session
    for cookie in driver_cookies:
        s.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
        
    # 3. Download the file as a stream
    with s.get(url, stream=True) as r:
        r.raise_for_status() # Will stop if we get a 401/403/404
        
        # 4. Save the file to disk chunk by chunk
        with open(target_zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                
    print(f"  > Saved to {os.path.basename(target_zip_path)}")


# 2. Set up the Chrome driver automatically
print("Setting up Chrome driver...")
# We no longer need to set a download directory for Chrome
service = ChromeService(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# Ensure the output directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
setup_database()

# Use a try...finally block to make sure the browser always closes
try:
    # Action 1: Go to the website
    print("Opening cds.climate.copernicus.eu...")
    driver.get("https://cds.climate.copernicus.eu/")

    # Action 2: Handle Cookie Banner
    try:
        print("Waiting for cookie banner...")
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Deny all']"))
        ).click()
        print("Clicked 'Deny all' on cookie banner.")
    except Exception as e:
        print("Cookie banner not found or 'Deny all' not clickable. Continuing...")

    # Action 3: Click "Login - Register"
    print("Waiting for Login button...")
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[.//p[text()='Login - Register']]"))
    ).click()

    # Action 4: Input Username and Password
    print("Waiting for login form...")
    username_field = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "username"))
    )
    
    print("Entering credentials...")
    username_field.send_keys(CDS_USERNAME)
    driver.find_element(By.ID, "password").send_keys(CDS_PASSWORD)

    # Action 5: Hit Enter
    print("Logging in...")
    driver.find_element(By.ID, "password").send_keys(Keys.RETURN)

    # Action 6: Click "Your requests"
    print("Waiting for 'Your requests' link...")
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Your requests"))
    ).click()
    
    # --- ACTION 6.5: Get cookies *after* login is successful ---
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-requid]"))
    )
    driver_cookies = driver.get_cookies()
    print("Login successful, session cookies captured.")

    # Action 7: Find and Download Completed Files
    print("Checking for files to download...")
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    request_rows = driver.find_elements(By.CSS_SELECTOR, "div[data-requid]")
    print(f"Found {len(request_rows)} requests on page.")
    
    download_count = 0
    
    for row in request_rows:
        request_id = row.get_attribute("data-requid")
        
        c.execute("SELECT output_filename, status, download FROM requests WHERE request_id = ?", (request_id,))
        db_row = c.fetchone()
        
        if not db_row:
            continue
            
        output_filename, status, downloaded = db_row
        
        if status == 'completed' and not downloaded:
            print(f"Found pending download: {output_filename} (ID: {request_id})")
            
            try:
                # 1. Find the download link element
                link_element = row.find_element(By.LINK_TEXT, "Download")
                
                # 2. Get the URL from the 'href' attribute
                download_url = link_element.get_attribute('href')
                
                # 3. Define where to save the .zip file
                # We'll use the request_id to make a unique temp zip name
                temp_zip_path = os.path.join(DOWNLOAD_DIR, f"{request_id}.zip")

                # 4. Download the file using requests + session cookies
                download_file_with_session(download_url, temp_zip_path, driver_cookies)
                
                # 5. Unzip, rename, and clean up
                process_downloaded_file(temp_zip_path, output_filename)
                
                # 6. Update database
                c.execute("UPDATE requests SET download = 1, updated_at = ? WHERE request_id = ?", (datetime.now(), request_id))
                conn.commit()
                print(f"  > Successfully processed and marked '{output_filename}' as downloaded in DB.")
                download_count += 1
                
            except Exception as e:
                print(f"  > FAILED to download {output_filename}. Error: {e}")

        elif status == 'completed' and downloaded:
             print(f"Already downloaded: {output_filename}")
            
    conn.close()
    print(f"\nDownload run complete. {download_count} new files processed.")

    print("\nBrowser will close in 10 seconds.")
    time.sleep(10)

except Exception as e:
    print(f"\nAn error occurred: {e}")
    print("Saving screenshot as 'error.png'")
    driver.save_screenshot("error.png")

finally:
    # Clean up and close the browser
    print("Closing browser.")
    driver.quit()



