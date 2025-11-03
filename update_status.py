import os
import time
import re
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

# 1. Load credentials from .env file
load_dotenv()
CDS_USERNAME = os.getenv("CDS_USERNAME")
CDS_PASSWORD = os.getenv("CDS_PASSWORD")
DB_NAME = "requests.db"

if not CDS_USERNAME or not CDS_PASSWORD:
    print("Error: CDS_USERNAME or CDS_PASSWORD not found in .env file.")
    print("Please create a .env file with your credentials.")
    exit()

def setup_database():
    """Creates the database table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS requests (
        request_id TEXT PRIMARY KEY,
        state_abbr TEXT NOT NULL,
        year TEXT NOT NULL,
        output_filename TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL,
        location TEXT,
        content_length INTEGER,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    """)
    conn.commit()
    conn.close()

def parse_size_to_bytes(size_str):
    """Converts a string like '9.57 MB' to bytes."""
    if not size_str:
        return None
    
    size_str = size_str.strip()
    # Use regex to find number and unit
    match = re.match(r'([\d.]+)\s*(\w+)', size_str)
    if not match:
        return None
        
    try:
        value = float(match.group(1))
        unit = match.group(2).upper()
        
        if unit == 'KB':
            return int(value * 1024)
        elif unit == 'MB':
            return int(value * 1024 * 1024)
        elif unit == 'GB':
            return int(value * 1024 * 1024 * 1024)
        elif unit == 'TB':
            return int(value * 1024 * 1024 * 1024 * 1024)
        elif unit == 'B':
            return int(value)
        else:
            return None
    except Exception:
        return None

# 2. Set up the Chrome driver automatically
print("Setting up Chrome driver...")
service = ChromeService(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# Use a try...finally block to make sure the browser always closes
try:
    # Action 1: Go to the website
    print("Opening cds.climate.copernicus.eu...")
    driver.get("https://cds.climate.copernicus.eu/")

    # Action 2: Handle Cookie Banner
    try:
        print("Waiting for cookie banner...")
        # Wait a shorter time (e.g., 5 seconds) for the cookie banner
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Deny all']"))
        ).click()
        print("Clicked 'Deny all' on cookie banner.")
    except Exception as e:
        # If the banner doesn't appear or times out, just log it and continue
        print("Cookie banner not found or 'Deny all' not clickable. Continuing...")

    # Action 3: Click "Login - Register"
    # We wait up to 10 seconds for the element to be clickable
    print("Waiting for Login button...")
    # --- MODIFIED XPATH ---
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[.//p[text()='Login - Register']]"))
    ).click()

    # Action 4: Input Username and Password
    # Wait for the username field to be visible (page has transitioned)
    print("Waiting for login form...")
    username_field = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "username"))
    )
    
    print("Entering credentials...")
    username_field.send_keys(CDS_USERNAME)
    
    # Find the password field (it should be loaded by now)
    driver.find_element(By.ID, "password").send_keys(CDS_PASSWORD)

    # Action 5: Hit Enter
    print("Logging in...")
    driver.find_element(By.ID, "password").send_keys(Keys.RETURN)

    # Action 6: Click "Your requests"
    # Wait for the login to complete and the "Your requests" link to be clickable
    print("Waiting for 'Your requests' link...")
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Your requests"))
    ).click()

    # Action 7: Scrape Data
    print("Waiting for request list to load...")
    
    # Wait for the first request row to be visible
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-requid]"))
    )

    print("Scraping request IDs and statuses...")
    
    # Find all the 'div' elements that act as a row container
    request_rows = driver.find_elements(By.CSS_SELECTOR, "div[data-requid]")
    
    scraped_data = []
    
    for row in request_rows:
        request_id = None
        status_text = None
        location = None
        content_length_str = None
        
        try:
            # Get the request ID from the 'data-requid' attribute
            request_id = row.get_attribute("data-requid")
            
            # Find the status span.
            status_element = row.find_element(By.CSS_SELECTOR, 'span[class^="sc-d2474931-"]')
            status_text = status_element.text
            
            # If the request is complete, get download link and file size
            if status_text == 'Complete':
                try:
                    link_element = row.find_element(By.LINK_TEXT, "Download")
                    location = link_element.get_attribute('href')
                except NoSuchElementException:
                    print(f"Warning: 'Complete' request {request_id} has no Download link.")
                
                try:
                    size_element = row.find_element(By.CSS_SELECTOR, 'p[class^="sc-d5be8ee9-8"]')
                    content_length_str = size_element.text
                except NoSuchElementException:
                    print(f"Warning: 'Complete' request {request_id} has no file size.")
            
            if request_id and status_text:
                scraped_data.append({
                    "id": request_id, 
                    "status": status_text,
                    "location": location,
                    "content_length_str": content_length_str
                })
        except Exception as e:
            # This prevents one bad row from crashing the whole scrape
            print(f"Could not parse a row. Error: {e}")

    # Success: We are on the requests page
    print(f"\nSuccessfully navigated to 'Your requests' page.")
    print(f"Current URL: {driver.current_url}")

    # Print out the data we just scraped
    print("\n--- Scraped Data ---")
    if scraped_data:
        print(f"Found {len(scraped_data)} requests.")
        for item in scraped_data:
            print(f"ID: {item['id']}, Status: {item['status']}, Size: {item['content_length_str'] or 'N/A'}")
    else:
        print("No requests found on the page.")
        
    # Action 8: Update Database
    if scraped_data:
        print("\n--- Updating local database ---")
        setup_database()
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Map web statuses to our DB statuses
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
                print(f"Skipping unknown status: {item['status']}")
                continue
                
            request_id = item['id']
            location = item['location']
            content_length = parse_size_to_bytes(item['content_length_str'])
            now_time = datetime.now()
            
            try:
                # We only update rows that already exist (from submit.py)
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
                print(f"Error updating DB for {request_id}: {e}")
                
        conn.commit()
        conn.close()
        print(f"Database update complete. {updated_count} rows updated.")
        

    print("\nBrowser will close in 10 seconds.")
    time.sleep(10)

except Exception as e:
    print(f"\nAn error occurred: {e}")
    print("Saving screenshot as 'error.png'")
    driver.save_screenshot("error.png")

finally:
    # 7. Clean up and close the browser
    print("Closing browser.")
    driver.quit()

