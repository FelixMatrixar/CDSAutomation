import cdsapi
import os
import time
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
DB_NAME = "requests.db"
MAX_ACTIVE_REQUESTS = 8
output_dir = "era5_data"
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

# --- Database Functions ---

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

def update_active_requests(client):
    """Checks the status of all 'queued' or 'running' requests."""
    print("--- Checking status of active requests ---")
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Get all active requests
    c.execute("SELECT request_id, output_filename FROM requests WHERE status IN ('queued', 'running')")
    active_requests = c.fetchall()
    
    if not active_requests:
        print("No active requests to update.")
        return 0

    updated_count = 0
    for request_id, filename in active_requests:
        print(f"Checking: {filename} ({request_id})...", end='')
        try:
            # Manually create a Result object to update it
            result = cdsapi.Result(client=client, reply={"request_id": request_id})
            result.update()
            
            new_status = result.reply['state']
            now_time = datetime.now()

            if new_status == 'completed':
                print(" COMPLETED")
                c.execute(
                    "UPDATE requests SET status=?, location=?, content_length=?, updated_at=? WHERE request_id=?",
                    (
                        new_status,
                        result.location,
                        result.content_length,
                        now_time,
                        request_id
                    )
                )
                updated_count += 1
            elif new_status == 'failed':
                print(" FAILED")
                c.execute(
                    "UPDATE requests SET status=?, updated_at=? WHERE request_id=?",
                    (new_status, now_time, request_id)
                )
            else:
                # Still 'queued' or 'running'
                print(f" {new_status.upper()}")
                c.execute("UPDATE requests SET updated_at=? WHERE request_id=?", (now_time, request_id))
            
        except Exception as e:
            print(f" ERROR checking status: {e}")
            # Could mark as failed, or just log
            c.execute("UPDATE requests SET status='failed', updated_at=? WHERE request_id=?", (datetime.now(), request_id))

    conn.commit()
    
    # Get new count of active requests
    c.execute("SELECT COUNT(*) FROM requests WHERE status IN ('queued', 'running')")
    current_active_count = c.fetchone()[0]
    
    conn.close()
    print(f"--- Status check complete. {current_active_count} requests are active. ---")
    return current_active_count

def get_all_filenames_in_db():
    """Gets a set of all filenames in the DB to prevent re-submission."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT output_filename FROM requests")
    filenames = set(row[0] for row in c.fetchall())
    conn.close()
    return filenames

# --- Main Execution ---

def main():
    os.makedirs(output_dir, exist_ok=True)
    setup_database()

    # Initialize a non-blocking client
    client = cdsapi.Client(wait_until_complete=False)

    # 1. Update status of existing requests
    current_active = update_active_requests(client)

    # 2. Check for available slots
    available_slots = MAX_ACTIVE_REQUESTS - current_active
    
    if available_slots <= 0:
        print(f"\nMax active request limit ({MAX_ACTIVE_REQUESTS}) reached.")
        print("No new requests will be submitted.")
        print("Run retrieve.py to download completed files and clear the queue.")
        return

    print(f"\nHave {available_slots} available slots. Starting new request submissions...")

    # 3. Get all filenames in the DB to avoid duplicates
    db_filenames = get_all_filenames_in_db()
    
    # 4. Connect to DB for submitting new requests
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    submitted_count = 0
    stop_submission = False

    # --- MODIFICATION START ---
    # Define the 3-month chunks to iterate over
    three_month_chunks = [
        {'label': 'Jan-Mar', 'months': ['01', '02', '03']},
        {'label': 'Apr-Jun', 'months': ['04', '05', '06']},
        {'label': 'Jul-Sep', 'months': ['07', '08', '09']},
        {'label': 'Oct-Dec', 'months': ['10', '11', '12']}
    ]

    for state_abbr in states_to_download:
        if stop_submission: break
        if state_abbr not in bounding_boxes:
            print(f"Warning: Bounding box for state '{state_abbr}' not found. Skipping.")
            continue
        
        state_area = bounding_boxes[state_abbr]
        
        for year in years_to_download:
            if stop_submission: break
            
            # Loop through the 3-month chunks
            for chunk in three_month_chunks:
                if stop_submission: break
                
                # Create a filename specific to the year and chunk
                target_filename = f"ERA5_hourly_multivariable_{state_abbr}_{year}_{chunk['label']}.nc"
                target_path = os.path.join(output_dir, target_filename)

                # --- Idempotency Checks ---
                if target_filename in db_filenames:
                    # print(f"Skipping {target_filename}: Already in database.")
                    continue
                
                if os.path.exists(target_path):
                    print(f"Skipping {target_filename}: File already exists on disk.")
                    continue
                # --- End Checks ---

                print(f"Submitting request for: {target_filename}")

                try:
                    result = client.retrieve(
                        'reanalysis-era5-single-levels',
                        {
                            'product_type': ['reanalysis'],
                            'variable': variables_to_download,
                            'year': [year],
                            'month': chunk['months'], # <-- Use the 3-month chunk
                            'day': [
                                '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
                                '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
                                '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'
                            ],
                            'time': [
                                '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                                '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                                '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                                '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
                            ],
                            'format': 'netcdf', 
                            'area': state_area,
                        }
                    )
                    
                    # Request submitted, add to DB
                    now_time = datetime.now()
                    request_id = result.reply['request_id']
                    status = result.reply['state']
                    
                    c.execute(
                        "INSERT INTO requests (request_id, state_abbr, year, output_filename, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (request_id, state_abbr, year, target_filename, status, now_time, now_time)
                    )
                    conn.commit()
                    db_filenames.add(target_filename) # Add to our set to prevent re-submission in this run
                    
                    print(f"  > Submitted. ID: {request_id}, Status: {status}")
                    submitted_count += 1

                except Exception as e:
                    print(f"ERROR: Request failed for {target_filename}.")
                    print(f"Details: {e}")
                
                finally:
                    if submitted_count >= available_slots:
                        print("\nReached max active request limit. Stopping submissions.")
                        stop_submission = True
                    
                    # A 15-second delay between each new request submission
                    if not stop_submission:
                        print("Waiting 15s before next request...")
                        time.sleep(15)

    conn.close()
    print("--- Request submission script finished. ---")


if __name__ == '__main__':
    main()