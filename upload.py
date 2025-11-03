import os
import subprocess
import json
import sys
import stat
from datetime import datetime

# --- 1. USER CONFIGURATION ---
# !!! YOU MUST CHANGE THESE 3 VARIABLES !!!
KAGGLE_USERNAME = "pratamasidhi"  # Your Kaggle username (lowercase)
KAGGLE_SLUG = "era5-us-hourly-weather"    # The URL-friendly name for your dataset
DATASET_TITLE = "ERA5 US Hourly Weather Data" # The human-friendly title
# -----------------------------

# --- Script Configuration ---
DATASET_DIR = "era5_data" # The folder with your .nc files
METADATA_FILE = os.path.join(DATASET_DIR, "dataset-metadata.json")


def run_command(command, fail_on_error=True):
    """Helper function to run shell commands."""
    print(f"\n--- Running: {' '.join(command)} ---")
    try:
        # We stream the output instead of capturing it
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                print(line, end='')
        
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, command)
            
        print("--- Command successful ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"--- Command failed with return code {e.returncode} ---")
        if fail_on_error:
            sys.exit(1)
        return False
    except Exception as e:
        print(f"--- An unexpected error occurred: {e} ---")
        if fail_on_error:
            sys.exit(1)
        return False

def check_auth():
    """
    Checks if kaggle.json is present in the local repository or default path.
    If found locally, sets the KAGGLE_CONFIG_DIR to use it.
    Also ensures file permissions are set correctly.
    """
    
    # --- 1. Define local path ---
    # Assumes kaggle.json is in the same directory as this script.
    try:
        # This works when running as a script
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # This works in interactive environments (like Jupyter)
        script_dir = os.getcwd()
        
    local_kaggle_json = os.path.join(script_dir, "kaggle.json")
    
    # --- 2. Check local path first ---
    if os.path.exists(local_kaggle_json):
        print(f"Found 'kaggle.json' in repository: {local_kaggle_json}")
        
        # --- 3. Set environment variable to point Kaggle CLI to this dir ---
        os.environ['KAGGLE_CONFIG_DIR'] = script_dir
        
        # --- 4. Set file permissions (CRITICAL for Kaggle API) ---
        # The API requires permissions to be 600 (read/write for user only)
        try:
            print("Setting file permissions to 600 (read/write for user)...")
            os.chmod(local_kaggle_json, stat.S_IRUSR | stat.S_IWUSR)
        except OSError as e:
            # This can happen on Windows, but the API is more lenient there
            print(f"Note: Could not set file permissions for kaggle.json (this is common on Windows).")
        except Exception as e:
            print(f"Warning: Could not set file permissions for kaggle.json.")
            print(f"Error: {e}")
            
        print("Kaggle API configured to use local 'kaggle.json'.")
        return

    # --- 5. If not found locally, check default path ---
    default_kaggle_json = os.path.expanduser("~/.kaggle/kaggle.json")
    if os.path.exists(default_kaggle_json):
        print(f"Found 'kaggle.json' in default location: {default_kaggle_json}")
        # No need to set env var, this is the default
        print("Kaggle API will use default 'kaggle.json'.")
        return

    # --- 6. If not found anywhere ---
    print("="*60)
    print("ERROR: kaggle.json not found.")
    print(f"Please place your 'kaggle.json' file in one of these locations:")
    print(f"  1. In the script directory: {script_dir}")
    print(f"  2. In the default directory: {default_kaggle_json}")
    print("="*60)
    sys.exit(1)

def check_dataset_exists():
    """Uses 'kaggle datasets status' to see if the dataset exists on Kaggle."""
    print(f"Checking if dataset '{KAGGLE_USERNAME}/{KAGGLE_SLUG}' exists on Kaggle...")
    
    # We don't want to exit if this fails, so we capture the return code
    result = subprocess.run(
        ["kaggle", "datasets", "status", f"{KAGGLE_USERNAME}/{KAGGLE_SLUG}"], 
        text=True, 
        capture_output=True
    )
    
    if result.returncode == 0:
        print(f"Result: Dataset already exists.")
        return True
    else:
        print(f"Result: Dataset does not exist.")
        return False

def create_or_update_metadata_file():
    """
    Creates the dataset-metadata.json file if it doesn't exist,
    then updates it with the correct title and id (slug).
    """
    if not os.path.exists(METADATA_FILE):
         print(f"Metadata file not found. Creating a new one...")
         run_command(["kaggle", "datasets", "init", "-p", DATASET_DIR])
    else:
        print(f"Metadata file found: {METADATA_FILE}")

    # Now, read and update the JSON file
    print("Updating metadata with correct title and ID...")
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: {METADATA_FILE} is corrupted. Deleting and recreating.")
        os.remove(METADATA_FILE)
        return create_or_update_metadata_file() # Recurse
        
    metadata['title'] = DATASET_TITLE
    metadata['id'] = f"{KAGGLE_USERNAME}/{KAGGLE_SLUG}"
    
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)
        
    print(f"  > Title set to: {DATASET_TITLE}")
    print(f"  > ID    set to: {metadata['id']}")


def main():
    print("--- Starting Kaggle Upload Script ---")
    
    # 0. Check for user-filled info
    if KAGGLE_USERNAME == "your-kaggle-username":
         print("="*60)
         print("ERROR: Please edit this script and fill in the")
         print("KAGGLE_USERNAME, KAGGLE_SLUG, and DATASET_TITLE variables.")
         print("="*60)
         sys.exit(1)
         
    # 1. Check for kaggle.json
    check_auth()

    # 2. Check for data directory
    if not os.path.exists(DATASET_DIR):
        print(f"Error: Data directory '{DATASET_DIR}' not found.")
        print("Please run your downloader scripts first, or check the DATASET_DIR variable.")
        sys.exit(1)
    
    # 3. Create or update the local metadata file
    # We do this every time to ensure it's correct
    create_or_update_metadata_file()

    # 4. Check if the dataset already exists on Kaggle
    if check_dataset_exists():
        # --- UPDATE (Idempotent) ---
        print("\nDataset exists. Creating a new version...")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"Automated data update: {timestamp}"
        
        # This command only uploads new or changed files
        run_command(["kaggle", "datasets", "version", "-p", DATASET_DIR, "-m", message])
        print("\n--- Dataset update complete! ---")
    else:
        # --- CREATE (First time) ---
        print("\nDataset does not exist. Creating a new dataset...")
        
        # This command uploads all files
        run_command(["kaggle", "datasets", "create", "-p", DATASET_DIR])
        print("\n--- New dataset creation complete! ---")

if __name__ == "__main__":
    main()

