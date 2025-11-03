# US Electricity Demand Forecast

This project is designed to automate the process of acquiring, managing, and uploading ERA5 weather data for the US to Kaggle. It consists of several Python scripts that work in conjunction to achieve this goal.

## Project Structure

```
.env
.gitignore
README.md
era5_data/
├── dataset-metadata.json
└── *.nc (ERA5 data files)
error.png
kaggle.json
manager.log
manager.py
peek.db.py
requests.db
retrieve.py
submit.py
update_status.py
upload.py
```

## Modules Overview

## Setup

1. Clone the repository.
2. Install dependencies.
3. Configure environment variables in `.env`.

## Usage

Detailed usage instructions will be added here.

## Modules

### `retrieve.py`

This script is responsible for automating the retrieval of ERA5 climate reanalysis data from the Copernicus Climate Data Store (CDS). It performs the following steps:

1.  **Environment Setup**: Loads CDS credentials (username and password) from a `.env` file.
2.  **Database Management**: Initializes an SQLite database (`requests.db`) to track the status of data requests, including whether a file has been downloaded.
3.  **Browser Automation (Selenium)**:
    *   Launches a Chrome browser instance.
    *   Navigates to the CDS website.
    *   Handles cookie consent.
    *   Logs in using the provided CDS credentials.
    *   Navigates to the "Your requests" section.
    *   Captures browser session cookies for authenticated downloads.
4.  **Data Download**:
    *   Iterates through the data requests listed on the "Your requests" page.
    *   For requests marked as 'completed' and not yet downloaded, it extracts the download URL.
    *   Uses the captured session cookies to download the `.zip` data file directly using the `requests` library.
5.  **File Processing**:
    *   Unzips the downloaded file.
    *   Renames the extracted `.nc` files to a logical format (e.g., distinguishing between `_instant.nc` and `_accum.nc` if multiple files are present in the zip).
    *   Moves the processed `.nc` files to the `era5_data/` directory.
    *   Deletes the temporary `.zip` file.
6.  **Status Update**: Updates the `requests.db` database to mark the downloaded files as processed.
7.  **Error Handling**: Includes a `try...finally` block to ensure the browser is always closed and saves a screenshot (`error.png`) if an error occurs during the process.

**Key Functions**:

*   `setup_database()`: Creates or ensures the existence of the `requests` table in `requests.db` to manage download states.
*   `process_downloaded_file(zip_path, target_nc_filename)`: Manages the unzipping, renaming, and cleanup of downloaded data files. It intelligently handles cases where a single `.zip` contains multiple `.nc` files (e.g., instant and accumulated variables).
*   `download_file_with_session(url, target_zip_path, driver_cookies)`: Facilitates authenticated downloads by passing browser session cookies to the `requests` library, allowing direct download of files that require login.

### `submit.py`

This script is designed to submit data retrieval requests to the Copernicus Climate Data Store (CDS) API for ERA5 climate reanalysis data. It manages the submission process, tracks request statuses, and ensures that the number of active requests does not exceed a defined limit.

**Key Features**:

1.  **Configuration**:
    *   Loads environment variables (though not directly used for CDS credentials in this script, it's good practice for other configurations).
    *   Defines `DB_NAME` (`requests.db`) for tracking requests.
    *   Sets `MAX_ACTIVE_REQUESTS` to limit concurrent submissions to the CDS API.
    *   Specifies `years_to_download` (2019-2024), `variables_to_download` (e.g., wind components, temperature, pressure, precipitation), and `bounding_boxes` for various US states.
    *   Divides the year into `three_month_chunks` for efficient data retrieval.
2.  **Database Management**:
    *   `setup_database()`: Ensures the `requests` table exists in `requests.db` to store details about each submitted request (ID, state, year, filename, status, timestamps).
    *   `update_active_requests(client)`: Periodically checks the status of 'queued' or 'running' requests with the CDS API. It updates their status in the database to 'completed' or 'failed' and retrieves download `location` and `content_length` for completed requests.
    *   `get_all_filenames_in_db()`: Fetches a list of all filenames already present in the database to prevent duplicate submissions.
3.  **Request Submission Logic (`main()` function)**:
    *   Creates the `era5_data` output directory if it doesn't exist.
    *   Initializes a non-blocking `cdsapi.Client`.
    *   Calls `update_active_requests()` to refresh the status of ongoing requests.
    *   Calculates `available_slots` for new submissions based on `MAX_ACTIVE_REQUESTS`.
    *   Iterates through defined states, years, and three-month chunks.
    *   **Idempotency Checks**: Before submitting a new request, it verifies if the target file is already in the database or exists on disk, skipping duplicates.
    *   Submits requests to the CDS API using `client.retrieve()`, specifying product type, variables, year, months (from the chunk), days, time (hourly), format (netcdf), and the bounding box for the state.
    *   Records the `request_id`, `state`, `year`, `output_filename`, and initial `status` in the `requests.db` upon successful submission.
    *   Implements a delay (`time.sleep(15)`) between submissions to avoid overwhelming the CDS API and respects the `MAX_ACTIVE_REQUESTS` limit.

This script is crucial for initiating the data acquisition pipeline by programmatically requesting the necessary climate data from the CDS.

### `update_status.py`

This script automates the process of updating the status of data requests in the local database by scraping information directly from the Copernicus Climate Data Store (CDS) website. It uses Selenium to interact with the web interface, log in, navigate to the "Your requests" page, and extract the current status, download links, and file sizes of previously submitted requests.

**Key Features**:

1.  **Environment Setup**: Loads CDS credentials (username and password) from a `.env` file.
2.  **Database Management**:
    *   `setup_database()`: Ensures the `requests` table exists in `requests.db`, which stores details about each request.
    *   Updates existing entries in `requests.db` with the latest status, download location, and content length scraped from the CDS website.
3.  **Browser Automation (Selenium)**:
    *   Launches a Chrome browser instance.
    *   Navigates to the CDS website and handles the cookie banner.
    *   Logs in using the provided CDS credentials.
    *   Clicks on "Your requests" to view the status of submitted data orders.
4.  **Data Scraping**:
    *   Waits for the request list to load on the "Your requests" page.
    *   Iterates through each request row, extracting the `request_id`, current `status` (e.g., 'Queued', 'In progress', 'Complete', 'Rejected'), download `location` (URL), and `content_length` (file size) if the request is complete.
    *   Includes robust error handling for scraping individual rows to prevent the entire process from failing due to a single malformed entry.
5.  **Status Mapping and Database Update**:
    *   Maps the scraped web statuses (e.g., 'Complete') to internal database statuses (e.g., 'completed').
    *   Updates the `requests` table in `requests.db` for existing `request_id` entries with the latest status, download URL, and file size. This script only updates existing requests, assuming they were initially submitted by `submit.py`.
6.  **Utility Function**:
    *   `parse_size_to_bytes(size_str)`: A helper function to convert human-readable file size strings (e.g., "9.57 MB") into bytes for consistent storage in the database.
7.  **Error Handling**: Uses a `try...finally` block to ensure the browser is always closed and captures a screenshot (`error.png`) if any unexpected error occurs during the automation process.

This script is essential for maintaining an up-to-date record of data request statuses, allowing `retrieve.py` to efficiently identify and download completed files.

### `upload.py`

This script automates the process of uploading the collected and processed ERA5 data to Kaggle as a dataset. It handles authentication, metadata management, and both initial dataset creation and subsequent version updates.

**Key Features**:

1.  **User Configuration**: Requires the user to set `KAGGLE_USERNAME`, `KAGGLE_SLUG`, and `DATASET_TITLE` variables at the top of the script to customize the Kaggle dataset.
2.  **Kaggle Authentication (`check_auth`)**:
    *   Verifies the presence of `kaggle.json` (Kaggle API credentials) either in the script's directory or the default `~/.kaggle/` location.
    *   Sets the `KAGGLE_CONFIG_DIR` environment variable if `kaggle.json` is found locally, ensuring the Kaggle CLI uses the correct credentials.
    *   Attempts to set file permissions for `kaggle.json` to `600` (read/write for user only), which is a requirement for the Kaggle API, with a note for Windows users where this might not be strictly necessary.
    *   Exits with an error if `kaggle.json` is not found in either location.
3.  **Dataset Metadata Management (`create_or_update_metadata_file`)**:
    *   Ensures a `dataset-metadata.json` file exists within the `era5_data` directory. If not, it initializes one using `kaggle datasets init`.
    *   Updates the `title` and `id` (slug) fields in `dataset-metadata.json` with the values provided in the user configuration, ensuring consistency with the desired Kaggle dataset.
    *   Includes error handling for corrupted metadata files, attempting to recreate them if necessary.
4.  **Kaggle Dataset Existence Check (`check_dataset_exists`)**:
    *   Uses `kaggle datasets status` to determine if a dataset with the specified `KAGGLE_USERNAME` and `KAGGLE_SLUG` already exists on Kaggle.
5.  **Dataset Upload Logic (`main`)**:
    *   **Initial Creation**: If the dataset does not exist on Kaggle, it uses `kaggle datasets create -p era5_data` to upload all files in the `era5_data` directory as a new dataset.
    *   **Version Update**: If the dataset already exists, it uses `kaggle datasets version -p era5_data -m "Automated data update: [timestamp]"` to create a new version. This command intelligently uploads only new or changed files, making updates efficient.
6.  **Command Execution (`run_command`)**: A helper function that executes shell commands (e.g., `kaggle` CLI commands), streams their output to the console, and handles errors, exiting the script if a command fails.
7.  **Error Handling**: Includes checks for missing user configuration and the `era5_data` directory, providing informative error messages and exiting the script if prerequisites are not met.

This script is the final step in the data pipeline, making the collected and processed ERA5 data available on Kaggle for further analysis and sharing.

### `peek.db.py`

This utility script provides a simple way to inspect the contents of the `requests.db` SQLite database, which stores the status and metadata of ERA5 data download requests. It offers a quick overview of the request statuses and displays the details of the most recent requests.

**Key Features**:

1.  **Database Connection**: Connects to the `requests.db` SQLite database.
2.  **Status Summary**:
    *   Queries the `requests` table to count the number of requests for each unique status (e.g., 'pending', 'in_progress', 'completed', 'failed').
    *   Prints a formatted summary, showing the count for each status.
3.  **Most Recent Requests**:
    *   Retrieves the 10 most recently created requests from the `requests` table, ordered by their `created_at` timestamp.
    *   Displays all columns for each of these recent requests, providing a detailed snapshot of their metadata.
    *   Dynamically fetches column names from the database schema to ensure accurate labeling of the output.
4.  **Error Handling**: Includes `try...except` blocks to catch `sqlite3.Error` for database-related issues and general `Exception` for other unexpected errors, providing informative messages to the user, including a suggestion to run `submit.py` if the `requests` table is not found.
5.  **Resource Management**: Ensures the database connection is properly closed in a `finally` block, regardless of whether an error occurred.

This script is useful for debugging and monitoring the state of data requests throughout the data acquisition pipeline, allowing users to quickly check the progress and identify any stalled or failed downloads.

### `manager.py`

Given its name and the presence of `retrieve.py`, `submit.py`, `update_status.py`, and `upload.py`, it is highly probable that `manager.py` serves as the central orchestration script for the entire data pipeline. It is likely responsible for coordinating the execution of these individual components in the correct sequence to automate the process of submitting data requests, updating their statuses, retrieving completed data, and finally uploading it to Kaggle. While the full contents of this file could not be analyzed due to its size, its role is inferred to be the primary entry point for running the complete ERA5 data acquisition and management workflow.