import os
import json
import time
import multiprocessing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import (
    StaleElementReferenceException, NoSuchElementException,
    TimeoutException, ElementClickInterceptedException, JavascriptException
)

# Function to print and log current time and message
def print_and_log_time(message, log_file):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"{message}: {current_time}"
    print(log_message)
    with open(log_file, 'a', encoding='utf-8') as file:
        file.write(log_message + '\n')

# Function to update the terminal output
def update_terminal_output(progress_tracker, completed_villages, total_villages):
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"Completed villages: {completed_villages}/{total_villages}\n")
    for key, value in progress_tracker.items():
        print(f"Instance {key}: {json.dumps(value, ensure_ascii=False)}")

# Function to select an option by text with retries
def select_option_by_text_with_retry(driver, select_element_id, option_text, log_file, instance_id, progress_tracker, retries=3):
    for attempt in range(retries):
        try:
            select_element = Select(driver.find_element(By.ID, select_element_id))
            for option in select_element.options:
                if option.text == option_text:
                    option.click()
                    return True
            return False
        except (StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException) as e:
            message = f"Error selecting option '{option_text}' on attempt {attempt + 1}/{retries}: {e}"
            print_and_log_time(message, log_file)
            progress_tracker[instance_id]['message'] = message
            update_terminal_output(progress_tracker, len([v for v in processed_villages if os.path.isfile(v)]), total_villages)
            time.sleep(1)
            if attempt < retries - 1:
                # Re-locate the element without refreshing the page
                select_element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, select_element_id))
                )
            else:
                raise
    return False

# Function to inject JavaScript for MutationObserver
def inject_mutation_observer(driver):
    script = """
        if (window.plotInfoObserver) {
            window.plotInfoObserver.disconnect();
        }
        window.plotInfoUpdated = false;
        var targetNode = document.getElementById('plotinfo');
        var observerOptions = {
            childList: true,
            subtree: true
        };
        function callback(mutationsList, observer) {
            for (var mutation of mutationsList) {
                if (mutation.type === 'childList') {
                    window.plotInfoUpdated = true;
                }
            }
        }
        window.plotInfoObserver = new MutationObserver(callback);
        window.plotInfoObserver.observe(targetNode, observerOptions);
    """
    driver.execute_script(script)

# Function to wait for plot info update using MutationObserver
def wait_for_plot_info_update(driver, log_file, instance_id, progress_tracker, previous_plot_info, retries=1):
    for attempt in range(retries):
        try:
            inject_mutation_observer(driver)
            WebDriverWait(driver, 20).until(
                lambda d: d.execute_script("return window.plotInfoUpdated")
            )
            plot_info = driver.find_element(By.ID, 'plotinfo').text
            return plot_info
        except TimeoutException:
            message = f"Timeout waiting for plot info update on attempt {attempt + 1}/{retries}"
            print_and_log_time(message, log_file)
            progress_tracker[instance_id]['message'] = message
            update_terminal_output(progress_tracker, len([v for v in processed_villages if os.path.isfile(v)]), total_villages)
            # Backup logic: compare with previous plot info
            try:
                plot_info = driver.find_element(By.ID, 'plotinfo').text
                if plot_info != previous_plot_info:
                    return plot_info
            except NoSuchElementException:
                pass
            if attempt < retries - 1:
                time.sleep(1)
            else:
                raise

# Function to check if the yellow map is loaded
def is_yellow_map_loaded(driver):
    try:
        # Check for the presence of the yellow map
        map_element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'ol-viewport'))
        )
        return True
    except TimeoutException:
        return False

def initialize_browser(webdriver_path, firefox_options, log_file, retries=3):
    for attempt in range(retries):
        try:
            service = Service(webdriver_path)
            driver = webdriver.Firefox(service=service, options=firefox_options)
            return driver
        except Exception as e:
            message = f"Error initializing browser on attempt {attempt + 1}/{retries}: {e}"
            print_and_log_time(message, log_file)
            time.sleep(1)
            if attempt == retries - 1:
                raise

def get_village_name_to_scrape(instance_id, villages, processed_villages, lock):
    with lock:
        for village_index, village_name in villages:
            if village_name not in processed_villages:
                processed_villages.append(village_name)
                return village_index, village_name
    return None, None

def scrape_village(instance_id, district_index, taluka_index, progress_tracker, lock, villages, processed_villages, total_villages):
    # Setup Firefox options
    firefox_options = Options()
    firefox_options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"  # Update this path if necessary
    firefox_options.add_argument('--headless')

    # Path to your Firefox WebDriver (geckodriver)
    webdriver_path = "./geckodriver.exe"

    while True:
        village_index, village_name = get_village_name_to_scrape(instance_id, villages, processed_villages, lock)
        if village_index is None:
            break

        log_path = os.path.join('logs', f'district_{district_index}', f'taluka_{taluka_index}')
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        log_file = os.path.join(log_path, f'village_{village_index}.txt')
        
        driver = initialize_browser(webdriver_path, firefox_options, log_file)
        village_start_time = datetime.now()
        try:
            # Open the webpage
            driver.get("https://mahabhunakasha.mahabhumi.gov.in/27/index.html")
            print_and_log_time("Opened the webpage", log_file)

            # Allow the page to load
            WebDriverWait(driver, 3600).until(
                EC.presence_of_element_located((By.ID, 'level_0'))
            )
            print_and_log_time("Page loaded", log_file)

            # Select the first option in the state dropdown
            state_select = Select(driver.find_element(By.ID, 'level_0'))
            state_select.select_by_index(0)

            # Wait for the category dropdown to be populated
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, 'level_1'))
            )
            time.sleep(5)  # Add a small delay to allow the dropdown to populate
            category_select = Select(driver.find_element(By.ID, 'level_1'))
            WebDriverWait(driver, 20).until(
                lambda d: len(category_select.options) > 1
            )
            category_select.select_by_index(0)

            # Wait for the district dropdown to be populated and select the specific district
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, 'level_2'))
            )
            district_select = Select(driver.find_element(By.ID, 'level_2'))
            WebDriverWait(driver, 20).until(
                lambda d: len(district_select.options) > 1
            )
            district_select.select_by_index(district_index)
            district_name = district_select.options[district_index].text

            # Create a folder for the district if it doesn't exist
            district_path = os.path.join(district_name)
            if not os.path.exists(district_path):
                os.makedirs(district_path)
            print_and_log_time(f"District folder '{district_name}' created or already exists", log_file)

            # Select the specific taluka
            taluka_select = Select(driver.find_element(By.ID, 'level_3'))
            WebDriverWait(driver, 20).until(
                lambda d: len(taluka_select.options) > 1
            )
            taluka_select.select_by_index(taluka_index)
            taluka_name = taluka_select.options[taluka_index].text

            # Create a folder for the taluka if it doesn't exist
            taluka_path = os.path.join(district_path, taluka_name)
            if not os.path.exists(taluka_path):
                os.makedirs(taluka_path)
            print_and_log_time(f"Taluka folder '{taluka_name}' created or already exists", log_file)

            # Wait for the village dropdown to be populated
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, 'level_4'))
            )
            village_select = Select(driver.find_element(By.ID, 'level_4'))
            WebDriverWait(driver, 20).until(
                lambda d: len(village_select.options) > 1
            )
            if not select_option_by_text_with_retry(driver, 'level_4', village_name, log_file, instance_id, progress_tracker):
                print_and_log_time(f"Village '{village_name}' not found", log_file)
                continue

            # Check if the yellow map is loaded
            if not is_yellow_map_loaded(driver):
                print_and_log_time(f"Yellow map not loaded for village '{village_name}'. Skipping...", log_file)
                continue

            # Wait for the "Select Plot No:" dropdown to be visible and populated
            plot_dropdown = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, 'surveyNumber'))
            )

            # Wait until the plot dropdown has options to select
            WebDriverWait(driver, 20).until(lambda d: len(Select(d.find_element(By.ID, 'surveyNumber')).options) > 1)
            plot_select = Select(driver.find_element(By.ID, 'surveyNumber'))

            # Initialize a list to hold plot data
            plot_data = []

            previous_plot_info = ""
            # Iterate over each plot option by index
            for plot_index in range(1, len(plot_select.options)):
                plot_option_text = plot_select.options[plot_index].text
                progress_tracker[instance_id] = {
                    "district": district_name,
                    "taluka": taluka_name,
                    "village": village_name,
                    "plot_index": plot_index,
                    "plot_info": plot_option_text
                }
                update_terminal_output(progress_tracker, len([v for v in processed_villages if os.path.isfile(os.path.join(taluka_path, f"{v}.xlsx"))]), total_villages)
                if not select_option_by_text_with_retry(driver, 'surveyNumber', plot_option_text, log_file, instance_id, progress_tracker):  # Select the plot by text
                    print_and_log_time(f"Plot option '{plot_option_text}' not found for village '{village_name}'", log_file)
                    break

                # Wait for the plot information to be updated
                try:
                    plot_info_text = wait_for_plot_info_update(driver, log_file, instance_id, progress_tracker, previous_plot_info)
                except TimeoutException:
                    print_and_log_time(f"Timeout waiting for plot info for village '{village_name}', option: {plot_option_text}", log_file)
                    continue

                previous_plot_info = plot_info_text

                # Split the plot information into lines
                plot_info_lines = plot_info_text.split('\n')

                # Group lines into sets of information for each survey number
                current_plot_info = {}
                for line in plot_info_lines:
                    if line.startswith('Survey No.'):
                        if current_plot_info:
                            plot_data.append(current_plot_info)
                        current_plot_info = {'Survey No.': line.split(': ')[1]}
                    elif line.startswith('Total Area'):
                        current_plot_info['Total Area'] = line.split(': ')[1]
                    elif line.startswith('Pot kharaba'):
                        current_plot_info['Pot kharaba'] = line.split(': ')[1]
                    elif line.startswith('Owner Name'):
                        current_plot_info['Owner Name'] = line.split(': ')[1]
                    elif line.startswith('Khata No.'):
                        current_plot_info['Khata No.'] = line.split(': ')[1]

                # Log the current plot info
                if current_plot_info:
                    print_and_log_time(f"Plot info: {current_plot_info}", log_file)
                    plot_data.append(current_plot_info)

            # Create a DataFrame for the village
            village_df = pd.DataFrame(plot_data)

            # Check for duplicates and remove them
            village_df.drop_duplicates(inplace=True)

            # Save the current state of the Excel file
            village_file_path = os.path.join(taluka_path, f'{village_name}.xlsx')
            with pd.ExcelWriter(village_file_path) as writer:
                village_df.to_excel(writer, sheet_name=village_name, index=False)

            # Print time taken for the village
            print_and_log_time(f"Village '{village_name}' processed", log_file)
            print_and_log_time(f"Time taken for village '{village_name}': {datetime.now() - village_start_time}", log_file)

        except Exception as e:
            print_and_log_time(f"Error encountered: {e}", log_file)

        finally:
            # Close the browser
            driver.quit()

        # Remove the instance from the progress tracker
        progress_tracker.pop(instance_id)
        update_terminal_output(progress_tracker, len([v for v in processed_villages if os.path.isfile(os.path.join(taluka_path, f"{v}.xlsx"))]), total_villages)

        # Print overall time taken
        print_and_log_time(f"Script completed for village '{village_name}'", log_file)

def get_villages(district_index, taluka_index):
    # Setup Firefox options
    firefox_options = Options()
    firefox_options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"  # Update this path if necessary
    firefox_options.add_argument('--headless')

    # Path to your Firefox WebDriver (geckodriver)
    webdriver_path = "./geckodriver.exe"

    driver = initialize_browser(webdriver_path, firefox_options, 'logs/log_village_discovery.txt')
    try:
        # Open the webpage
        driver.get("https://mahabhunakasha.mahabhumi.gov.in/27/index.html")
        WebDriverWait(driver, 3600).until(
            EC.presence_of_element_located((By.ID, 'level_0'))
        )

        # Select the first option in the state dropdown
        state_select = Select(driver.find_element(By.ID, 'level_0'))
        state_select.select_by_index(0)

        # Wait for the category dropdown to be populated
        WebDriverWait(driver, 360).until(
            EC.presence_of_element_located((By.ID, 'level_1'))
        )
        time.sleep(5)  # Add a small delay to allow the dropdown to populate
        category_select = Select(driver.find_element(By.ID, 'level_1'))
        WebDriverWait(driver, 20).until(
            lambda d: len(category_select.options) > 1
        )
        category_select.select_by_index(0)

        # Wait for the district dropdown to be populated and select the specific district
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'level_2'))
        )
        district_select = Select(driver.find_element(By.ID, 'level_2'))
        WebDriverWait(driver, 20).until(
            lambda d: len(district_select.options) > 1
        )
        district_select.select_by_index(district_index)
        district_name = district_select.options[district_index].text

        # Select the specific taluka
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'level_3'))
        )
        taluka_select = Select(driver.find_element(By.ID, 'level_3'))
        WebDriverWait(driver, 20).until(
            lambda d: len(taluka_select.options) > 1
        )
        taluka_select.select_by_index(taluka_index)
        taluka_name = taluka_select.options[taluka_index].text

        # Wait for the village dropdown to be populated
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'level_4'))
        )
        village_select = Select(driver.find_element(By.ID, 'level_4'))
        WebDriverWait(driver, 20).until(
            lambda d: len(village_select.options) > 1
        )
        village_options = [(index, option.text) for index, option in enumerate(village_select.options[1:], start=1)]
        return village_options, district_name, taluka_name

    finally:
        driver.quit()

def get_already_processed_villages(district_name, taluka_name):
    taluka_path = os.path.join(district_name, taluka_name)
    if not os.path.exists(taluka_path):
        return []
    processed_villages = [file.replace('.xlsx', '') for file in os.listdir(taluka_path) if file.endswith('.xlsx')]
    return processed_villages

if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()

    manager = multiprocessing.Manager()
    progress_tracker = manager.dict()
    lock = manager.Lock()

    district_index = 1  # Adjust this to the desired district index
    taluka_index = 7  # Adjust this to the desired taluka index

    villages, district_name, taluka_name = get_villages(district_index, taluka_index)
    total_villages = len(villages)

    processed_villages = manager.list(get_already_processed_villages(district_name, taluka_name))

    num_instances = 6  # Number of instances to run in parallel

    with multiprocessing.Pool(processes=num_instances) as pool:
        pool.starmap(scrape_village, [
            (instance_id, district_index, taluka_index, progress_tracker, lock, villages, processed_villages, total_villages)
            for instance_id in range(num_instances)
        ])
