import csv
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

def convert_coordinates_to_decimal(coord_str):
    """Convert coordinates from format like '39.1895° N, 106.9497° W' to decimal format."""
    try:
        # Extract latitude
        lat_match = re.search(r'(\d+\.\d+)°\s*([NS])', coord_str)
        if not lat_match:
            return None, None
        
        lat_value = float(lat_match.group(1))
        if lat_match.group(2) == 'S':
            lat_value = -lat_value
            
        # Extract longitude
        lng_match = re.search(r'(\d+\.\d+)°\s*([EW])', coord_str)
        if not lng_match:
            return None, None
            
        lng_value = float(lng_match.group(1))
        if lng_match.group(2) == 'W':
            lng_value = -lng_value
            
        return lat_value, lng_value
    except Exception as e:
        print(f"Error converting coordinates: {e}")
        return None, None

def extract_coordinates_from_text(text):
    """Try to extract coordinates from any text on the page using pattern matching."""
    # Look for patterns like "39.1895° N, 106.9497° W" or "39.1895°N, 106.9497°W"
    coord_pattern = re.compile(r'(\d+\.\d+)°\s*([NS])[\s,]*(\d+\.\d+)°\s*([EW])')
    match = coord_pattern.search(text)
    
    if match:
        lat = float(match.group(1))
        if match.group(2) == 'S':
            lat = -lat
            
        lng = float(match.group(3))
        if match.group(4) == 'W':
            lng = -lng
            
        return lat, lng
    
    return None, None

def extract_state_from_text(text):
    """Try to extract state information from any text on the page."""
    # List of US state names
    us_states = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", 
        "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", 
        "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", 
        "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
        "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", 
        "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
        "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", 
        "Wisconsin", "Wyoming"
    ]
    
    # Also check for state abbreviations
    state_abbrevs = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    
    # Look for state names
    for state in us_states:
        if state in text:
            return state
    
    # Look for abbreviations - need to be careful with this as these are common combinations of letters
    abbrev_pattern = r'\b([A-Z]{2})\b'
    matches = re.findall(abbrev_pattern, text)
    for match in matches:
        if match in state_abbrevs:
            return match
    
    return None

def get_resort_info(driver, resort_name):
    """Search for a resort and extract coordinates and state information."""
    search_query = f"{resort_name} ski resort coordinates"
    search_url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}"
    
    try:
        driver.get(search_url)
        # Add delay to avoid being blocked
        time.sleep(2)
        
        # Wait for the search results to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "search"))
        )
        
        # Get the page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # First attempt: Look for coordinates in the standard format
        coord_pattern = re.compile(r'\d+\.\d+°\s*[NS],\s*\d+\.\d+°\s*[EW]')
        coord_elements = soup.find_all(string=coord_pattern)
        
        lat, lng = None, None
        if coord_elements:
            coordinates_str = coord_elements[0]
            lat, lng = convert_coordinates_to_decimal(coordinates_str)
        
        # Second attempt: Try to extract coordinates from any text if not found in standard format
        if lat is None or lng is None:
            lat, lng = extract_coordinates_from_text(soup.text)
            
        # If coordinates still not found, return None
        if lat is None or lng is None:
            print(f"Could not find coordinates for {resort_name}")
            return None, None, None
        
        # Look for state information
        state = None
        state_patterns = [
            r'in (.+?), ([A-Z]{2})',  # Matches "in City, State"
            r'in ([A-Z][a-z]+)'        # Matches "in State"
        ]
        
        for pattern in state_patterns:
            state_match = re.search(pattern, soup.text)
            if state_match:
                if len(state_match.groups()) > 1:
                    state = state_match.group(2)  # Get the state code
                else:
                    state = state_match.group(1)  # Get the state name
                break
        
        # If standard pattern didn't work, try generic state extraction
        if not state:
            state = extract_state_from_text(soup.text)
        
        return lat, lng, state
            
    except Exception as e:
        print(f"Error processing {resort_name}: {e}")
        return None, None, None


unsuccessful_file = 'unsuccessful_resorts.csv'  # File for unsuccessful resorts
input_file = 'resorts_us.csv'
output_file = 'resorts_updated.csv'

# Set up Selenium with direct Chrome driver
chrome_options = Options()

try:
    # Initialize Chrome directly
    driver = webdriver.Chrome(options=chrome_options)
    
    # Read the input CSV
    resorts = []
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            resorts.append(row)
    
    # Process only the first 5 resorts for testing
    test_resorts = resorts
    
    # Track successes and failures
    successful = []
    failed = []
    
    # Process each resort
    results = []
    for resort in test_resorts:
        resort_name = resort['Resort']
        print(f"Processing: {resort_name}")
        
        lat, lng, state = get_resort_info(driver, resort_name)
        
        if lat is not "" and lng is not "":
            results.append({
                'Resort': resort_name,
                'Latitude': lat,
                'Longitude': lng,
                'State': state if state else resort['State']  # Use original state if new one not found
            })
            successful.append(resort_name)
            print(f"Success: {resort_name} - Lat: {lat}, Lng: {lng}, State: {state}")
        else:
            # Add the original data if scraping failed
            results.append({
                'Resort': resort_name
            })
            failed.append(resort_name)
            print(f"Failed: {resort_name}")
        
        # Add a delay between requests
        time.sleep(3)
    
    # Write the successfulresults to the output CSV
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['Resort', 'Latitude', 'Longitude', 'State']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    # Write the unsuccessful results to the unsuccessful CSV
    with open(unsuccessful_file, 'w', newline='') as f:
        fieldnames = ['Resort']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(failed)
    
    # Print summary
    print("\nScraping Summary:")
    print(f"Total resorts processed: {len(test_resorts)}")
    print(f"Successful: {len(successful)} - {successful}")
    print(f"Failed: {len(failed)} - {failed}")

except Exception as e:
    print(f"Exception Error: {e}")

finally:
    # Close the browser if it was initialized
    if 'driver' in locals():
        driver.quit()

 