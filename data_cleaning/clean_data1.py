import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def add_states_to_us_resorts(input_csv, output_csv, selected_columns) -> None:
    '''
    Reads input CSV, filters for rows where Country == 'United States',
    reverse geocodes (Latitude, Longitude) to find the State, and saves
    only the user-specified columns (plus 'State') to output_csv.

    :param input_csv:       Path to the original CSV file.
    :param output_csv:      Path where the resulting CSV will be saved.
    :param selected_columns: List of columns to include in final CSV 
                             (add 'State' to this list if you want it).
    '''

    # Load the dataset (use an alternate encoding if needed)
    df = pd.read_csv(input_csv, encoding="latin1", on_bad_lines="skip")

    # Filter to only U.S. rows
    df_us = df[df["Country"] == "United States"].copy()
    if df_us.empty:
        print("No rows found for 'United States'. Exiting.")
        return

    # Reverse geocode setup (Nominatim)
    geolocator = Nominatim(user_agent="ski_resort_state_finder")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    # Populate the 'State' column via reverse geocoding
    states = []
    for i, row in df_us.iterrows():
        lat, lon = row["Latitude"], row["Longitude"]
        try:
            location = reverse((lat, lon))
            if location and location.raw.get("address"):
                address = location.raw["address"]
                state_name = address.get("state")
                states.append(state_name if state_name else None)
            else:
                states.append(None)
        except Exception as e:
            print(f"Error geocoding resort ID {row.get('ID', 'Unknown')}: {e}")
            states.append(None)

    df_us["State"] = states

    # Ensure 'State' is in the final selection
    final_cols = selected_columns.copy()
    if "State" not in final_cols:
        final_cols.append("State")

    # Make sure each selected column actually exists in df_us
    final_cols = [col for col in final_cols if col in df_us.columns]

    # Select only the desired columns
    return df_us[final_cols]

input_file = "resorts.csv"
output_file = "resorts_us.csv"

# The user specifies which columns they want in the final CSV
columns_to_keep = ["ID", "Resort", "Latitude", "Longitude", "State"]

df = add_states_to_us_resorts(input_file, output_file, columns_to_keep)

df.to_csv(output_file, index=False)
print(f"Updated U.S. resorts with limited columns saved to '{output_file}'.")