import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
import re
from datetime import datetime
import io
import os

def scrape_ampol_tgp():
    """
    Scrapes the Ampol Terminal Gate Price PDF, processes the data,
    and appends it to a CSV file.
    """
    # 1. Find the PDF URL from the main pricing page
    page_url = "https://www.ampol.com.au/business/pricing"
    print(f"Fetching page: {page_url}")
    try:
        response = requests.get(page_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        link = soup.find('a', string=re.compile("TERMINAL GATE PRICES", re.IGNORECASE))
        if not link or not link.get('href'):
            raise ValueError("Could not find the 'TERMINAL GATE PRICES' link on the page.")
        pdf_url = link['href']
        print(f"Found PDF URL: {pdf_url}")
    except requests.RequestException as e:
        print(f"Error fetching the pricing page: {e}")
        return

    # 2. Download the PDF into an in-memory buffer
    print("Downloading PDF...")
    try:
        pdf_response = requests.get(pdf_url, timeout=30)
        pdf_response.raise_for_status()
        pdf_file = io.BytesIO(pdf_response.content)
    except requests.RequestException as e:
        print(f"Error downloading the PDF file: {e}")
        return

    # 3. Extract data by parsing raw text (more robust for this PDF)
    print("Extracting data from PDF via text parsing...")
    with pdfplumber.open(pdf_file) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

        # 3a. Extract and parse dates
        current_date_str = re.search(r"Current Effective Date:\s*(.*)", text).group(1).strip()
        previous_date_str = re.search(r"Previous Effective Date:\s*(.*)", text).group(1).strip()
        current_date = datetime.strptime(current_date_str, "%A, %d %B %Y").date()
        previous_date = datetime.strptime(previous_date_str, "%A, %d %B %Y").date()
        print(f"Current Date: {current_date}, Previous Date: {previous_date}")

        # 3b. Manually parse the table from the text lines
        lines = text.split('\n')
        data_rows = []
        state_regex = re.compile(r'^(NSW|QLD|VIC|TAS|SA|NT|WA)\s')

        for line in lines:
            line = line.strip()
            if not state_regex.match(line):
                continue

            # Split by whitespace, which may split multi-word locations
            tokens = re.split(r'\s+', line)
            
            # The last 10 items are always the price data
            if len(tokens) >= 12: # State + at least one word for location + 10 prices
                prices = tokens[-10:]
                location = ' '.join(tokens[1:-10])
                state = tokens[0]
                data_rows.append([state, location] + prices)
            else:
                print(f"Warning: Skipping row that doesn't seem to have enough columns: '{line}'")

        if not data_rows:
            raise ValueError("Failed to parse any data rows from the PDF text.")

        # 3c. Create DataFrame with hardcoded headers
        headers = [
            'state', 'terminal',
            'E10_Previous', 'E10_Current',
            'ULP_Previous', 'ULP_Current',
            'PULP95_Previous', 'PULP95_Current',
            'PULP98_Previous', 'PULP98_Current',
            'DIESEL_Previous', 'DIESEL_Current'
        ]
        df = pd.DataFrame(data_rows, columns=headers)

    # 4. Process the DataFrame into a long format
    print("Processing data...")
    df['terminal'] = df['terminal'].str.replace(r'\*+$', '', regex=True)
    
    long_df = pd.melt(df, id_vars=['state', 'terminal'], var_name='fuel_day', value_name='tgp')
    
    long_df[['fuel', 'day']] = long_df['fuel_day'].str.split('_', expand=True)
    long_df['effective_date'] = long_df['day'].map({'Previous': previous_date.isoformat(), 'Current': current_date.isoformat()})
    
    long_df['tgp'] = pd.to_numeric(long_df['tgp'], errors='coerce')
    long_df.dropna(subset=['tgp'], inplace=True)
    
    final_df = long_df[['state', 'terminal', 'effective_date', 'fuel', 'tgp']].copy()
    final_df['date_downloaded'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # 5. Append to existing data file and save
    output_filename = "ampol_tgp_data.csv"
    if os.path.exists(output_filename):
        print(f"Appending data to {output_filename}")
        existing_df = pd.read_csv(output_filename)
        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
    else:
        print(f"Creating new data file: {output_filename}")
        combined_df = final_df

    # Remove duplicates, keeping the most recently downloaded record for any day/location/fuel combination
    combined_df.sort_values('date_downloaded', ascending=False, inplace=True)
    combined_df.drop_duplicates(subset=['state', 'terminal', 'effective_date', 'fuel'], keep='first', inplace=True)
    combined_df.sort_values(by=['effective_date', 'state', 'terminal', 'fuel'], inplace=True)
    
    combined_df.to_csv(output_filename, index=False)
    print(f"Successfully saved data to {output_filename}")

if __name__ == "__main__":
    scrape_ampol_tgp()
