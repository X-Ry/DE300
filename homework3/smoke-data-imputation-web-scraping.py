import requests
from bs4 import BeautifulSoup
import json
import pandas as pd

# Web Scraping does not make sense to perform within a Spark script, so I am performing the steps to get the 
# data that is needed for data imputation here first, and uploading it to the S3 bucket for usage.

# Function to extract percentage from text
def extract_percentage(text):
    start = text.find('(') + 1
    end = text.find('%)')
    return float(text[start:end])/100

# Scrape CDC data
url_cdc = 'https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm'
response_cdc = requests.get(url_cdc)
soup_cdc = BeautifulSoup(response_cdc.content, 'html.parser')
ul_tags_cdc = soup_cdc.find_all('ul', {'class': 'block-list'})
gender_smoke_data_cdc = ul_tags_cdc[1].find_all('li')
age_smoke_data_cdc = ul_tags_cdc[2].find_all('li')

# Process CDC data
gender_smoke_cdc = {
    'gender': ['male', 'female'],
    'percentage': [extract_percentage(gender_smoke_data_cdc[0].text), extract_percentage(gender_smoke_data_cdc[1].text)]
}
df_gender_smoke_cdc = pd.DataFrame(gender_smoke_cdc)
df_gender_smoke_cdc.to_csv('gender_smoke_cdc.csv', index=False)

age_smoke_cdc = []
age_brackets_cdc = {
    '18–24 years': [18,24],
    '25–44 years': [25,44],
    '45–64 years': [45,64],
    '65 years and older': [65, 1000]
}
for li in age_smoke_data_cdc:
    for bracket_key, bracket_range in age_brackets_cdc.items():
        if bracket_key in li.text:
            age_smoke_cdc.append({'age_range': bracket_key, 'percentage': extract_percentage(li.text)})
df_age_smoke_cdc = pd.DataFrame(age_smoke_cdc)
df_age_smoke_cdc.to_csv('age_smoke_cdc.csv', index=False)

# Scrape ABS data
url_abs = 'https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking/latest-release'
response_abs = requests.get(url_abs)
soup_abs = BeautifulSoup(response_abs.content, 'html.parser')

# Find and process ABS data
chart_data_abs = None
for d in soup_abs.find_all('div', {'class': 'chart-data-wrapper'}):
    c = d.find('pre', {'class': 'chart-caption'}).text
    if "Proportion of people 15 years and over who were current daily smokers by age, 2011–12 to 2022" in c:
        chart_data_abs = json.loads(d.find('pre', {'class': 'chart-data'}).text)
        break

# Process ABS data
age_ranges_abs = ['15-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+']
# Extract the first element of each inner list and divide by 100
smoking_values_abs = [value[0] / 100 for value in chart_data_abs[7]]
smoke_dict_abs = {'age_range': age_ranges_abs, 'percentage': smoking_values_abs}
df_smoke_dict_abs = pd.DataFrame(smoke_dict_abs)
df_smoke_dict_abs.to_csv('smoke_dict_abs.csv', index=False)

print("Data scraping complete. CSV files saved.")