# -------------------------------------------------------- #
# Only run the API calls if you dont have the current data #
# -------------------------------------------------------- #

import pandas as pd
import os
import requests

url = 'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/2023CrimeData_OpenData/FeatureServer/0/query'  # noqa

batch_size = 1000

offset = 0

data_list = []

params = {
    'where': '1=1',
    'outFields': '*',
    'returnGeometry': 'false',
    'f': 'json',
    'resultOffset': offset,
    'resultRecordCount': batch_size
}

while True:
    params['resultOffset'] = offset
    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        break

    query_result = response.json()
    features = query_result.get('features', [])

    if not features:
        break

    for feature in features:
        data_list.append(feature['attributes'])

    if len(features) < batch_size:
        break

    offset += batch_size


df = pd.DataFrame(data_list)

output_directory = 'python/data'

# Create the directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Save the DataFrame to a CSV file in the specified directory
output_path = os.path.join(output_directory, 'crime_24.csv')
df.to_csv(output_path, index=False)

print(f"Data saved to {output_path}")