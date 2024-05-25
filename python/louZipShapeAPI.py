# -------------------------------------------------------- #
# Only run the API calls if you dont have the current data #
# -------------------------------------------------------- #

import pandas as pd
import os
import requests


shape = 'https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataAddresses/MapServer/3/query?outFields=*&where=1%3D1&f=geojson' # noqa
batch_size = 1000  # Number of records to retrieve per batch
offset = 0  # Initial offset value
data_list = []

while True:
    params = {
        'where': '1=1',
        'outFields': '*',
        'returnGeometry': 'false',
        'f': 'json',
        'resultOffset': offset,
        'resultRecordCount': batch_size
    }

    response = requests.get(shape, params=params)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        break

    query_result = response.json()
    features = query_result.get('features', [])

    if not features:
        break

    for feature in features:
        data_list.append(feature['properties'])

    if len(features) < batch_size:
        break

    offset += batch_size

# Convert the data list to a DataFrame
df = pd.DataFrame(data_list)

# Specify the output directory
output_directory = 'python/data'

# Create the directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Save the DataFrame to a CSV file in the specified directory
output_path = os.path.join(output_directory, 'lou_shape.csv')
df.to_csv(output_path, index=False)

print(f"Data saved to {output_path}")