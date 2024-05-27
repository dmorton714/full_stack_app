# -------------------------------------------------------- #
# Only run the API calls if you dont have the current data #
# -------------------------------------------------------- #

import requests
import pandas as pd
import os

urls = [
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Louisville_Metro_KY_Uniform_Citation_Data_2023/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Louisville_Metro_KY_Uniform_Citation_Data_2022/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Louisville_Metro_KY_Uniform_Citation_Data_2021/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Louisville_Metro_KY_Uniform_Citation_Data_2020/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/UniformCitationData_2016_2019/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/UniformCitationData_2012_2015/FeatureServer/0' # noqa
]

batch_size = 1000
data_list = []

for url in urls:
    offset = 0

    while True:
        params = {
            'where': '1=1',
            'outFields': '*',
            'returnGeometry': 'false',
            'resultOffset': offset,
            'resultRecordCount': batch_size,
            'f': 'json'
        }

        response = requests.get(f"{url}/query", params=params)
        response.raise_for_status()

        query_result = response.json()
        features = query_result.get('features', [])

        for feature in features:
            data_list.append(feature['attributes'])

        if len(features) == 0:
            break

        offset += batch_size

df = pd.DataFrame(data_list)

output_directory = 'python/data'

os.makedirs(output_directory, exist_ok=True)

df.to_csv(os.path.join(output_directory, 'citation.csv'), index=False)

print(f"Data saved to {os.path.join(output_directory, 'citation.csv')}") # noqa
