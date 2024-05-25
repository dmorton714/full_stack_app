# -------------------------------------------------------- #
# Only run the API calls if you dont have the current data #
# -------------------------------------------------------- #

import requests
import pandas as pd
import os

urls = [
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2024/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2023/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2022/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2021/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2019/FeatureServer/0', # noqa
    'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2018/FeatureServer/0'  # noqa
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

df.to_csv(os.path.join(output_directory, 'expenditures_data.csv'), index=False)

print(f"Data saved to {os.path.join(output_directory, 'expenditures_data.csv')}") # noqa


'''
API call to Expenditures Data For Fiscal Year 2018 - 2024

- Louisville Metro KY - Expenditures Data For Fiscal Year 2024
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2024/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson

- Louisville Metro KY - Expenditures Data For Fiscal Year 2023
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2023/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson

- Louisville Metro KY - Expenditures Data For Fiscal Year 2022
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2022/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson

- Louisville Metro KY - Expenditures Data For Fiscal Year 2021
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2021/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson

- Louisville Metro KY - Expenditures Data For Fiscal Year 2019
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2019/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson

- Louisville Metro KY - Expenditures Data For Fiscal Year 2018
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/eExpenditures_2018/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
'''
