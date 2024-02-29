from google.cloud import bigquery
import os

credentials_path = 'stocks-poc-key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()
table_id = 'stocks-poc.options.funky-users'

rows_to_insert = [
    {"first_name": "Darshak", "last_name": "Bohra", "has_tried_popsql": False, "number_of_friends": 3},
]

errors = client.insert_rows_json(table_id, rows_to_insert)
if errors == []:
    print('New rows have been added.')
else:
    print(f'Encountered errors while inserting rows: {errors}')
