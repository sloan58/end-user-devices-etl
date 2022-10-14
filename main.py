import json
import os
import sys

import pymssql
import requests
from dotenv import load_dotenv

load_dotenv()

server = os.getenv('PYMSSQL_SERVER')
user = os.getenv('PYMSSQL_USERNAME')
password = os.getenv('PYMSSQL_PASSWORD')
db = os.getenv('PYMSSQL_DB')

customer_id = os.getenv('PALO_CUSTOMER_ID')

conn = pymssql.connect(server, user, password, db, autocommit=True)
cursor = conn.cursor(as_dict=True)

offset = 0

base_url = os.getenv('PALO_BASE_API') + '/pub/v4.0/device/list'

headers = {
    'X-Key-Id': os.getenv('PALO_API_KEY_ID'),
    'X-Access-Key': os.getenv('PALO_API_ACCESS_KEY'),
}

while True:
    url = base_url + f'?offset={offset}&pagelength=1000&detail=true&customerid={customer_id}'

    try:
        response = requests.request('GET', url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e, file=sys.stderr)
        sys.exit()
    except requests.exceptions.RequestException as e:
        print(e, file=sys.stderr)
        sys.exit()

    items = response.json() or []

    payload = list(map(lambda item: ('testing3', json.dumps(item)), items['devices']))

    cursor.executemany('INSERT INTO test_pymssql(stringValue, jsonValue) VALUES (%s, %s)', payload)

    if len(payload) < 1000:
        break

    offset += 1000
