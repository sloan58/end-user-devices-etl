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

conn = pymssql.connect(server, user, password, db, autocommit=True)
cursor = conn.cursor(as_dict=True)

customer_id = os.getenv('PALO_CUSTOMER_ID')
table = os.getenv('PALO_TABLE_NAME')

offset = 0

base_url = os.getenv('PALO_BASE_API') + '/pub/v4.0/device/list'

headers = {
    'X-Key-Id': os.getenv('PALO_API_KEY_ID'),
    'X-Access-Key': os.getenv('PALO_API_ACCESS_KEY'),
}

db_ints = [
    'pkey',
    'risk_score',
    'number_of_critical_alerts',
    'number_of_warning_alerts',
    'number_of_caution_alerts',
    'number_of_info_alerts',
    'zone',
]

db_json = [
    'tags',
    'attr',
    'allTags',
]

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

    all_rows = []
    for item in items['devices']:
        row = []
        for key, val in item.items():
            if key in db_ints:
                row.append(f"{int(val)}")
            elif key in db_json:
                json = val.replace("\'", "\"")
                row.append(f"'{json}'")
            else:
                row.append(f"'{str(val)}'")
        all_rows.append(f"({','.join(row)})")

    all_rows = ','.join(all_rows)
    fields = ','.join(f'[{str(val)}]' for val in items['devices'][0].keys())
    update_assignments = ','.join(f'[{str(val)}] = [script_source].[{str(val)}]' for val in items['devices'][0].keys())

    dynamic_content = {
        'table': table,
        'fields': fields,
        'all_rows': all_rows,
        'update_assignments': update_assignments
    }

    statement = '''merge [{table}] using (values {all_rows}) [script_source] ({fields})
        on [script_source].[MAC] = [{table}].[MAC]
        when matched then update set {update_assignments}
        when not matched then insert ({fields})
        values ({fields});
    '''.format(**dynamic_content)

    cursor.execute(statement)

    if len(items['devices']) < 1000:
        break

    offset += 1000
