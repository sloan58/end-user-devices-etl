import json
import os
import sys

import _mssql
import pymssql
import requests
from dotenv import load_dotenv

load_dotenv()

server = os.getenv('PYMSSQL_SERVER')
user = os.getenv('PYMSSQL_USERNAME')
password = os.getenv('PYMSSQL_PASSWORD')
db = os.getenv('PYMSSQL_DB')
table = os.getenv('PYMSSQL_TABLE')

conn = pymssql.connect(server, user, password, db, autocommit=True, timeout=10)

customer_id = os.getenv('PALO_CUSTOMER_ID')
page_length = os.getenv('PALO_PAGE_LENGTH')

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

db_fields = [
    'AD_Domain',
    'AET',
    'Access_Point_IP',
    'Access_Point_Name',
    'Applications',
    'DHCP',
    'MAC',
    'Serial_Number',
    'Switch_IP',
    'Switch_Name',
    'Switch_Port',
    'allTags',
    'attr',
    'category',
    'confidence_score',
    'deviceid',
    'endpoint_protection',
    'endpoint_protection_vendor',
    'first_seen_date',
    'hostname',
    'in_use',
    'ip_address',
    'last_activity',
    'mac_address',
    'model',
    'number_of_caution_alerts',
    'number_of_critical_alerts',
    'number_of_info_alerts',
    'number_of_warning_alerts',
    'os_combined',
    'os_firmware_version',
    'os_group',
    'producer',
    'profile',
    'profile_type',
    'profile_vertical',
    'risk_level',
    'risk_score',
    'services',
    'site_name',
    'source',
    'subnet',
    'tags',
    'vendor',
    'wire_or_wireless',
    'zone'
 ]

while True:
    url = base_url + f'?offset={offset}&pagelength={page_length}&detail=true&customerid={customer_id}'

    try:
        response = requests.request('GET', url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e, file=sys.stderr)
        sys.exit()
    except requests.exceptions.RequestException as e:
        print(e, file=sys.stderr)
        sys.exit()

    try:
        items = response.json()['devices']
    except KeyError as e:
        items = []

    all_rows = []
    for item in items:
        row = []
        for field in db_fields:
            if field not in item.keys():
                row.append(f"''")
                continue
            if field in db_ints:
                row.append(f"{int(item[field])}")
            elif field in db_json:
                row.append(f"'{json.dumps(item[field])}'")
            else:
                row.append(f"'{str(item[field])}'")
        all_rows.append(f"({','.join(row)})")

    all_rows = ','.join(all_rows)
    fields = ','.join(f'[{str(val)}]' for val in db_fields)
    update_assignments = ','.join(f'[{str(val)}] = [script_source].[{str(val)}]' for val in db_fields)

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

    with conn.cursor(as_dict=True) as cursor:
        try:
            cursor.execute(statement)
        except pymssql.InterfaceError as e:
            print(f'Exception (InterfaceError): {e}', file=sys.stderr)
            sys.exit()
        except pymssql.DatabaseError as e:
            print(f'Exception (DatabaseError): {e}', file=sys.stderr)
            sys.exit()

    if len(items) < int(page_length):
        break

    offset += int(page_length)
