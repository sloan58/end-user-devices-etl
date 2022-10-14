import sys

import requests

url = "https://api.punkapi.com/v2/beers"

payload={}
headers = {
    'X-Key-Id': 'api_key_id',
    'X-Access-Key': 'api_access_key',
}

response = requests.request("GET", url, headers=headers, data=payload).json()

for item in response:
    print(item)
    sys.exit()
