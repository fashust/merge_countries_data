# -*- coding: utf-8 -*-

import json
import asyncio
import aiohttp


COUNTRIES_JSON_URL = (
    'http://iatistandard.org/201/codelists/downloads'
    '/clv3/json/en/OrganisationRegistrationAgency.json'
)
COUNTRIES_CSV_DATA_URL = (
    'https://www.artlebedev.ru/tools/country-list/tab/'
)
EXCLUDE_KEYS = ['description', 'public-database']


async def get_countries_json_data(loop):
    """
    """
    _response = None
    with aiohttp.ClientSession(loop=loop) as session:
        async with session.get(COUNTRIES_JSON_URL) as response:
            assert response.status == 200
            _response = await response.read()
    _json_response = json.loads(_response.decode('utf-8')).get('data')
    if _json_response:
       _json_response = {
            _['code']: {
                key: val
                for key, val in _.items()
                if key not in EXCLUDE_KEYS
            }
            for _ in _json_response
        }
    return _json_response


async def get_countries_csv(loop):
    """
    """
    _response = None
    with aiohttp.ClientSession(loop=loop) as session:
        async with session.get(COUNTRIES_CSV_DATA_URL) as response:
            assert response.status == 200
            _response = await response.read()
    _response = [
        _.split('\t')[:4] for _ in _response.decode('utf-8').split('\n')[1:]
    ]
    _response = {
        _[3]: {
            'name_ru': _[0],
            'name_en': _[2]
        }
        for _ in _response
    }
    return _response
    
    
async def merge_data(countries_json, countries_csv):
    """
    """
    for key, val in countries_csv.items():
        orig_keys = [
            _ for _ in countries_json.keys()
            if _[:2] == key
        ]
        if orig_keys:
            for _key in orig_keys:
                data = countries_json[_key]
                data.update(val)
                countries_json[_key] = data
    return countries_json


def main():
    """
    main entry point
    """
    loop = asyncio.get_event_loop()
    countries_json = loop.run_until_complete(get_countries_json_data(loop))
    countries_csv = loop.run_until_complete(get_countries_csv(loop))
    merged_data = loop.run_until_complete(merge_data(
        countries_json, countries_csv
    ))
    with open('merged.json', 'w', encoding='utf-8') as merged_file:
        merged_file.write(json.dumps(merged_data, indent=4, sort_keys=True))


if __name__ == '__main__':
    main()
