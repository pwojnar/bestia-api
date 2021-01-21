# by obywatel39
# FIRST pip install requests

import json
import logging
import os
from queue import Queue
from threading import Thread

import requests
logger = logging.getLogger('api')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

BASE_URL = 'https://bestia-api.mf.gov.pl/api'


def save_json_to_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump({'data': data}, f, ensure_ascii=False, indent=4)


def is_file_exists(filename):
    return os.path.exists(filename)


def get_data_from_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)['data']


def fetch_all(url, params=None):
    start_url = BASE_URL + url
    page = 1
    records = []
    self_url = url
    last_url = ''
    params = params or {}
    params['page'] = page
    params['limit'] = 100

    while self_url != last_url:
        params['page'] = page
        r = requests.get(start_url, params)
        logger.info(url + ': ' + str(r.status_code) + ' len: ' + str(len(r.content)))
        if r.status_code != 200:
            return []
        response = r.json()
        data = response['data']
        links = response['links']
        self_url = links['self']
        last_url = links['last']
        records += data
        page += 1
    return records


def get_units():
    filename = 'units.json'
    if is_file_exists(filename):
        return get_data_from_file(filename)
    url = '/jednostki'
    all_units = fetch_all(url)
    selected_units = [u for u in all_units if u['gt'] == '1' or u['gt'] == '2' or u['gt'] == '3 ' or u['pt'] == '2']
    save_json_to_file(filename, selected_units)
    return selected_units


def get_unit_data(unit_id):
    url = '/pozycje-rb27s'
    version = 10
    params = {
        'filter[jednostka-const-id]': unit_id,
        'filter[okres-okres]': '4',
        'filter[okres-rok]': '2019'
    }
    data = []
    while len(data) == 0 and version >= 0:
        params['filter[sprawozdanie-wersja]'] = version
        logger.info('Get unit ' + unit_id + ' version ' + str(version))
        data = fetch_all(url, params)
        version = version - 1
    if len(data) > 0:
        logger.info('Got data for ' + unit_id + '/' + str(version + 1))
    return data


def save_unit_data(unit_id):
    filename = 'unit-{}.json'.format(unit_id)

    if not is_file_exists(filename):
        unit_data = get_unit_data(unit_id)
        save_json_to_file(filename, unit_data)


def get_queued_unit_data(queue):
    while not queue.empty():
        unit_id = queue.get()
        try:
            logger.info('Start: ' + unit_id)
            save_unit_data(unit_id)
            logger.info('Done: ' + unit_id)
        except Exception as e:
            logger.error('Error: ' + str(e))
        queue.task_done()
    return True


if __name__ == '__main__':
    units = get_units()
    queue = Queue(maxsize=0)
    num_threads = 10
    for index, unit in enumerate(units):
        unit_id = unit['const-id']
        queue.put(unit_id)

    for i in range(num_threads):
        logger.debug('Starting thread {}'.format(i))
        worker = Thread(target=get_queued_unit_data, args=(queue, ))
        worker.setDaemon(True)
        worker.start()
    queue.join()
