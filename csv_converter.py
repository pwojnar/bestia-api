import json
import logging
import os
import re
import csv

logger = logging.getLogger('api')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def save_json_to_csv(src_filename, dest_file):
    with open(src_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)['data']
        for row in data:
            writer = csv.writer(dest_file)
            writer.writerow(row.values())


def init_csv_file(filename):
    if not os.path.exists(filename):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["jednostka-id", "jednostka-const-id", "jednostka-nazwa", "jednostka-wk", "jednostka-pk", "jednostka-gk", "jednostka-gt", "jednostka-pt", "jednostka-regon", "okres-rok", "okres-okres", "sprawozdanie-wersja", "sprawozdanie-data", "dzial", "rozdzial", "paragraf", "pl", "na", "po", "dw", "do", "no", "nz", "np", "so", "su", "sd", "uz", "ot"])


_, _, filenames = next(os.walk('.'))
dest_filename = 'data.csv'
init_csv_file(dest_filename)
with open(dest_filename, 'a', newline='') as csvfile:
    for name in filenames:
        if re.match('unit-([a-z0-9-]+)-([0-9]{4}).json', name):
            save_json_to_csv(name, csvfile)
