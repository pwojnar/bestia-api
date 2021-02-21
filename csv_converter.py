import json
import logging
import os
import re
import csv

logger = logging.getLogger('api')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

headers = ["plik", "jednostka-id", "jednostka-const-id", "jednostka-nazwa", "jednostka-wk", "jednostka-pk", "jednostka-gk", "jednostka-gt", "jednostka-pt", "jednostka-regon", "okres-rok", "okres-okres", "sprawozdanie-wersja", "sprawozdanie-data", "dzial", "rozdzial", "paragraf", "finansowanie-paragrafu", "pl", "na", "po", "dw", "do", "no", "nz", "np", "so", "su", "sd", "uz", "ot"]
columns = ["jednostka-id", "jednostka-const-id", "jednostka-nazwa", "jednostka-wk", "jednostka-pk", "jednostka-gk", "jednostka-gt", "jednostka-pt", "jednostka-regon", "okres-rok", "okres-okres", "sprawozdanie-wersja", "sprawozdanie-data", "dzial", "rozdzial", "paragraf", "finansowanie-paragrafu", "pl", "na", "po", "dw", "do", "no", "nz", "np", "so", "su", "sd", "uz", "ot"]

def save_json_to_csv(src_filename, dest_file):
    with open(src_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)['data']
        for unit in data:
            writer = csv.writer(dest_file)
            row = [unit["jednostka-const-id"] + "-" + unit["okres-rok"]]
            for column in columns:
                if column in unit:
                    row.append(unit[column])
                else:
                    row.append('')
            writer.writerow(row)


def walk_dir(path, dir, csvfile):
    new_path, subdirs, filenames = next(os.walk(path + '/' + dir))
    for name in filenames:
        if re.match('unit-([a-z0-9-]+)-([0-9]{4}).json', name):
            save_json_to_csv(new_path + '/' + name, csvfile)
    for subdir in subdirs:
        walk_dir(new_path, subdir, csvfile)


def init_csv_file(filename):
    if not os.path.exists(filename):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)


dest_filename = 'data.csv'
init_csv_file(dest_filename)
with open(dest_filename, 'a', newline='') as csvfile:
    walk_dir('.', '', csvfile)
