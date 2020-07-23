#!/usr/bin/env python3

import argparse
import shutil
import tempfile
import urllib.request
from urllib.parse import urlparse
import requests
import logging
from http import HTTPStatus
from pymarc import parse_xml_to_array


# TAGS according to https://www.loc.gov/marc/bibliographic/ecbdlist.html
ELECTRONIC_LOCATION_AND_ACCESS = '856'

# indicators according to https://www.loc.gov/marc/bibliographic/ecbdlist.html
HTTP_ACCESS_METHOD = '4'

ACCEPTED_TYPES = ['MP4', 'MKV', 'MOV']

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Register files in the Onedata system')

requiredNamed = parser.add_argument_group('required named arguments')

requiredNamed.add_argument(
    '-H', '--host',
    action='store',
    help='Oneprovider host.',
    dest='host',
    required=True)

requiredNamed.add_argument(
    '-spi', '--space-id',
    action='store',
    help='Id of the space in which the files will be registered.',
    dest='space_id',
    required=True)

requiredNamed.add_argument(
    '-sti', '--storage-id',
    action='store',
    help='Id of the storage on which the files are located. Storage must be created as an `imported` storage with path type equal to `canonical`.',
    dest='storage_id',
    required=True)

requiredNamed.add_argument(
    '-t', '--token',
    action='store',
    help='Onedata access token.',
    dest='token',
    required=True)

requiredNamed.add_argument(
    '-c', '--collection-url',
    action='append',
    help='Open data collection URL. Many collections can be passed (e.g. `-c URL1 -c URL2`).',
    dest='collections',
    required=True)

parser.add_argument(
    '-m', '--file-mode',
    action='store',
    help='POSIX mode with which files will be registered, represented as an octal string.',
    dest='mode',
    default="0664"
)

parser.add_argument(
    '-dd', '--disable-auto-detection',
    action='store_true',
    help='Flag which disables automatic detection of file attributes and verification whether file exists on storage. '
         'Passing this flag results in faster registration of files but there is a risk of registering files that '
         'don\'t exist on storage. Such files will be visible in the space but not accessible.',
    dest='disable_auto_detection',
    default=False
)

parser.add_argument(
    '-lf', '--logging-frequency',
    action='store',
    type=int,
    help='Frequency of logging. Log will occur after registering every logging_freq number of files.',
    dest='logging_freq',
    default=None)

parser.add_argument(
    '-dv', '--disable-cert-verification',
    action='store_true',
    help='Flag which disables verification of SSL certificate.',
    dest='disable_cert_verification',
    default=False)


REGISTER_FILE_ENDPOINT = "https://{0}/api/v3/oneprovider/data/register"


def strip_server_url(storage_file_id):
    parsed_url = urlparse(storage_file_id)
    if parsed_url.scheme:
        return parsed_url.path
    else:
        return storage_file_id


def register_file(storage_file_id, size, checksum):
    headers = {
        'X-Auth-Token': args.token,
        "content-type": "application/json"
    }
    storage_file_id = strip_server_url(storage_file_id)
    payload = {
        'spaceId': args.space_id,
        'storageId': args.storage_id,
        'storageFileId': storage_file_id,
        'destinationPath': storage_file_id,
        'size': size,
        'mode': args.mode,
        'xattrs': {
            'checksum': checksum
        },
        'autoDetectAttributes': not args.disable_auto_detection
    }
    try:
        response = requests.post(REGISTER_FILE_ENDPOINT.format(args.host), json=payload, headers=headers, verify=(not args.disable_cert_verification))
        if response.status_code == HTTPStatus.CREATED:
            return True
        else:
            logging.error("Registration of {0} failed with HTTP status {1}.\n""Response: {2}"
                          .format(storage_file_id, response.status_code, response.content)),
            return False
    except Exception as e:
        logging.error("Registration of {0} failed due to {1}".format(storage_file_id, e), exc_info=True)


def download_and_load_marc21_record(url):
    with urllib.request.urlopen(url) as response:
        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            shutil.copyfileobj(response, tmp_file)
            tmp_file.flush()
            with open(tmp_file.name, 'r') as f:
                records = parse_xml_to_array(f)
                if records:
                    return records[0]


def get_file_fields(collection_url):
    collection_record = download_and_load_marc21_record(collection_url)
    if collection_record:
        return collection_record.get_fields(ELECTRONIC_LOCATION_AND_ACCESS)


def get_access_method(field):
    return field.indicator1


def is_http_access_method(field):
    return get_access_method(field) == HTTP_ACCESS_METHOD


def get_subfield(field, subfield_name):
    if field.get_subfields(subfield_name):
        return field.get_subfields(subfield_name)[0]


def get_type(field):
    return get_subfield(field, 'q')


def get_size(field):
    size = get_subfield(field, 's')
    if size:
        return int(size)


def get_control_number(field):
    return get_subfield(field, 'w')


def get_uri(field):
    return get_subfield(field, 'u')


def get_md5_checksum(field):
    control_number = get_control_number(field)
    return parse_md5(control_number)


def parse_md5(control_number):
    return control_number.split(';')[1]


args = parser.parse_args()
total_size = 0
total_count = 0

for collection_url in args.collections:
    print("Processing collection {0}".format(collection_url))
    file_fields = get_file_fields(collection_url)
    for file_field in file_fields:
        if is_http_access_method(file_field):
            if get_type(file_field) in ACCEPTED_TYPES:
                if register_file(get_uri(file_field), get_size(file_field), get_md5_checksum(file_field)):
                    total_size += get_size(file_field)
                    total_count += 1
                    if args.logging_freq and total_count % args.logging_freq == 0 and total_count > 0:
                        print("Registered {0} files".format(total_count))

print("\nTotal registered files count: {0}".format(total_count))
print("Total size: {0}".format(total_size))
