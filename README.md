# cern-marc21-uploader
This repository contains script for registering files which metadata are stored in MARC21 format to the Onedata
system. 

The files are registered using Oneprovider [REST API](https://onedata.org/#/home/api/stable/oneprovider?anchor=operation/register_file).

## Getting started

Required packages can be installed using pip:
```bash
pip install -r requirements.txt
``` 

## Arguments

Script has the following list of **required** arguments:
* `-H`, `--host` - Oneprovider host.
* `-spi`, `--space-id` - Id of the space in which the files will be registered.
* `-sti`, `--storage-id` - Id of the storage on which the files are located.
                        Storage must be created as an `imported` storage with
                        path type equal to `canonical`.
* `-t`, `--token` - Onedata access token.
* `-c`, `--collection-url` - URL to MARC21 record describing collection of files. Many collections can be passed (e.g. `-c URL1 -c URL2`).

Additionally, script has the following list of **optional** arguments:
* `-m`, `--file-mode` - POSIX mode with which files will be registered, represented as an octal string (default: `"664"`)
* `-dd`, `--disable-auto-detection` - Flag which disables automatic detection of file attributes and verification whether file exists on storage.
Passing this flag results in faster registration of files but there is a risk of registering files that don't exist on storage.
Such files will be visible in the space but not accessible.
* `-dv`, `--disable-cert-verification` - Flag which disables verification of SSL certificate.
* `-lf`, `--logging-frequency` - Frequency of logging. Log will occur after registering every logging_freq number of files.

## Usage
```bash
./upload.py -spi $SPACE_ID -sti $STORAGE_ID -t $TOKEN -H $HOST \ 
 -c https://cds.cern.ch/record/423168/export/xm
```
