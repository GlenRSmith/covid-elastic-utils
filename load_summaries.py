#!/usr/bin/env python
"""
This is a script to read a csv file with a first line of headers and index
to Elasticsearch, one line per document

The data is from:
https://github.com/CSSEGISandData/COVID-19

The familiar Johns Hopkins dashboard for the data:
https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6
"""

# core library modules
import argparse
import csv
from datetime import datetime
import json
import os
from os import listdir
from os.path import isfile, join

# third party packages
from elasticsearch import Elasticsearch, helpers
import dateutil.parser as dparser

# local project modules

ES = Elasticsearch()
DEFAULT_INDEX = 'covid_summary'


def scan_directory(pth):
    """
    List all the data files in the data path
    :return:
    """
    entries = {}
    for entry in listdir(pth):
        if isfile(join(pth, entry)) and 'csv' in entry:
            entries[entry] = dparser.parse(
                entry, fuzzy=True, dayfirst=False
            ).date().strftime("%Y-%m-%d")
    #  yyyy-MM-dd (strict_date_optional_time)
    return entries


def get_latest_file(directory):
    """
    Given a directory of summary report data files, return the name
    of the file with the most recent apparent date in the name.
    :param directory: Directory on local filesystem to scan
    :return:
    """
    latest = None
    latest_name = ''
    for file in [f for f in os.listdir(directory) if f.endswith('.csv')]:
        name = os.path.splitext(file)[0]
        dt = datetime.strptime(name, '%m-%d-%Y')
        if not latest or dt > latest:
            latest = dt
            latest_name = file
    return latest_name


class SummaryDocGenerator:
    """
    Generator class for summary report documents from a file.
    Class implementation so filename can be retained between documents
    :return:
    """

    def __init__(self, filename):
        self.filename = filename
        return

    def generate(self, fileobj, index_name):
        """
        Apply per-doc cleanup/customization and wrap in ES bulk-index-ready form
        :param fileobj: iterable file object with CSV summary reports
        :param index_name: name of ES index to send docs to
        :return:
        """
        for row in fileobj:
            # add a date field for the date of the data file so many summary
            # files can be in the same index
            row['date_data_file'] = dparser.parse(
                self.filename, fuzzy=True, dayfirst=False
            ).date().strftime("%Y-%m-%d")
            # change the format of date fields for ES recognition
            for field in ['Last Update']:
                row[field] = dparser.parse(
                    row[field], fuzzy=True, dayfirst=False
                ).date().strftime("%Y-%m-%d")
                #  yyyy-MM-dd (strict_date_optional_time)
            doc = {"_index": index_name, "_source": row}
            yield doc


def console_dump(es_client, doc_generator):
    """
    Helper function to send documents to the console instead of Elasticsearch.
    Signature must match signature of ES.helper.bulk, so has an unused param.
    :param es_client: Unused Elasticsearch client
    :param doc_generator: Document iterable
    :return:
    """
    for doc in doc_generator:
        print(json.dumps(doc))


def process_summary_file(
        output_handler,
        data_path,
        file_name=None,
        encoding="utf-8",
        index=DEFAULT_INDEX
):
    """
    Function to open and process a file of CSV summary reports
    """
    if not file_name:
        file_name = get_latest_file(data_path)
    sdg = SummaryDocGenerator(filename=file_name)
    target = join(data_path, file_name)
    with open(target, encoding=encoding) as csv_read:
        csv_reader = csv.DictReader(csv_read)
        output_handler(ES, sdg.generate(csv_reader, index))
    return


def process_all_summary_files(output_handler, data_path, index=DEFAULT_INDEX):
    """
    """
    file_map = scan_directory(data_path)
    for name in file_map:
        process_summary_file(
            output_handler, data_path, file_name=name, index=index
        )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Index COVID report data to Elasticsearch.',
        prog="load_summaries.py"
    )
    parser.add_argument(
        'data_path', type=str, help="directory path of summary report files"
    )
    which_files_group = parser.add_mutually_exclusive_group()
    which_files_group.add_argument(
        '--all', dest="all", action="store_true",
        help="process all files in the directory"
    )
    which_files_group.add_argument(
        '--files', type=str, nargs='*', dest='files',
        help="list of specific files to process"
    )
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument(
        '--console', dest='console', action='store_true',
        help="send documents to console instead of Elasticsearch"
    )
    target_group.add_argument(
        '--index', type=str, dest='index', default=DEFAULT_INDEX,
        help="name of Elasticsearch index for documents"
    )
    args = parser.parse_args()
    if args.console:
        handler = console_dump
    else:
        handler = helpers.bulk
    if args.all:
        process_all_summary_files(handler, args.data_path, index=args.index)
    elif args.files:
        for file in args.files:
            process_summary_file(
                handler, args.data_path, file, index=args.index
            )
    else:
        process_summary_file(handler, args.data_path)