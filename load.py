#!/usr/bin/env python
"""
This is a script to read a csv file with a first line of headers and index
to Elasticsearch, one line per document
"""

# core library modules
import argparse
import csv
from os.path import join

# third party packages
from elasticsearch import Elasticsearch, helpers

# local project modules
import load_summaries, load_cases
from utils import console_dump, get_latest_file, scan_directory, test_parse

ES = Elasticsearch()


def process_file(
        output_handler,
        data_path,
        target,
        file_name=None,
        ndx=None
):
    """
    Function to open and process a file of CSV summary reports
    """
    if not file_name:
        file_name = get_latest_file(data_path)
    doc_gen = target_module.DocGenerator(file_name)
    data_file = join(data_path, file_name)
    if not ndx:
        ndx = target.DEFAULT_INDEX
    with open(data_file, encoding=target.DEFAULT_FILE_ENCODING) as csv_read:
        csv_reader = csv.DictReader(csv_read)
        output_handler(
            ES, doc_gen.generate(csv_reader, ndx)
        )
    return


def process_all_files(
        output_handler, data_path, target, ndx=None
):
    """
    """
    file_map = scan_directory(data_path)
    for name in file_map:
        process_file(
            output_handler, data_path, target, file_name=name, ndx=ndx
        )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Index COVID data to Elasticsearch.',
        prog="load.py"
    )
    parser.add_argument(
        'data_type', type=str,
        choices=['summaries', 'cases'],
        help="type of data to load"
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
        '--test', dest='test', action='store_true',
        help="scan the files and parse for data problems"
    )
    target_group.add_argument(
        '--index', type=str, dest='index', default=None,
        help="name of Elasticsearch index for documents"
    )
    args = parser.parse_args()
    if args.data_type == 'summaries':
        target_module = load_summaries
    elif args.data_type == 'cases':
        target_module = load_cases
    else:
        # Already guarded by parser config, but the linter doesn't get that,
        # and complains about "target_module" usage below.
        raise ValueError("Make a valid choice")
    if args.console:
        handler = console_dump
    elif args.test:
        handler = test_parse
    else:
        handler = helpers.bulk
    if args.all:
        process_all_files(
            handler, args.data_path, target_module, ndx=args.index
        )
    elif args.files:
        for file in args.files:
            process_file(
                handler, args.data_path, target_module, file, ndx=args.index
            )
    else:
        process_file(
            handler,
            args.data_path,
            target_module,
            ndx=args.index
        )