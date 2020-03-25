# core library modules
import json
import os

# third party packages
import dateutil.parser as dparser

# local project modules


def scan_directory(pth, day_first=False):
    """
    List all the data files in the data path
    :return: dict with filename keys and date string values
    """
    entries = {}
    for entry in os.listdir(pth):
        if os.path.isfile(os.path.join(pth, entry)) and 'csv' in entry:
            entries[entry] = dparser.parse(
                entry, fuzzy=True, dayfirst=day_first
            ).date().strftime("%Y-%m-%d")
    #  yyyy-MM-dd (strict_date_optional_time)
    return entries


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


def test_parse(es_client, doc_generator):
    """
    Helper function to scan files and test parse each line.
    Signature must match signature of ES.helper.bulk, so has an unused param.
    :param es_client: Unused Elasticsearch client
    :param doc_generator: Document iterable
    :return:
    """
    for doc in doc_generator:
        pass


def get_latest_file(directory, date_pattern='%m-%d-%Y'):
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
        # dt = datetime.strptime(name, date_pattern)
        dt = dparser.parse(name, fuzzy=True, dayfirst=False)
        if not latest or dt > latest:
            latest = dt
            latest_name = file
    return latest_name




