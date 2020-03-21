# core library modules
import json

# third party packages

# local project modules


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


