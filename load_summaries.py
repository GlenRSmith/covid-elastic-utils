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
import difflib
import json

# third party packages
import dateutil.parser as dparser

# local project modules

DEFAULT_INDEX = 'covid_summary'
DEFAULT_FILE_ENCODING = 'utf-8'


class SummaryConf(object):

    # prepopulate with known alts
    field_match_map = {
        "\ufeffProvince/State": "Province/State",
        "Province_State": "Province/State",
        "Country_Region": "Country/Region",
        "Last_Update": "Last Update",
        "Lat": "Latitude", "Long_": "Longitude"
    }

    def __init__(self):
        with open('config/covid_summary_template.json') as conf_file:
            conf = json.loads(conf_file.read())
            self.conf = conf['covid_summary']
        for field in self.conf['mappings']['properties'].keys():
            self.field_match_map[field] = field

    def get_fields(self):
        return self.conf['mappings']['properties'].keys()

    def add_field_match(self, new_label, original_label):
        print("adding new match {0} for {1}".format(
            new_label, original_label
        ))
        self.field_match_map[new_label] = original_label
        print("field_match_map is now {}".format(json.dumps(
            self.field_match_map
        )))

    def find_field_name(self, match):
        ret_val = self.field_match_map.get(match, None)
        if not ret_val:
            for co in [1, 0.8, 0.6]:
                match_list = difflib.get_close_matches(
                    match, self.get_fields(), 1, cutoff=co
                )
                if not match_list:
                    print("no match for {0}, cutoff {1}".format(match, co))
                    continue
                else:
                    ret_val = match_list[0]
                    self.add_field_match(match, ret_val)
                    print(
                        "match for {0} cutoff {1} is {2}".format(
                            match, co, ret_val
                        )
                    )
                    break
        if not ret_val:
            raise ValueError("never found a match for {0}".format(match))
        return ret_val


class DocGenerator:
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
            # Deal with slight changes to field names from upstream
            doc = {}
            mapper = SummaryConf()
            for field in row:
                doc[mapper.find_field_name(field)] = row[field]
            # change the format of date fields for ES recognition
            for field in ['Last Update']:
                try:
                    doc[field] = dparser.parse(
                        doc[field], fuzzy=True, dayfirst=False
                    ).date().strftime("%Y-%m-%d")
                    #  yyyy-MM-dd (strict_date_optional_time)
                except KeyError as err:
                    print("KeyError: {}".format(err))
                    print(doc)
                except Exception as err:
                    print("Exception: {}".format(err))
                    print(err)
                    print(doc)
            yield {"_index": index_name, "_source": doc}
