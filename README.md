# covid-elastic-utils
Utilities for using the Elastic Stack with data about COVID-19

### Loading summary reports

#### Obtain the data locally
You will need a local copy of the data files found in the [John Hopkins repo](https://github.com/CSSEGISandData/COVID-19)
at the path `csse_covid_19_data/csse_covid_19_daily_reports`

#### Set up Elasticsearch

Currently only a local instance is supported.

You will need to install and run it, then use the the index template in the config directory to assure your mapping is "good" after you run indexing.

#### Set up this utility

- Clone this repo.
- Create a virtual environment and install the requirements from requirements.txt.
- Execute `load_summaries.py <path>` where `path` is the full path to the directory where the summary reports are on your local file system. (This can be a symlink.)
- Use the `--console` option to dump the JSON docs to the console instead of bulk indexing to Elasticsearch.

