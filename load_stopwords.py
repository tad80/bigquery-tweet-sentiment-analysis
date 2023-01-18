import argparse
from google.cloud import bigquery
from configparser import ConfigParser
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import csv
from libs.logger import Logger
from libs.bigquery_client import BigQueryClient

class StopwordsLoader:


    def __init__(self, conf):
        self.logger = Logger("./config/logger.ini", self.__class__.__name__)
        stop_words = set(stopwords.words('english'))
        self.bq = BigQueryClient(conf)
        with open('stopwords.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames = ['word'])
            writer.writeheader()
            for word in stop_words:
                writer.writerow({'word': word})
        f.close()


    def load(self):
        with open('stopwords.csv', 'rb') as f:
            print(csv.reader(f))
            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField('word', 'STRING')
                ],
                skip_leading_rows = 1,
                source_format = bigquery.SourceFormat.CSV,
            )
            destination = 'keio-sdm-masters-research.tweets.stopwords'
            self.bq.client.load_table_from_file(f, destination, job_config=job_config).result()
            self.logger.log.info("Loaded {} rows".format(self.bq.client.get_table(destination).num_rows))
        f.close()

    def main(self):
        self.load()

if __name__ == "__main__":
    try:
        ARGPARSER = argparse.ArgumentParser(description="Load stopwords into BigQuery table")
        ARGPARSER.add_argument("--conf", required=True, help="BigQuery config file path")
        ARGS = ARGPARSER.parse_args()
        LOADER = StopwordsLoader(ARGS.conf)
        LOADER.main()
    except Exception as ex:
        print(str(ex))
        raise

