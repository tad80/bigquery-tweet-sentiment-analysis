import argparse
import os
import numpy as np
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt
from google.cloud import storage
from libs.logger import Logger
from libs.bigquery_client import BigQueryClient

class TweetsVisualizer:

    def __init__(self, conf, location):
        """
        Init constructor
        """
        self.logger = Logger("./config/logger.ini", self.__class__.__name__)
        self.parser = ConfigParser(interpolation=None)
        self.parser.read(conf)
        self.location = location
        self.bq = BigQueryClient(conf)

    def __build_sql(self):
        sql = """
select
  concat(year, '-', format('%02d', month)) ym,
  tweets
from (
  select
    extract(YEAR from `created_at`) year,
    extract(MONTH from `created_at`) month,
    count(1) tweets
  from
    `keio-sdm-masters-research.tweets.tweets_sentiment_v1`
  where
    location = '{location}'
  group by
    extract(YEAR from `created_at`),
    extract(MONTH from `created_at`)
)
order by
  year,
  month
""".format(location = self.location).strip()
        print(sql)
        self.logger.log.info(sql)
        return sql

    def __draw(self, result):
        fig = plt.figure(figsize=(20.0, 8.0))
        plt.title("Monthly amount of tweets from %s" % (self.location), fontsize=20)
        plt.xlabel("month", fontsize=16)
        plt.ylabel("# of tweets", fontsize=16)
        plt.grid()
        plt.plot(result['ym'], result['tweets'])
        plt.xticks(np.arange(min(result['ym']), max(result['ym']), np.timedelta64(6, 'M'), dtype='datetime64').astype(str), rotation=60)
        filename = "%s_tweets.png" % (self.location)
        plt.savefig(filename)
        with open(filename, mode="rb") as f:
            gcs = storage.Client()
            bucket = gcs.get_bucket("cs.gcp.tdsnkm.com")
            blob = bucket.blob("tweets/%s" % filename)
            blob.upload_from_string(
                f.read(),
                content_type="image/png"
            )
        os.remove(filename)

    def main(self):
        """
        Main method
        """
        result = self.bq.select(self.__build_sql()).to_dataframe()
        print(result)
        self.__draw(result)


if __name__ == "__main__":
    try:
        ARGPARSER = argparse.ArgumentParser(description="Analyze tweet sentiment")
        ARGPARSER.add_argument("--conf", required=True, help="Config file path")
        ARGPARSER.add_argument("--location", required=True, help="Location string")
        ARGS = ARGPARSER.parse_args()
        ANALYZER = TweetsVisualizer(ARGS.conf, ARGS.location)
        ANALYZER.main()
    except Exception as ex:
        raise



