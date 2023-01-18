import argparse
import os
import numpy as np
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt, dates as mdates
from google.cloud import storage
from libs.logger import Logger
from libs.bigquery_client import BigQueryClient

class TweetsVisualizer:

    def __init__(self, conf, location, key):
        """
        Init constructor
        """
        self.logger = Logger("./config/logger.ini", self.__class__.__name__)
        self.parser = ConfigParser(interpolation=None)
        self.parser.read(conf)
        self.parser.read('config/plot_%s.ini' % key)
        self.location = location
        self.key = key
        self.bq = BigQueryClient(conf)

    def __build_sql(self):
        sql = open("sql/%s.sql" % self.key).read()\
            .format(location = self.location).strip()
        print(sql)
        self.logger.log.info(sql)
        return sql

    def __draw(self, result):
        fig = plt.figure(figsize=(20.0, 10.0))
        plt.title(self.parser["PLOT"]["title"] % (self.location), fontsize=20)
        plt.xlabel(self.parser["PLOT"]["xlabel"], fontsize=16)
        plt.ylabel(self.parser["PLOT"]["ylabel"], fontsize=16)
        plt.grid()
        xcolumn = self.parser["PLOT"]["xcolumn"]
        ycolumn = self.parser["PLOT"]["ycolumn"]
        plt.plot(result[xcolumn], result[ycolumn])
        plt.xticks(np.arange(min(result[xcolumn]), max(result[xcolumn]), np.timedelta64(self.parser["PLOT"]["xinterval"], self.parser["PLOT"]["xunit"]), dtype='datetime64').astype(str), rotation=60)
        filename = "%s_%s.png" % (self.location, self.key)
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
        ARGPARSER.add_argument("--key", required=True, help="key string correspoding to sql file")
        ARGS = ARGPARSER.parse_args()
        ANALYZER = TweetsVisualizer(ARGS.conf, ARGS.location, ARGS.key)
        ANALYZER.main()
    except Exception as ex:
        raise



