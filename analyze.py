import argparse
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.logger import Logger
from libs.bigquery_client import BigQueryClient

class TweetSentimentAnalyzer:

    def __init__(self, conf, key, start, end):
        """
        Init constructor
        """
        self.logger = Logger("./config/logger.ini", self.__class__.__name__)
        self.parser = ConfigParser(interpolation=None)
        self.parser.read(conf)
        self.key = key
        self.start = datetime.strptime(start, "%Y-%m-%d")
        self.end = datetime.strptime(end, "%Y-%m-%d")
        self.bq = BigQueryClient(conf)

    def __build_sql(self, first_day):
        from_dt = first_day.strftime("%Y-%m-%d 00:00:00")
        until_dt = (first_day + relativedelta(months = +1, day = 1)).strftime("%Y-%m-%d 00:00:00")
        sql = """
        SELECT
          sentiment.id,
          sentiment.text,
          sentiment.classifier[offset(1)] score,
          sentiment.created_at,
          array_agg(annotation order by annotation.probability desc limit 1) annotation,
          '{key}' location
        FROM
          ML.PREDICT(MODEL `keio-sdm-masters-research.tweets.sentiment_v1`, (
              SELECT
                tc.id,
                tc.text,
                tc.created_at,
                annotation,
                tc.lang
            FROM
                tweets.tweets_{key} tc
            CROSS JOIN UNNEST(tc.entities.annotations) as annotation
            WHERE
                created_at >= '{from_dt}' and
                created_at < '{until_dt}' and
                text is not null and
                annotation.type = 'Place' and
                replace(lower(annotation.normalized_text), ' ', '_') like '%{key}%'
          )) sentiment
        group by id, text, score, created_at

        """.format(key = self.key, from_dt = from_dt, until_dt = until_dt).strip()
        return sql

    def main(self):
        """
        Main method
        """
        queries = []
        first_day = self.start
        table_id = "%s.%s.tweets_sentiment_v1" % (self.parser["BigQuery"]["project_id"], self.parser["BigQuery"]["dataset_id"])
        while first_day < self.end:
            queries.append(self.__build_sql(first_day))
            first_day = first_day + relativedelta(months=1)
        with ThreadPoolExecutor(max_workers=10) as executor:
            for query in queries:
                self.logger.log.info(query)
                executor.submit(self.bq.select_insert, query, table_id)
        executor.shutdown()


if __name__ == "__main__":
    try:
        ARGPARSER = argparse.ArgumentParser(description="Analyze tweet sentiment")
        ARGPARSER.add_argument("--conf", required=True, help="Config file path")
        ARGPARSER.add_argument("--key", required=True, help="Location string")
        ARGPARSER.add_argument("--start", required=True, help="Start date. YYYY-MM-DD")
        ARGPARSER.add_argument("--end", required=True, help="End date. YYYY-MM-DD")
        ARGS = ARGPARSER.parse_args()
        ANALYZER = TweetSentimentAnalyzer(ARGS.conf, ARGS.key, ARGS.start, ARGS.end)
        ANALYZER.main()
    except Exception as ex:
        raise

