select
  concat(year, '-', format('%02d', month)) ym,
  tweets
from (
  select
    extract(YEAR from `created_at` at time zone 'Asia/Manila') year,
    extract(MONTH from `created_at` at time zone 'Asia/Manila') month,
    count(1) tweets
  from
    `keio-sdm-masters-research.tweets.tweets_sentiment_v1`
  where
    location = '{location}'
  group by
    extract(YEAR from `created_at` at time zone 'Asia/Manila'),
    extract(MONTH from `created_at` at time zone 'Asia/Manila')
)
order by
  year,
  month
