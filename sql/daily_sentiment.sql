select
  format_timestamp('%Y-%m-%d', cast(max(`created_at`) as timestamp), 'Asia/Manila') dt,
  avg(score) sentiment
FROM
  `keio-sdm-masters-research.tweets.tweets_sentiment_v1`
where
  location = '{location}'
group by
  format_timestamp('%Y-%m-%d', cast(`created_at` as timestamp), 'Asia/Manila')
order by
  dt
