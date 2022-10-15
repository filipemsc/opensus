library(bigrquery)

bq_auth(path = 'gcp-auth-dev.json')

bq_dataset_create("opensus-dev.sia")

bq_perform_query("
  LOAD DATA OVERWRITE sia.pa_sergipe
  FROM FILES (
  format = 'parquet',
  uris = ['gs://opensus-dev/br_ms_sia/pase/data/*parquet']);",
  "opensus-dev")


