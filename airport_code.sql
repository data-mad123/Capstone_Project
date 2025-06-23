CREATE TABLE IF NOT EXISTS airport_code (
    ident STRING
    type STRING
    name STRING
    elevation_ft INT
    continent STRING
    iso_country STRING
    iso_region STRING
    municipality STRING
    icao_code STRING
    iata_code STRING
    gps_code STRING
    local_code STRING
    coordinates STRING
  )
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION 's3://myawsbucketfk11/airport_code/';
