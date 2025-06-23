 CREATE TABLE IF NOT EXISTS world_temperature (
      date  DATE,
      avg_temp  FLOAT,
      avg_temp_uncertainty  FLOAT,
      country  STRING
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://myawsbucketfk11/world_temperature/';
