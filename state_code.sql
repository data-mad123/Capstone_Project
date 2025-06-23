 CREATE TABLE IF NOT EXISTS state_code (
         state_code STRING,
         state_name STRING
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://myawsbucketfk11/state_code/';
