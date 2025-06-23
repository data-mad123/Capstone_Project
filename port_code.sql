 CREATE TABLE IF NOT EXISTS port_code (
         port_code STRING,
         port_name STRING
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://myawsbucketfk11/port_code/';
