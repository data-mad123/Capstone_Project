 CREATE TABLE IF NOT EXISTS country_code (
         code INT,
         country STRING 
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://myawsbucketfk11/country_code/';
