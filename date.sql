 CREATE TABLE IF NOT EXISTS date_code (
         sas_date INT,
         converted_date DATE
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://myawsbucketfk11/date_code/';
