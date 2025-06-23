 CREATE TABLE IF NOT EXISTS us_city_demographic (
      city STRING PRIMARY KEY,
      state STRING,
      median_age FLOAT,
      male_pop INT,
      female_pop INT,
      total_pop INT,
      no_of_veterans INT,
      foreign_born INT,
      avg_household_size FLOAT,
      state_code STRING,
      race STRING,
      count INT
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://myawsbucketfk11/us_city_demographic/';
