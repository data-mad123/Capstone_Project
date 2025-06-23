import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Immigration Table
ImmigrationTable_node1750677161294 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="i94_immigration", transformation_ctx="ImmigrationTable_node1750677161294")

# Script generated for node Date Code Table
DateCodeTable_node1750677163691 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="date_code", transformation_ctx="DateCodeTable_node1750677163691")

# Script generated for node Country Code Table
CountryCodeTable_node1750677167038 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="country_code", transformation_ctx="CountryCodeTable_node1750677167038")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
immigration.record_id,
immigration.arrival_year,
immigration.arrival_month,
c1.country as citizenship_country,
c2.country as residence_country,
immigration.us_port_of_entry_code,
d1.converted_date AS arrival_date,
immigration.mode_of_transport_code,
immigration.us_state_code,
d2.converted_date AS departure_date,
immigration.age,
immigration.visa_category_code,
immigration.traveler_count,
d3.converted_date AS file_added_date,
immigration.visa_issued_post_code,
immigration.occupation,
immigration.entry_flag,
immigration.departure_flag,
immigration.update_flag,
immigration.match_flag,
immigration.birth_year,
d4.converted_date AS admitted_until_date,
immigration.gender,
immigration.ins_number,
immigration.airline_code,
immigration.admission_number,
immigration.flight_number,
immigration.visa_type
FROM immigration
LEFT JOIN date_code d1 on immigration.arrival_date = d1.sas_date
LEFT JOIN date_code d2 on immigration.departure_date = d2.sas_date
LEFT JOIN date_code d3 on immigration.file_added_date = d3.sas_date
LEFT JOIN date_code d4 on immigration.admitted_until_date = d4.sas_date
LEFT JOIN country_code c1 on immigration.citizenship_country_code = c1.code
LEFT JOIN country_code c2 on immigration.residence_country_code = c1.code;
'''
SQLQuery_node1750677239798 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"country_code":CountryCodeTable_node1750677167038, "immigration":ImmigrationTable_node1750677161294, "date_code":DateCodeTable_node1750677163691}, transformation_ctx = "SQLQuery_node1750677239798")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750677239798, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750677081664", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750677264449 = glueContext.getSink(path="s3://myawsbucketfk11/i94_immigration_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750677264449")
AmazonS3_node1750677264449.setCatalogInfo(catalogDatabase="database",catalogTableName="immigration_curated")
AmazonS3_node1750677264449.setFormat("glueparquet", compression="snappy")
AmazonS3_node1750677264449.writeFrame(SQLQuery_node1750677239798)
job.commit()
