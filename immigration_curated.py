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

# Script generated for node Date table
Datetable_node1750612051687 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="date_code", transformation_ctx="Datetable_node1750612051687")

# Script generated for node Country code table
Countrycodetable_node1750659500926 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="country_code", transformation_ctx="Countrycodetable_node1750659500926")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1750675988558 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="i94_immigration", transformation_ctx="AWSGlueDataCatalog_node1750675988558")

# Script generated for node Add feature engineering to existing columns
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
Addfeatureengineeringtoexistingcolumns_node1750611928686 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"date_code":Datetable_node1750612051687, "country_code":Countrycodetable_node1750659500926, "immigration":AWSGlueDataCatalog_node1750675988558}, transformation_ctx = "Addfeatureengineeringtoexistingcolumns_node1750611928686")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Addfeatureengineeringtoexistingcolumns_node1750611928686, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750611882392", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750613344623 = glueContext.write_dynamic_frame.from_options(frame=Addfeatureengineeringtoexistingcolumns_node1750611928686, connection_type="s3", format="glueparquet", connection_options={"path": "s3://myawsbucketfk11/i94_immigration_curated/", "partitionKeys": []}, format_options={"compression": "uncompressed"}, transformation_ctx="AmazonS3_node1750613344623")

job.commit()
