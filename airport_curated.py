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

# Script generated for node Airport code table
Airportcodetable_node1750662653835 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="airport_code", transformation_ctx="Airportcodetable_node1750662653835")

# Script generated for node Clean column
SqlQuery0 = '''
select 
ident,
REPLACE(type,'_',' ')
name,
elevation_ft,
continent,
iso_country,
RIGHT(iso_region,2) AS iso_region,
municipality,
icao_code,
iata_code,
gps_code,
local_code,
coordinates
from airport_code
'''
Cleancolumn_node1750662667275 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"airport_code":Airportcodetable_node1750662653835}, transformation_ctx = "Cleancolumn_node1750662667275")

# Script generated for node Airport curated
EvaluateDataQuality().process_rows(frame=Cleancolumn_node1750662667275, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750659341645", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Airportcurated_node1750664090383 = glueContext.getSink(path="s3://myawsbucketfk11/airport_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Airportcurated_node1750664090383")
Airportcurated_node1750664090383.setCatalogInfo(catalogDatabase="database",catalogTableName="airport_curated")
Airportcurated_node1750664090383.setFormat("glueparquet", compression="snappy")
Airportcurated_node1750664090383.writeFrame(Cleancolumn_node1750662667275)
job.commit()
