from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import json
import sys

spark = SparkSession.builder.appName("SGDApp").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

hdfs_host = 'hdfs://namenode:9000'
kafka = "broker:29092"

try:
    hdfs_host = sys.argv[1]
    kafka = sys.argv[2]
except:
    pass

dfs = {}

LOGGER.info(f"hdfs host: {hdfs_host}")
LOGGER.info(f"kafka broker: {kafka}")


metadata = ''.join(spark.sparkContext.textFile(f'{hdfs_host}/data/metadata.json').collect())
metadata = json.loads(metadata)




for dataflow in metadata['dataflows']:
    for source in dataflow['sources']:
        dfs[source['name']] = spark.read.load(hdfs_host+source['path'],format=source['format'].lower())
    for transformation in dataflow['transformations']:
        if transformation['type'] == 'validate_fields':
            dfs[transformation['name']+'_ok'] = dfs[transformation['params']['input']]
            dfs[transformation['name']+'_ko'] = dfs[transformation['params']['input']].withColumn('arraycoderrorbyfield',lit(None))
            for validation in transformation['params']['validations']:
                for v in validation['validations']:
                    if v == 'notEmpty':
                        dfs[transformation['name']+'_ok'] = dfs[transformation['name']+'_ok'].filter(col(validation['field']) != '')
                        dfs[transformation['name']+'_ko'] = dfs[transformation['name']+'_ko'].withColumn('arraycoderrorbyfield', when(col(validation['field']) == '',lit(validation['field'])).otherwise(col('arraycoderrorbyfield')))
                    if v == 'notNull':
                        dfs[transformation['name']+'_ok'] = dfs[transformation['name']+'_ok'].filter(col(validation['field']).isNotNull())
                        dfs[transformation['name']+'_ko'] = dfs[transformation['name']+'_ko'].withColumn('arraycoderrorbyfield', when(col(validation['field']).isNull(),lit(validation['field'])).otherwise(col('arraycoderrorbyfield')))
            dfs[transformation['name']+'_ko'] = dfs[transformation['name']+'_ko'].filter(col('arraycoderrorbyfield').isNotNull())
        if transformation['type'] == 'add_fields':
            dfs[transformation['name']] = dfs[transformation['params']['input']]
            for field in transformation['params']['addFields']:
                dfs[transformation['name']] = dfs[transformation['name']].withColumn(field['name'],eval(f"{field['function']}()"))
    for sink in dataflow['sinks']:
        if sink['format'] == "KAFKA":
            dfs[sink["name"]] = dfs[sink["input"]].select(to_json(struct([col(c) for c in dfs[sink["input"]].columns])).alias("value"))
            LOGGER.info("Sending to kafka ...")
            LOGGER.info(dfs[sink["input"]].show())
            for topic in sink['topics']:
                dfs[sink["name"]].write.format("kafka").option("kafka.bootstrap.servers", kafka).option("topic", topic).save()
                LOGGER.info(f"Sent to topic: {topic}")
        if sink['format'] == "JSON":
            LOGGER.info("Sending to hdfs ...")
            LOGGER.info(dfs[sink["input"]].show())
            for path in sink["paths"]:
                dfs[sink["input"]].coalesce(1).write.mode(sink["saveMode"].lower()).format('json').save(hdfs_host+path)

