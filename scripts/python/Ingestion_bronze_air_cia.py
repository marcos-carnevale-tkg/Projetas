# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
from schemas import air_cia_schema
from credentials import read_ini

# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_air_cia").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()

dict_data = dict_data = read_ini(data)

# Set input and output paths
input_path = dict_data["hdfs_path_input_air_cia"]
output_path = dict_data["hdfs_path_output_air_cia"]

#===================================================================================================
# Read the file csv
schema  = air_cia_schema()
air_cia = spark.read.csv(input_path, header=True, schema=schema, sep=";")

#===================================================================================================
# Normalize header to snake case
air_cia = air_cia.select([col(c).alias(c.replace(" ", "_")) for c in air_cia.columns])
air_cia = air_cia.select([col(c).alias(c.lower()) for c in air_cia.columns])
air_cia = (
            air_cia.
            withColumnRenamed("razão_social", "razao_social").
            withColumnRenamed("data_decisão_operacional", "data_decisao_operacional").
            withColumnRenamed("decisão_operacional", "decisao_operacional").
            withColumnRenamed("atividades_aéreas", "atividades_aereas")
        )

#===================================================================================================
# Split the column "ICAO IATA" in two columns "icao" and "iata" using the separator " "
air_cia = (
    air_cia.
    withColumn("icao", split(col("icao_iata"), " ").getItem(0))
    .withColumn("iata", split(col("icao_iata"), " ").getItem(1))
    .drop("icao_iata")
)

#===================================================================================================
# ETL Column "telefone"

# Replaces 
air_cia = (
    air_cia
    .withColumn("telefone", regexp_replace(col("telefone"), "\/", "|"))
    .withColumn("telefone", regexp_replace(col("telefone"), " ", ""))
    .withColumn("telefone", regexp_replace(col("telefone"), "Fax:", ""))
    
)

# Split column "telefone" using the separator "/" and create a new column "telefone"
air_cia = (
    air_cia.
    withColumn("telefone", split(col("telefone"), "\|"))
)

# Remove "(55)"
air_cia = (
    air_cia
    .withColumn("telefone", array(
        regexp_replace(col("telefone")[0], r"^\((55)\)", ""),
        regexp_replace(col("telefone")[1], r"^\((55)\)", ""))
    )
)

# Create column area_code
air_cia = (
    air_cia
    .withColumn("area_code", array(
        regexp_extract(col("telefone")[0], r"^\((\d{2})\)", 1),
        regexp_extract(col("telefone")[1], r"^\((\d{2})\)", 1)
    ))
)

# Remove area code from column "telefone"
air_cia = (
    air_cia
    .withColumn("telefone", array(
        regexp_replace(col("telefone")[0], r"^\((\d{2})\)", ""),
        regexp_replace(col("telefone")[1], r"^\((\d{2})\)", "")
    ))
)

#===================================================================================================
# Format the column "data_decisao_operacional" and "validade_operacional" to "yyyy-MM-dd"
air_cia = (
    air_cia.
    withColumn("data_decisao_operacional", to_date(col("data_decisao_operacional"), "dd/MM/yyyy")).
    withColumn("validade_operacional", to_date(col("validade_operacional"), "dd/MM/yyyy"))
)

#===================================================================================================
# Format the column "decisao_operacional" extracting numbers
air_cia = (
    air_cia.
    withColumn("decisao_operacional", regexp_extract(col("decisao_operacional"), r"(\d+)", 1))
)

#===================================================================================================
# Format the column "cnpj" to "00000000000000"
air_cia = (
    air_cia.
    withColumn("cnpj", regexp_replace(col("cnpj"), r"\D", ""))
    .withColumn("cnpj", lpad(col("cnpj"), 14, "0"))
)

#===================================================================================================
# Insert Into in table air_cia using overwrite mode and partition by ano_mes
db = "bronze"
table = "air_cia"
air_cia.insertInto(f"{db}.{table}", overwrite=True)

# Stop Spark Session
spark.stop()
