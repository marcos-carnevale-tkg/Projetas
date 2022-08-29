# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
from schemas import vra_schema
from credentials import read_ini

# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_vra").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()

dict_data = read_ini(data)

# Set input and output paths
input_path = dict_data["hdfs_path_input_vra"]
output_path = dict_data["hdfs_path_output_vra"]

# Read the file json
schema  = vra_schema()
vra = spark.read.json(input_path, schema=schema)

# Normalize header to snake case
vra = (
        vra.
        withColumnRenamed("ICAOEmpresaAérea", "icao_empresa_aerea").
        withColumnRenamed("NúmeroVoo", "numero_voo").
        withColumnRenamed("CódigoAutorização", "codigo_autorizacao").
        withColumnRenamed("CódigoTipoLinha", "codigo_tipo_linha").
        withColumnRenamed("ICAOAeródromoOrigem", "icao_aerodromo_origem").
        withColumnRenamed("ICAOAeródromoDestino", "icao_aerodromo_destino").
        withColumnRenamed("PartidaPrevista", "partida_prevista").
        withColumnRenamed("PartidaReal", "partida_real").
        withColumnRenamed("ChegadaPrevista", "chegada_prevista").
        withColumnRenamed("ChegadaReal", "chegada_real").
        withColumnRenamed("SituaçãoVoo", "situacao_voo").
        withColumnRenamed("CódigoJustificativa", "codigo_justificativa")
    )

# Insert Into in table vra using overwrite mode and partition by ano_mes
db = "bronze"
table = "vra"
vra.insertInto(f"{db}.{table}", overwrite=True)

# Stop Spark Session
spark.stop()
