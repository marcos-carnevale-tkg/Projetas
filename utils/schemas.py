def vra_schema():
    return StructType([
        StructField("ICAOEmpresaAérea", StringType(), True),
        StructField("NúmeroVoo", StringType(), True),
        StructField("CódigoAutorização", StringType(), True),
        StructField("CódigoTipoLinha", StringType(), True),
        StructField("ICAOAeródromoOrigem", StringType(), True),
        StructField("ICAOAeródromoDestino", StringType(), True),
        StructField("PartidaPrevista", StringType(), True),
        StructField("PartidaReal", StringType(), True),
        StructField("ChegadaPrevista", StringType(), True),
        StructField("ChegadaReal", StringType(), True),
        StructField("SituaçãoVoo", StringType(), True),
        StructField("CódigoJustificativa", StringType(), True)
    ])

def air_cia_schema():
    return StructType([
        StructField("Razão Social", StringType(), True),
        StructField("ICAO IATA", StringType(), True),
        StructField("CNPJ", StringType(), True),
        StructField("Atividades Aéreas", StringType(), True),
        StructField("Endereço Sede", StringType(), True),
        StructField("Telefone", StringType(), True),
        StructField("E-Mail", StringType(), True),
        StructField("Decisão Operacional", StringType(), True),
        StructField("Data Decisão Operacional", StringType(), True),
        StructField("Validade Operacional", StringType(), True)
    ])

def aerodromo_schema():
    return StructType([
    StructField("id", IntegerType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("street_number", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("county", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country_iso", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("uct", IntegerType(), True),
    StructField("website", StringType(), True)
])
