--------------------------------------------------------------create table vra--------------------------------------------------------------
CREATE EXTERNAL TABLE vra(
    icao_empresa_aerea string,
    numero_voo int,
    codigo_autorizacao string,
    codigo_tipo_linha string,
    icao_aerodromo_origem string,
    icao_aerodromo_destino string,
    partida_prevista timestamp,
    partida_real timestamp
    chegada_prevista timestamp
    chegada_real timestamp
    situacao_voo string,
    codigo_justificativa string
) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/data/out/vra';

