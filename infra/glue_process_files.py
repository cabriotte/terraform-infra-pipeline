import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, min, max, concat_ws, current_timestamp, from_utc_timestamp, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from awsglue.dynamicframe import DynamicFrame

# Inicialização do contexto do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminho do arquivo Parquet no S3
s3_path = "s3://dev-challenge2-files/b3/pregao/"
df = spark.read.parquet(s3_path)

# Renomeando as colunas
df = df.withColumnRenamed("Código", "Codigo") \
       .withColumnRenamed("Ação", "Acao") \
       .withColumnRenamed("Qtde. Teórica", "QtdTeorica") \
       .withColumnRenamed("Part. (%)", "ParticipacaoPorcentagem")

# Conta total de registros do DataFrame
total_registros = df.count()
df = df.withColumn("TotalRegistros", lit(total_registros))

# Encontrar valor mínimo e máximo de QtdTeorica
window_spec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
min_val = min("ParticipacaoPorcentagem").over(window_spec)
max_val = max("ParticipacaoPorcentagem").over(window_spec)

# Filtrar linhas com os valores mínimo e máximo
df_min = df.withColumn("min_val", min_val).filter(col("ParticipacaoPorcentagem") == col("min_val")) \
           .select("acao", "ParticipacaoPorcentagem").limit(1)
df_max = df.withColumn("max_val", max_val).filter(col("ParticipacaoPorcentagem") == col("max_val")) \
           .select("acao", "ParticipacaoPorcentagem").limit(1)

# Coletar os valores para uso em concatenação
min_row = df_min.collect()[0]
max_row = df_max.collect()[0]
min_concat = f"{min_row['acao']}-{min_row['ParticipacaoPorcentagem']}"
max_concat = f"{max_row['acao']}-{max_row['ParticipacaoPorcentagem']}"

# Adicionar essas colunas ao DataFrame original
df = df.withColumn("ParticipacaoPorcentagemMin", lit(min_concat)) \
       .withColumn("ParticipacaoPorcentagemMax", lit(max_concat))

# Adiciona coluna com timestamp atual e particao 
df = df.withColumn("data_carga", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))
df = df.withColumn("anomesdia", date_format(col("data_carga"), "yyyyMMdd"))

# Define o nome da tabela e caminho de saída no S3
tabela_nome = "tb_b3_acoes_pregao"
s3_output_path = "s3://dev-challenge2-files/b3/refined/pregao/tb_b3_acoes_pregao/"

# Converte para DynamicFrame
df_dyf = DynamicFrame.fromDF(df, glueContext, "df_dyf")

# Define o caminho de saída e configura o sink
sink = glueContext.getSink(
    path = s3_output_path,
    connection_type = "s3",
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys = ["anomesdia"],
    compression = "snappy",
    enableUpdateCatalog = True,
    transformation_ctx = "datasink"
)

# Define o catálogo Glue
sink.setCatalogInfo(
    catalogDatabase = "default",
    catalogTableName = "tb_b3_acoes_pregao"
)

# Define o formato de saída
sink.setFormat("glueparquet")

# Escreve os dados
sink.writeFrame(df_dyf)

# Finalizando o job
job.commit()