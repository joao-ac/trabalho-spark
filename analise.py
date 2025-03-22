from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# Iniciar uma sessão Spark
spark = SparkSession.builder \
    .appName("Analise Reviews IMDB") \
    .getOrCreate()
# Configurando logs para facilitar a leitura
spark.sparkContext.setLogLevel('WARN')

# Carregar o dataset
df = spark.read.csv("hdfs://namenode:8020/imdb-reviews-pt-br.csv", header=True, inferSchema=True)

# Exibir o schema do dataset
df.printSchema()

# Realizar uma consulta simples (exemplo: contar o número de linhas)
row_count = df.count()
print(f"Número de linhas no dataset: {row_count}")


# PRIMEIRA QUESTAO

# Filtrar as linhas onde o sentimento é negativo
df_neg = df.filter(col("sentiment") == "neg")

# Calcular a soma dos IDs
soma_ids_neg = df_neg.agg({"id": "sum"}).collect()[0][0]
print(f"Soma dos IDs com sentimento negativo: {soma_ids_neg}")


# SEGUNDA QUESTAO:
def contar_palavras(texto):
    if texto is None:
        return 0
    return len(texto.split())

# Registrar a função como UDF (User Defined Function)
contar_palavras_udf = udf(contar_palavras, IntegerType())

# Adicionar colunas com a contagem de palavras
df_neg = df_neg.withColumn("palavras_en", contar_palavras_udf(col("text_en"))) \
               .withColumn("palavras_pt", contar_palavras_udf(col("text_pt")))

# Calcular a diferença de palavras
df_neg = df_neg.withColumn("diff_palavras", col("palavras_en") - col("palavras_pt"))

# Soma das diferenças
diff_total = df_neg.agg({"diff_palavras": "sum"}).collect()[0][0]
print(f"Diferença total de palavras (text_en - text_pt) com sentimento negativo: {diff_total}")
