from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf,  trim, size, split
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Iniciar uma sessão Spark
spark = SparkSession.builder \
    .appName("Analise Reviews IMDB") \
    .getOrCreate()
# Configurando logs para facilitar a leitura
spark.sparkContext.setLogLevel('WARN')

# Definir o schema manualmente
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("text_en", StringType(), nullable=False),
    StructField("text_pt", StringType(), nullable=False),
    StructField("sentiment", StringType(), nullable=False)
])

# Carregar o dataset
df = spark.read.csv("hdfs://namenode:8020/imdb-reviews-pt-br.csv", header=True, schema=schema, quote="\"",
escape="\"", encoding="UTF-8")

# Remover espaços em branco e caracteres especiais
df = df.withColumn("text_en", trim(col("text_en"))) \
       .withColumn("text_pt", trim(col("text_pt"))) \
       .withColumn("sentiment", trim(col("sentiment")))

# Mostrando RU
RU = 4232643
print(f"Meu RU: {RU}")

# Realizar uma consulta simples (exemplo: contar o número de linhas)
row_count = df.count()
print(f"Número de linhas no dataset: {row_count}")


# PRIMEIRA QUESTAO
# Mostrando RU
RU = 4232643
print(f"Meu RU: {RU}")

# Verificar valores nulos ou inválidos
print("Valores nulos na coluna 'id':")
df.filter(col("id").isNull()).show()

print("Valores nulos na coluna 'sentiment':")
df.filter(col("sentiment").isNull()).show()

print("Valores únicos na coluna 'sentiment':")
df.select("sentiment").distinct().show()

# Converter a coluna 'id' para IntegerType
df = df.withColumn("id", col("id").cast(IntegerType()))

# Filtrar as linhas onde o sentimento é negativo
df_neg = df.filter((col("sentiment") == "neg") & (col("id").isNotNull()))

# Filtrar as linhas onde o sentimento é positivo
df_pos = df.filter((col("sentiment") == "pos") & (col("id").isNotNull()))

# Filtrar as linhas onde o sentimento é negativo
df_neg = df.filter(col("sentiment") == "neg")

# Filtrar as linhas onde o sentimento é positivo
df_pos = df.filter(col("sentiment") == "pos")

# Soma de todos os IDs
soma_ids_total = df.agg({"id": "sum"}).collect()[0][0]
print(f"Soma dos IDs: {soma_ids_total}")

# Calcular a soma dos IDs positivos
soma_ids_pos = df_pos.agg({"id": "sum"}).collect()[0][0]
print(f"Soma dos IDs com sentimento positivo: {soma_ids_pos}")

# Calcular a soma dos IDs negativos
soma_ids_neg = df_neg.agg({"id": "sum"}).collect()[0][0]
print(f"Soma dos IDs com sentimento negativo: {soma_ids_neg}")

# Validando se os numeros estao corretos
print(f"A soma dos IDs positivos e negativos é {soma_ids_pos + soma_ids_neg}, deve ser igual a {soma_ids_total}")


# SEGUNDA QUESTAO:
# Mostrando RU
RU = 4232643
print(f"Meu RU: {RU}")

# Criando funcao para contar palavras
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
df_neg = df_neg.withColumn("diff_palavras", col("palavras_pt") - col("palavras_en"))

# Soma das diferenças
diff_total = df_neg.agg({"diff_palavras": "sum"}).collect()[0][0]
print(f"Diferença total de palavras (text_pt - text_en) com sentimento negativo: {diff_total}")
