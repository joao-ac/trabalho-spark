# Usar a imagem do Bitnami Spark
FROM bitnami/spark:latest

# Definir o diretório de trabalho
WORKDIR /app

# Copiar o dataset e scripts para o contêiner
COPY . /app