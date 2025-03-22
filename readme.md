Trabalho acadêmico realizado utilizando cluster de Spark, conteinerizado utilizando Docker. A planilha utilizada neste trabalho está disponível no seguinte link: https://www.kaggle.com/datasets/luisfredgs/imdb-ptbr/data

Como Executar o Projeto

    Instale o Docker:

        Certifique-se de que o Docker e o Docker Compose estão instalados na sua máquina. Você pode baixá-los em docker.com.

    Construa o Projeto:

        No terminal, navegue até a pasta do projeto e execute o seguinte comando para construir as imagens Docker:
        bash
        Copy

        docker-compose build

    Personalize o Script de Análise:

        Edite o arquivo analise.py conforme necessário para atender às suas necessidades de pesquisa.

    Inicie o Cluster:

        Execute o seguinte comando para iniciar o cluster Spark e HDFS:
        bash
        Copy

        docker-compose up -d

    Carregue os Dados no HDFS:

        Copie o arquivo CSV para o HDFS usando o comando:
        bash
        Copy

        docker exec namenode hdfs dfs -put /app/nome-do-seu-arquivo.csv /nome-do-seu-arquivo.csv

        Para verificar se o arquivo foi carregado corretamente, use:
        bash
        Copy

        docker exec namenode hdfs dfs -ls /

    Envie a Tarefa para o Cluster:

        Execute o script de análise no cluster Spark com o comando:
        bash
        Copy

        docker exec spark-master spark-submit --master spark://spark-master:7077 /app/analise.py

    Reiniciar o Cluster (Opcional):

        Caso precise alterar o script analise.py, pare o cluster com:
        bash
        Copy

        docker-compose down

        Em seguida, inicie o cluster novamente:
        bash
        Copy

        docker-compose up -d


Dicas

    Acompanhe o Progresso:

        Acesse a UI do Spark em http://localhost:8080 para monitorar a execução dos jobs.

        Acesse a UI do HDFS em http://localhost:9870 para verificar os arquivos armazenados.

    Persistência de Dados:

        Se precisar salvar os resultados da análise, use o HDFS ou um volume Docker para persistir os dados.

    Escalabilidade:

        Para datasets maiores, aumente o número de workers no docker-compose.yml (ex: scale: 4).