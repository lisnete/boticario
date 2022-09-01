from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyodbc as pyodbc
import os
import functions


# building main
if __name__ == '__main__':
    # init spark session
    spark = SparkSession \
        .builder \
        .appName("etl-load-local-py") \
        .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
        .enableHiveSupport() \
        .getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # seta o nivel do log, no nosso caso INFO
    spark.sparkContext.setLogLevel("INFO")

    # credenciais do banco de dados SQL Server
    dados_conexao = (
        "Driver={SQL Server};"
        "Server=GMPNC002023\INTROSQLSERVER;"
        "Database=twitter;"
    )

    # faz a conexão com o banco de dados
    conexao = pyodbc.connect(dados_conexao)

    # cria o cursor object da conexão
    cursor = conexao.cursor()

    # variavel path com o caminho dos arquivos a serem processados
    path = '/Users/lisnete.miranda/PycharmProjects/APITwitterTweepy/arquivos'

    # parth dos arquivos landing (prontos para serem processados)
    path_landing = f"{path}{'/landing'}"

    # parth dos arquivos já processados
    path_processed = f"{path}{'/processed'}"

    # parth dos arquivos com erro
    path_error = f"{path}{'/error'}"

    # ----------------------------------------------------------------------------------------------
    # 1 - analisa se existem arquivos para serem processados
    # ----------------------------------------------------------------------------------------------

    # obtem a lista dos nomes dos arquivos do diretorio path criado acima
    lista_arquivos = os.listdir(path_landing)

    # verifica se o diretorio não está vazio, caso não esteja, segue processo
    if len(lista_arquivos) != 0:

        # lê um arquivo para obter o schema
        struct_schema = spark.read \
            .option("header", "true") \
            .format("csv") \
            .option("sep", ",") \
            .load(f"{path_landing}{'/*.csv'}")

        # obtem o schema do arquivo lido acima e guarda em uma variavel
        schema_load = struct_schema.schema

        # lê os arquivos do caminho acima e insere em um dataframe
        df_files = spark.read \
            .format("csv") \
            .schema(schema_load) \
            .option("header", "true") \
            .option("sep", ",") \
            .load(f"{path_landing}{'/*.csv'}")

        # lê o df com os dados dos arquivos lidos e salva no banco de dados
        insert = False

        for index, row in df_files.toPandas().iterrows():
            try:
                marca = functions.remove_combine_regex(row.MARCA)

                linha = functions.remove_combine_regex(row.LINHA)

                cursor.execute("INSERT INTO dbo.venda_boticario (ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA) values(?,?,?,?,?,?)", row.ID_MARCA, marca, row.ID_LINHA, linha, row.DATA_VENDA, row.QTD_VENDA)

                insert = True

            except:
                break

        if insert == True:

            # comita o processo no banco de dados
            conexao.commit()


            for arquivo in lista_arquivos:

                # move os arquivos que foram lidos para a pasta (processed)
                os.rename(f"{path_landing}{'/'}{arquivo}", f"{path_processed}{'/'}{arquivo}")

                print('Os arquivos foram processados com sucesso e foram movidos para a pasta processed')

        else:

            conexao.rollback()

            for arquivo in lista_arquivos:

                # move os arquivos para a pasta (error)
                os.rename(f"{path_landing}{'/'}{arquivo}", f"{path_error}{'/'}{arquivo}")

            print('Os arquivos foram processados com erro. Por favor, analise a pasta error')

    else:
        print('Não há arquivos a serem processados')

    # stop spark session
    spark.stop()













