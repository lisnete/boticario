# batching application running locally
# import libraries
import shutil
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyodbc as pyodbc
from datetime import datetime
import openpyxl
import os

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

    # cria variavel path com o caminho dos arquivos a serem lidos
    path = '/Users/lisnete.miranda/PycharmProjects/APITwitterTweepy/arquivos'

    # obterndo a data e hora atual para inserir no nome do arquivo
    data_hora_atual = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')

    # path dos arquivos csv
    path_csv = f"{path}{'/csv'}"

    # path dos arquivos xlsx
    path_xlsx = f"{path}{'/xlsx'}"

    # path do arquivo dos arquivos slsx
    path_archive_xlsx = f"{path}{'/archive/xlsx'}"

    # path do arquivo dos arquivos csv
    path_archive_csv = f"{path}{'/archive/csv'}"

    # parth dos arquivos landing (prontos para serem processados)
    path_landing = f"{path}{'/landing'}"

    # path dos arquivos processados
    path_processed = f"{path}{'/processed'}"

    tem_arquivos = False
    # --------------------------------------------------------------------------------------------------------
    # 1 - analisa se existe arquivo csv - path para csv (renomeia com nome padrao e cria backup em archive/csv
    # --------------------------------------------------------------------------------------------------------
    for file in os.listdir(path_csv):

        if file.endswith(".csv"):

            tem_arquivos = True

            # obtendo nome do arquivo para usa-lo no nome do csv convertido
            basename = os.path.basename(f"{path_csv}{'/'}{file}")

            # gera o nome do arquivo concatenando nome + data_hora_atual
            nome_arquivo = f"{os.path.splitext(basename)[0]}{'_'}{data_hora_atual}{'.csv'}"

            # retira espaco, :, -  do nome di arquivo
            nome_arquivo = nome_arquivo.replace(" ", "_").replace("-", "_").replace(":", "_")

            # renomeia o arquivo para formato padrao de nome_arquivo+data_hora
            os.rename(f"{path_csv}{'/'}{file}", f"{path_csv}{'/'}{nome_arquivo}")

            # faz uma copia do arquivo csv para a pasta archive/csv
            shutil.copy2(f"{path_csv}{'/'}{nome_arquivo}", f"{path_archive_csv}{'/'}{nome_arquivo}")

            # move o arquivo para a pasta landed
            os.rename(f"{path_csv}{'/'}{nome_arquivo}", f"{path_landing}{'/'}{nome_arquivo}")

    # -----------------------fim analise arquivos csv--------------------------------------------------------------



    # --------------------------------------------------------------------------------------------------------
    # 1 - analisa se existe arquivo xlsx - path xlsx
    # --------------------------------------------------------------------------------------------------------
    for file in os.listdir(path_xlsx):

        # if xlsx inicia processo de conversão do arquivo para csv e disponibiliza na pasta landing
        if file.endswith(".xlsx"):

            tem_arquivos = True

            xlsx = openpyxl.load_workbook(f"{path_xlsx}{'/'}{file}")

            # opening the active sheet
            sheet = xlsx.active

            # getting the data from the sheet
            data = sheet.rows

            # obtendo nome do arquivo para usa-lo no nome do csv convertido
            basename = os.path.basename(f"{path_xlsx}{'/'}{file}")

            # gera o nome do arquivo concatenando nome + data_hora_atual
            nome_arquivo = f"{os.path.splitext(basename)[0]}{'_'}{data_hora_atual}"

            nome_arquivo = nome_arquivo.replace(" ", "_").replace("-", "_").replace(":", "_")
            
            try:
                arquivo_csv = open(f"{path_landing}{'/'}{nome_arquivo}"".csv", "w+", newline="", encoding="utf-8")

                for row in data:
                    l = list(row)
                    for i in range(len(l)):
                        if i == len(l) - 1:
                            arquivo_csv.write(str(l[i].value))
                        else:
                            arquivo_csv.write(str(l[i].value) + ',')
                    arquivo_csv.write('\n')

                # fecha o arquivo
                arquivo_csv.close()

                # após criar o arquivo csv, move o xls para a pasta archive/xls
                os.rename(f"{path_xlsx}{'/'}{file}", f"{path_archive_xlsx}{'/'}{file}")
            except:
                break

    if tem_arquivos:
        print("Processamento de conversao e rename realizado")
    else:
        print("Não há arquivos a serem convertidos ou renomeados")

    # -----------------------fim analise arquivos xlsx-------------------------------------

    # stop spark session
    spark.stop()


