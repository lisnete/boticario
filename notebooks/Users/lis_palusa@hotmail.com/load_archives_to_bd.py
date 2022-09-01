# Databricks notebook source
# DBTITLE 1,desmonta o ambiente caso esteja montado
#comando usado para desmontar o ponto de montagem olhando para o datalake azure blob
dbutils.fs.unmount(mount_point = "/mnt")

# COMMAND ----------

# DBTITLE 1,configurações

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "a5033566-e917-4521-86c5-93dc64e0f007",
       "fs.azure.account.oauth2.client.secret": "0tH8Q~Gr~APVo9Pusd.WahDlRuqBGEGutYTnEdi5",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b50ad81e-184c-48fa-b8a7-dcc7687933c8/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}


# COMMAND ----------

# DBTITLE 1,monta o ambiente
dbutils.fs.mount(
source = "abfss://boticario@dlakegen2treinamento.dfs.core.windows.net/",
mount_point = "/mnt",
extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,importa bibliotecas necessárias 
#importa as bibliotecas necessárias
import shutil
from os.path import abspath
from datetime import datetime
#import openpyxl
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

# COMMAND ----------

# DBTITLE 1,cria o banco de dados que vamos usar no projeto
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS boticario;

# COMMAND ----------

# DBTITLE 1,define o uso do banco
# MAGIC %sql
# MAGIC USE boticario;

# COMMAND ----------

# DBTITLE 1,cria uma variáveis com path do caminhos dos arquivos que vamos trabalhar
path = '/mnt/dltreinamento/dlakegen2treinamento/boticario'

# path dos arquivos com erro
path_error = f"{path}{'/error'}"

# COMMAND ----------

# DBTITLE 1,lê um arquivo para extrair o schema
#lê um arquivo pra obter o schema
get_schema = spark.read.format("csv").option("inferSchema", "True").option("header", "true").option("maxRowsInMemory",10).load(f"{path}{'/arquivos_venda'}{'/'}{'*.csv'}") 

# COMMAND ----------

# DBTITLE 1,obtém um schema do arquivo lido
# obtem o schema
schema = get_schema.schema 

# COMMAND ----------

# DBTITLE 1,lê arquivos e faz a carga
tem_arquivos = False 

for file in  dbutils.fs.ls(f"{path}{'/arquivos_venda'}"):
        
    error = False
    
    extensao = file.name.split(".")[1]    
    
   
    if extensao == "csv":

        tem_arquivos = True        
        
        try:
            
            df_csv = spark.read.format("csv").option("inferSchema", "True").option("schema", schema).option("header", "true").option("maxRowsInMemory",10).load(f"{path}{'/arquivos_venda'}{'/'}{file.name}")  
            
            #df_final = df_csv[['id_marca', 'marca', 'id_linha', 'linha', 'data_venda', 'qtd_venda']]        
            
            df_csv.createOrReplaceTempView("update_vendas")      
                    
            df_csv.write.mode("overwrite").format("delta").save(f"{path}{'/delta/vendas'}")
            #df_csv.write.mode("overwrite").format("delta").save('mnt/dltreinamento/dlakegen2treinamento/boticario/delta/vendas')
                                   
            write_format = 'delta'            
            save_path = f"{path}{'/delta/vendas'}"
            #save_path = 'dbfs:/mnt/dltreinamento/dlakegen2treinamento/boticario/delta/vendas'
            table_name = 'vendas_boticario'
            
            # Create the table if it does not already exist.           
            spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + save_path + "'")          
   
        except:
            # move o arquivo para a pasta error
            #dbutils.fs.mv(f"{path_landing}{'/'}{file.name}", f"{path_error}{'/'}{file.name}")
            error = True
            break
            

if (tem_arquivos == True and error == True):
    print("Arquivos processados com erro") 
elif (tem_arquivos == True and error == False):
    print("Processamento csv realizado com sucesso") 
else: 
    print("Não há arquivos csv a serem processados")   

# COMMAND ----------

vendas = spark.sql(""" SELECT *
            FROM delta.`dbfs:/mnt/dltreinamento/dlakegen2treinamento/boticario/delta/vendas` v
            """)


# COMMAND ----------

print(vendas.show())