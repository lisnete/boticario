# Databricks notebook source
teste salvando os dados no git

# COMMAND ----------

#comando usado para desmontar o ponto de montagem olhando para o datalake azure blob
dbutils.fs.unmount(mount_point = "/mnt")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "a5033566-e917-4521-86c5-93dc64e0f007",
       "fs.azure.account.oauth2.client.secret": "0tH8Q~Gr~APVo9Pusd.WahDlRuqBGEGutYTnEdi5",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b50ad81e-184c-48fa-b8a7-dcc7687933c8/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# COMMAND ----------

dbutils.fs.mount(
source = "abfss://boticario@dlakegen2treinamento.dfs.core.windows.net/",
mount_point = "/mnt",
extra_configs = configs)

# COMMAND ----------

#Comando para listar como está os arquivos dentro do dalatake (/mnt)
dbutils.fs.ls('/mnt/')

# COMMAND ----------

# (get_vendas_boticario)
df_vendas_boticario = spark.sql(""" SELECT *
            FROM delta.`dbfs:/mnt/dltreinamento/dlakegen2treinamento/boticario/delta/vendas` v
            """)

# COMMAND ----------

df_vendas_boticario.createOrReplaceTempView("venda")

# COMMAND ----------

# DBTITLE 1,VENDAS CONSOLIDADAS POR ANO E MES
 df_venda_ano_mes = spark.sql("""
              SELECT  YEAR(v.data_venda) as ano 
                      ,MONTH(v.data_venda) as mes 
                      ,sum(v.qtd_venda) as qtde_venda                      
              FROM venda AS v
              GROUP BY MONTH(v.data_venda),YEAR(v.data_venda)
              ORDER BY YEAR(v.data_venda),MONTH(v.data_venda) asc       
        """) 

# COMMAND ----------

display(df_venda_ano_mes)

# COMMAND ----------

# DBTITLE 1,VENDAS CONSOLIDADAS POR MARCA E LINHA
 df_venda_marca_linha = spark.sql("""
              SELECT   v.marca as marca  
                      ,v.linha as linha   
                      ,sum(v.qtd_venda) as qtde_venda                      
              FROM venda AS v
              GROUP BY v.marca, v.linha
              ORDER BY v.marca, v.linha asc      
        """) 

# COMMAND ----------

display(df_venda_marca_linha)

# COMMAND ----------

# DBTITLE 1,VENDAS CONSOLIDADAS POR MARCA, ANO E MES
 df_venda_marca_ano_mes = spark.sql("""
              SELECT   v.marca as marca  
                      ,YEAR(v.data_venda) as ano
                      ,MONTH(v.data_venda) as mes
                      ,sum(v.qtd_venda) as qtde_venda                      
              FROM venda AS v
              GROUP BY v.marca, YEAR(v.data_venda), MONTH(v.data_venda)
              ORDER BY YEAR(v.data_venda), v.marca, MONTH(v.data_venda) asc      
        """) 

# COMMAND ----------

display(df_venda_marca_ano_mes)

# COMMAND ----------

# DBTITLE 1,VENDAS CONSOLIDADAS POR LINHA, ANO E MES
 df_venda_linha_ano_mes = spark.sql("""
              SELECT   v.linha as linha  
                      ,YEAR(v.data_venda) as ano
                      ,MONTH(v.data_venda) as mes
                      ,sum(v.qtd_venda) as qtde_venda                      
              FROM venda AS v
              GROUP BY v.linha, YEAR(v.data_venda), MONTH(v.data_venda)
              ORDER BY YEAR(v.data_venda), v.linha, MONTH(v.data_venda) asc      
        """) 

# COMMAND ----------

display(df_venda_linha_ano_mes)