# Databricks notebook source
pip install demoji

# COMMAND ----------

pip install spacy

# COMMAND ----------

!python -m spacy download en_core_web_lg

# COMMAND ----------

!pip install tweepy 

# COMMAND ----------

# DBTITLE 1,importa a biblioteca que extrai dados do twitter
import tweepy

# COMMAND ----------

# DBTITLE 1,cria a variável com o nome  do segredo que contém as chaves de conexão com a API
SECRET_SCOPE = "scopedbricks"

# COMMAND ----------

# DBTITLE 1,configura as variáveis de conexão com a API
#novo
app_key = dbutils.secrets.get(scope = SECRET_SCOPE, key = "app_key")
app_key_secret = dbutils.secrets.get(scope = SECRET_SCOPE, key = "app_key_secret")
access_token = dbutils.secrets.get(scope = SECRET_SCOPE, key = "access_token")
access_token_secret = dbutils.secrets.get(scope = SECRET_SCOPE, key = "access_token_secret")

# COMMAND ----------

# DBTITLE 1,faz a autenticação na API
auth = tweepy.OAuthHandler(app_key, app_key_secret)
auth.set_access_token(access_token, access_token_secret)

# COMMAND ----------

# DBTITLE 1,constrói a instancia da API
api  = tweepy.API(auth)

# COMMAND ----------

# DBTITLE 1,Define o uso do banco
# MAGIC %sql
# MAGIC USE boticario;

# COMMAND ----------

# DBTITLE 1,carrega o dataframe que contém as vendas
# get_vendas_boticario
df_vendas_boticario = spark.sql(""" SELECT *
            FROM delta.`dbfs:/mnt/dltreinamento/dlakegen2treinamento/boticario/delta/vendas` v
            """)

# COMMAND ----------

# DBTITLE 1,cria uma tabela temporária do dataframe
df_vendas_boticario.createOrReplaceTempView("venda")

# COMMAND ----------

# DBTITLE 1,executa uma consulta SQL com dados das vendas e insere em um dataframe
 df_venda_ano_mes = spark.sql("""
              SELECT   v.linha
                       ,sum(v.qtd_venda) as qtd_venda                                         
              FROM venda AS v
WHERE YEAR(v.data_venda) = 2019 AND MONTH(v.data_venda) = 12
              GROUP BY v.linha
              ORDER BY sum(v.qtd_venda) desc  
        """) 

# COMMAND ----------

# DBTITLE 1,pega a primeira linha da ordenação acima
dflinha = df_venda_ano_mes[['linha']].first()

# COMMAND ----------

# DBTITLE 1,cria variável com palavra a ser pesquisada na API / removendo retweets
#query_search = "Boticario " + dflinha['linha']# + " filter:retweets"
query_search = "Boticario " + dflinha['linha'] + " -is:retweet"

# COMMAND ----------

# DBTITLE 1,faz a extração dos tweets na API do Twitter
tweets =  tweepy.Cursor(api.search_tweets, query_search, lang="pt", count=10).items(50)

# COMMAND ----------

# DBTITLE 1,transforma resultado dos twuits em uma lista
#transforma resultado dos twuits em uma lista
data = [[tweet.user.name, tweet.text, tweet.created_at] for tweet in tweets]

# COMMAND ----------

# DBTITLE 1,coloca os dados em um dataframe
#coloca os dados em um dataframe
import pandas as pd
df = pd.DataFrame(data, columns=['name', 'text', 'created_at'])

# COMMAND ----------

# DBTITLE 1,define o nome das colunas
df.columns=['user_name', 'text', 'created_at']

# COMMAND ----------

# DBTITLE 1,remove URLs e emojis
#remove URLs e emojis
import demoji
import re

def cleanTweetText(tweets):
  tweetsText = []
  for i in tweets:
    # remove emojis
    tweet = demoji.replace(i)
    # remove URLs
    tweetNoURL = re.sub(r'http\S+', '', tweet)
    tweetsText.append(tweetNoURL)
  return tweetsText
 
df["text"] = cleanTweetText(df["text"])

# COMMAND ----------

# DBTITLE 1,tokeniza os Tweets
#tokeniza os Tweets para que tenhamos uma lista de tokens (palavras) para cada Tweet. Isso é anexado como uma nova coluna ao nosso Dataframe
import spacy
import string
from spacy.tokenizer import Tokenizer
import string
from string import digits
 
nlp = spacy.load("en_core_web_lg")
tokenizer = Tokenizer(nlp.vocab)
punct = str.maketrans('', '', string.punctuation)
remove_digits = str.maketrans('', '', digits)
 
def getTokens(text):
  tokens = []
  for doc in tokenizer.pipe(text):
    doc_tokens = []    
    for token in doc:
      if token.text.lower().startswith('@') or token.text.lower().startswith('\\') or '_' in token.text.lower() or 'rt' in token.text.lower() or '‰' in token.text.lower():
        continue
      token_no_punc = str(token.text).translate(punct).translate(remove_digits)
      if len(token_no_punc)>1:
        doc_tokens.append(token_no_punc)
    tokens.append(doc_tokens)
  return tokens
 
df["tokens_as_str"] = [' '.join(map(str, i)) for i in getTokens(df["text"])]

# COMMAND ----------

# DBTITLE 1,Remove todas as linhas com valores nulos e retira duplicados no dataframe.
import numpy as np
 
df = df.replace(r'^\s*$', np.nan, regex=True)
df = df.dropna()
df = df.drop_duplicates(subset=['text'])

# COMMAND ----------

# DBTITLE 1,Exibe twittes
display(df)

# COMMAND ----------

updates = spark.createDataFrame(df)
updates.createOrReplaceGlobalTempView("updates")

# COMMAND ----------

updates.write.mode("overwrite").format("delta").save("/mnt/twitter/tweets.delta")

# COMMAND ----------

# DBTITLE 1,cria uma tabela apontando para o caminho dos arquivos criados acima
write_format = 'delta'
save_path = '/mnt/twitter/tweets.delta'
table_name = 'tweets_tokens_raw'
# Create the table if it does not already exist.
spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + save_path + "'")


# COMMAND ----------

# DBTITLE 1,Mostra os twittes gravados na tabela delta
# MAGIC %sql
# MAGIC select * from delta.`/mnt/twitter/tweets.delta` limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO boticario.tweets_tokens_raw AS raw
# MAGIC USING global_temp.updates
# MAGIC ON raw.text = updates.text
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *