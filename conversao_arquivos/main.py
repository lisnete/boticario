import pyodbc as pyodbc
import toml as toml
import tweepy as tw
import pandas as pd

# abrindo e lendo o arquivo com as credenciais e salvando nas variaveis
with open('config.toml') as config:
    config = toml.loads(config.read())
    APP_NAME = config['APP_NAME']
    APP_KEY = config['API_KEY']
    API_KEY_SECRET = config['API_KEY_SECRET']
    ACCESS_TOKEN = config['ACCESS_TOKEN']
    ACCESS_TOKEN_SECRET = config['ACCESS_TOKEN_SECRET']

# autenticando
auth = tw.OAuthHandler(APP_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# construindo a instancia da API
api = tw.API(auth)

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

#vendas = pd.read_csv("/Users/lisnete.miranda/OneDrive - META SERVIÇOS EM INFORMÁTICA LTDA/Documentos/Cursos/GeraisGoogle/APITwitterTweepy/vendas/vendastweetsfl.csv", index_col=0)

#variavel com palavra a ser pesquisada na API e removendo retweets
query_search = "Boticario" + " filter:retweets"

# A linha de código que faz a extração dos tweets
tweets = tw.Cursor(api.search_tweets, q=query_search, lang="pt").items(50)


# transformando resultado dos twuits em uma lista - list
data = [[tweet.user.screen_name, tweet.text, tweet.created_at] for tweet in tweets]

# colocando os dados em um dataframe
df = pd.DataFrame(data, columns=['screen_name', 'text', 'created_at'])

#renomeando as colunas do dataframe
df.columns = ['t_name', 't_text', 't_created_at']

print(df.columns)

# interando no dataframe para pegar cada linha e salvar no banco
for index, row in df.iterrows():
    print(row.t_text)
    #cursor.execute("INSERT INTO dbo.twitter (twitter_username, twitter_text, tweet_created_at) values(?,?,?)", row.t_name, row.t_text, row.t_created_at)
try:
    conexao.commit()
except:
    conexao.rollback()

cursor.close()








