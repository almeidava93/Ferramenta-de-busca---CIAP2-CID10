import pandas as pd
from google.cloud import firestore
import pickle
import streamlit as st
from rank_bm25 import BM25Okapi
import spacy
from unidecode import unidecode



#IMPORTANT VARIABLES TO BE USED
service_account_info = {
  "type": st.secrets['type'],
  "project_id": st.secrets['project_id'],
  "private_key_id": st.secrets['private_key_id'],
  "private_key": st.secrets['private_key'],
  "client_email": st.secrets['client_email'],
  "client_id": st.secrets['client_id'],
  "auth_uri": st.secrets['auth_uri'],
  "token_uri": st.secrets['token_uri'],
  "auth_provider_x509_cert_url": st.secrets['auth_provider_x509_cert_url'],
  "client_x509_cert_url": st.secrets['client_x509_cert_url']
}

firebase_storage_config = {
  "apiKey": st.secrets['apiKey'],
  "authDomain": st.secrets['authDomain'],
  "projectId": st.secrets['projectId'],
  "storageBucket": st.secrets['storageBucket'],
  "messagingSenderId": st.secrets['messagingSenderId'],
  "appId": st.secrets['appId'],
  "measurementId": st.secrets['measurementId']
}

firestore_client = firestore.Client.from_service_account_info(service_account_info)


#IMPORTANT FUNCTIONS

"""

def load_data():
    df = pd.read_parquet("text.parquet", engine="pyarrow")
    ciap_list = list(df[['CIAP2_Código1', 'titulo original']].agg(" | ".join, axis=1).drop_duplicates())
    db = firestore.Client.from_service_account_info(service_account_info)
    with open('CIAP_CID_indexed_data.pkl', 'rb') as pickle_file:
        bm25 = pickle.load(pickle_file)
    search_history = db.collection('search_history').stream()
    return df, ciap_list, db, bm25, search_history

#df, ciap_list, db, bm25, search_history = load_data()

class Database():
    TESAURO_DF = df
    CIAP_LIST = ciap_list
    DB = db
    N_RECORDS = len(TESAURO_DF)
    BM25 = bm25
    SEARCH_HISTORY = search_history
    SERVICE_ACCOUNT_INFO = service_account_info

"""

def load():
  #Load dataframe for code search
  firestore_client = firestore.Client.from_service_account_info(service_account_info)
  firestore_collection = firestore_client.collection('tesauro')
  filtered_collection = firestore_collection.select(field_paths=['text', '`text with special characters`'])#Fields containing useful data for search engine
  filtered_collection = filtered_collection.get() #Returns a list of document snapshots, from which data can be retrieved
  filtered_collection_dict = [doc.to_dict() for doc in filtered_collection] #Returns list of dictionaries 
  search_dataframe = pd.DataFrame.from_records(filtered_collection_dict) #Returns dataframe

@st.cache(hash_funcs={firestore.Client: lambda _: None}, ttl=300, show_spinner=True)
def firestore_query(firestore_client = firestore_client, field_paths = [], collection = 'tesauro'):
  #Load dataframe for code search
  firestore_client = firestore.Client.from_service_account_info(service_account_info)
  firestore_collection = firestore_client.collection(collection)
  filtered_collection = firestore_collection.select(field_paths)#Fields containing useful data for search engine
  filtered_collection = filtered_collection.get() #Returns a list of document snapshots, from which data can be retrieved
  filtered_collection_dict = [doc.to_dict() for doc in filtered_collection] #Returns list of dictionaries 
  filtered_collection_dataframe = pd.DataFrame.from_records(filtered_collection_dict) #Returns dataframe
  return filtered_collection_dataframe


#OTHER VARIABLES OF INTEREST
search_code_data = firestore_query(field_paths=['text', '`text with special characters`'])
ciap_df = firestore_query(field_paths=['`CIAP2_Código1`', '`titulo original`']).agg(" | ".join, axis=1).drop_duplicates()
ciap_list = list(ciap_df)

#Função que gera o índice BM25 para a busca e atualiza o arquivo
@st.cache(ttl=300, show_spinner=True)
def bm25_index(data = search_code_data['text']):
    #Launch the language object
    nlp = spacy.load("pt_core_news_lg")
    #Preparing for tokenisation
    text_list = data.str.lower().values
    tok_text=[] # for our tokenised corpus
    #Tokenising using SpaCy:
    for doc in nlp.pipe(text_list, disable=["tagger", "parser","ner"]):
        tok = [t.text for t in doc]
        tok_text.append(tok)
    #Building a BM25 index
    bm25 = BM25Okapi(tok_text)
    return bm25

bm25 = bm25_index()

#Função que retorna o código escolhido
def search_code(input, n_results, data = search_code_data):
    if input != "":
        #Generate search index
        #bm25 = bm25_index()
        #Querying this index just requires a search input which has also been tokenized:
        input = unidecode(input) #remove acentos e caracteres especiais
        tokenized_query = input.lower().split(" ")
        results = bm25.get_top_n(tokenized_query, data.text.values, n=n_results)
        results = [i for i in results]
        selected_code = st.radio('Esses são os códigos que encontramos. Selecione um para prosseguir.', results, index=0, help='Selecione um dos códigos para prosseguir.')
        return selected_code