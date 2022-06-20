import pandas as pd
from google.cloud import firestore
import streamlit as st
from rank_bm25 import BM25Okapi
import spacy
from unidecode import unidecode
import streamlit as st
import uuid
from datetime import datetime as dt



#IMPORTANT VARIABLES TO BE USED
service_account_info = st.secrets["gcp_service_account_firestore"]
firebase_storage_config = st.secrets["gcp_service_account"]


@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True)
def load_firestore_client(service_account_info = service_account_info):
  firestore_client = firestore.Client.from_service_account_info(service_account_info)
  return firestore_client

firestore_client = load_firestore_client() #Carrega a conexão com a base de dados com cache.



@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True, allow_output_mutation=True)
def firestore_query(firestore_client = firestore_client, field_paths = [], collection = 'tesauro'):
  #Load dataframe for code search
  firestore_collection = firestore_client.collection(collection)
  filtered_collection = firestore_collection.select(field_paths)#Fields containing useful data for search engine
  filtered_collection = filtered_collection.get() #Returns a list of document snapshots, from which data can be retrieved
  filtered_collection_dict = [doc.to_dict() for doc in filtered_collection] #Returns list of dictionaries 
  filtered_collection_dataframe = pd.DataFrame.from_records(filtered_collection_dict) #Returns dataframe
  return filtered_collection_dataframe



#OTHER VARIABLES OF INTEREST
search_code_data = firestore_query(field_paths=['text', '`text with special characters`'])
search_code_data_cid = firestore_query(field_paths=['CAT', 'DESCRICAO', '`Termo Português`'], collection='tesauro_cid')

ciap_df = firestore_query(field_paths=['`CIAP2_Código1`', '`titulo original`']).agg(" | ".join, axis=1).drop_duplicates()
ciap_list = list(ciap_df)



#Função que gera o índice BM25 para a busca e atualiza o arquivo
@st.cache(ttl=None, show_spinner=True)
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


#OTHER VARIABLES
bm25 = bm25_index()


#Função que retorna o código escolhido
def search_code(input, n_results, data = search_code_data, bm25=bm25):
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

@st.cache(ttl=None, show_spinner=True)
def join_columns(dataframe, column_names, delimiter=' | ', drop_duplicates=False):
  df = dataframe[column_names].agg(delimiter.join, axis=1)
  if drop_duplicates==True: df.drop_duplicates()
  return df


#Função que remove caracteres especiais de uma coluna de um dataframe
@st.cache(ttl=None, show_spinner=True)
def unidecode_df(dataframe, column_names):
  return dataframe[column_names].apply(lambda x: unidecode(x))


#Preparing data for CID10 search
##Create dataframe and a merged column for a multiselect option list
search_code_data_cid_multiselect = pd.DataFrame()
search_code_data_cid_multiselect['text'] = join_columns(search_code_data_cid, column_names=['CAT', 'DESCRICAO', 'Termo Português'])
##Create a column with the same data, but unidecoded, for bm25 index creation
search_code_data_cid['text'] = unidecode_df(search_code_data_cid_multiselect, column_names='text')
##Create bm25 index for CID10 search
bm25_cid = bm25_index(data = search_code_data_cid['text'])


#Função que salva os dados na base de dados
@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True)
def save_search(text_input, n_records, n_results, selected_code, collection_name, firestore_client=firestore_client):
  #input -> text input for code search
  #n_records -> number of records searched
  #n_results -> number of results shown
  #selected_code -> selected code in radio button
  
  search_id = 'search_id_' + str(uuid.uuid4()) #id for document name
  datetime = dt.now() #date and time of search
  
  ##Saving data:
  doc_ref = firestore_client.collection(collection_name).document(search_id)
  doc_ref.set({
            'search id': search_id,
            'text input': text_input,
            'timestamp': datetime,
            'n records searched': n_records,
            'n results shown': n_results,
            'selected code': selected_code
        })
        

