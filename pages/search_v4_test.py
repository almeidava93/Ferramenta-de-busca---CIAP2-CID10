import pandas as pd
from google.cloud import firestore
import streamlit as st
from rank_bm25 import BM25Okapi
import spacy
from unidecode import unidecode
import streamlit as st
import uuid
from datetime import datetime as dt
from spellchecker import SpellChecker
import time



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

@st.cache(ttl=None, show_spinner=True)
def join_columns(dataframe, column_names, delimiter=' | ', drop_duplicates=False):
  df = dataframe[column_names].agg(delimiter.join, axis=1)
  if drop_duplicates==True: df.drop_duplicates()
  return df

#Função que remove caracteres especiais de uma coluna de um dataframe
@st.cache(ttl=None, show_spinner=True)
def unidecode_df(dataframe, column_name):
  return dataframe[column_name].apply(lambda x: unidecode(x))


#Função que salva os dados na base de dados
#@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True)
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

df = firestore_query(field_paths=['`CIAP2_Código1`', '`titulo original`','`Termo Português`'])
df['text'] = df['Termo Português'].copy().apply(lambda x: unidecode(x))

@st.cache(hash_funcs={firestore.Client: id}, ttl=86400, show_spinner=True, persist=True)
def bm25_index_(data = df['text']):
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

bm25 = bm25_index_()

def search_code(input, n_results, data = df, bm25=bm25):
    if input != "":
        #Generate search index
        #bm25 = bm25_index()
        #Querying this index just requires a search input which has also been tokenized:
        revised_input = spell_check_input(input) #corrige possíveis erros de grafia no input
        decoded_input = unidecode(revised_input) #remove acentos e caracteres especiais
        tokenized_query = decoded_input.lower().split(" ")
        results = bm25.get_top_n(tokenized_query, data['text'].values, n=n_results)
        results = [i for i in results]
        # Organizing results for visualization and grouping by code
        results_df = pd.DataFrame(columns=df.columns)
        for result in results:
          row = df[df['text']==result]
          results_df = pd.concat([results_df, row])
        results_df = results_df.groupby(['CIAP2_Código1'], as_index = False, sort=False).agg({'titulo original': 'first', 'Termo Português': ' | '.join})
        results_df['CIAP2'] = join_columns(results_df, ['CIAP2_Código1','titulo original'], delimiter=' | ', drop_duplicates=False)
        st.write(f'Resultados encontrados para: **{revised_input}**')
        for row in results_df[['CIAP2', 'Termo Português']].to_numpy().tolist():
          with st.expander(f"{row[0]}"):
            st.write(f"_{row[1]}_")

          #st.write(f"**{row[0]}** - _{row[1]}_")
        return results_df[['CIAP2', 'Termo Português']]

# Prepare spellchecking function:
spell = SpellChecker(language='pt')
def spell_check_input(input):
  revised_input = []
  tokenized_query = input.lower().split(" ")
  for word in tokenized_query:
      # Get the one `most likely` answer
      revised_input.append(spell.correction(word))
  return " ".join(revised_input)

def ciap_search():
    with st.container():
        st.header('Nova versão da busca de CIAP2')
        st.write('Digite abaixo a condição clínica que deseja codificar e nós encontraremos para você os melhores códigos CIAP2.')
        col1, col2 = st.columns([3,1])
        with col1:
            input = st.text_input('Condição clínica ou motivo de consulta:')
        with col2:
            n_results = st.number_input('Número de resultados mostrados', value = 5, min_value=1, max_value=20, step=1, key=None, help='Número de resultados mostrados')
    if input != "":
        t0 = time.time()
        results = search_code(input, n_results)
        t1 = time.time()
        #st.write(f'Resultados encontrados para: **{input}**')
        #st.dataframe(data=results.style.set_properties(**{'text-align': 'left', 'column-width': '500px'}), width=None, height=None)
        n_records = len(df)
        search_time = round(t1-t0,3)
        st.text(f'Searched {n_records} records in {search_time} seconds \n')
        save_search(input, n_records, n_results, results.iloc[0,0], collection_name='search_history')

# Styling
st.markdown("""
<style>
.streamlit-expanderHeader {
  font-weight: bold;
  font-size: 1rem;
}
</style>
""", unsafe_allow_html=True)


#OTHER VARIABLES
def app():
  ciap_search()