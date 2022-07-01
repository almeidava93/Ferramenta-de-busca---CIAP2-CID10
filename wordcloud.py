import streamlit as st 
import streamlit_wordcloud as wordcloud
from google.cloud import firestore
import pandas as pd

# Source: https://github.com/rezaho/streamlit-wordcloud

#IMPORTANT VARIABLES TO BE USED
service_account_info = st.secrets["gcp_service_account_firestore"]

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

def _max_width_():
    max_width_str = f"max-width: 100%;"
    st.markdown(
        f"""
    <style>
    .reportview-container .main .block-container{{
        {max_width_str}
    }}
    </style>    
    """,
        unsafe_allow_html=True,
    )

st.markdown(
        """
    <style>
    svg {
        height: 100% !important;
        width: 100% !important;
    }
    </style>    
    """,
        unsafe_allow_html=True,
    )


_max_width_()


df = firestore_query(firestore_client = firestore_client, field_paths = ['`text input`'], collection = 'search_history')

series = df.groupby(['text input']).size().sort_values(ascending=False)

words = []
for index, value in series.items():
    word_dict = {"text":index, "value":value}
    words.append(word_dict)

return_obj = wordcloud.visualize(words, tooltip_data_fields={
    'text':'Search', 'value':'N of searches'}, per_word_coloring=False, height="550px", layout='archimedean', font_min=16, palette='plasma', max_words=200)

print('acabou!')