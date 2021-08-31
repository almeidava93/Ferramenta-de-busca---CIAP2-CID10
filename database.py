import pandas as pd
from google.cloud import firestore
import pickle
import streamlit as st

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

def load_data():
    df = pd.read_parquet("text.parquet", engine="pyarrow")
    ciap_list = list(df[['CIAP2_Código1', 'titulo original']].agg(" | ".join, axis=1).drop_duplicates())
    db = firestore.Client.from_service_account_info(service_account_info)
    #db = firestore.Client.from_service_account_json('firestore_key.json')
    with open('CIAP_CID_indexed_data.pkl', 'rb') as pickle_file:
        bm25 = pickle.load(pickle_file)
    search_history = db.collection('search_history').stream()
    return df, ciap_list, db, bm25, search_history

df, ciap_list, db, bm25, search_history = load_data()

class Database():
    TESAURO_DF = df
    CIAP_LIST = ciap_list
    DB = db
    N_RECORDS = len(TESAURO_DF)
    BM25 = bm25
    SEARCH_HISTORY = search_history
