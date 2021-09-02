import pandas as pd
from google.cloud import firestore
import pickle
import streamlit as st
import pyrebase


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

def load_data():
    df = pd.read_parquet("text.parquet", engine="pyarrow")
    ciap_list = list(df[['CIAP2_Código1', 'titulo original']].agg(" | ".join, axis=1).drop_duplicates())
    db = firestore.Client.from_service_account_info(service_account_info)
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
    SERVICE_ACCOUNT_INFO = service_account_info
