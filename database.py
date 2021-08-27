import pandas as pd
from google.cloud import firestore
import pickle
import streamlit as st

def load_data():
    df = pd.read_parquet("text.parquet", engine="pyarrow")
    ciap_list = list(df[['CIAP2_Código1', 'titulo original']].agg(" | ".join, axis=1).drop_duplicates())
    #db = firestore.Client.from_service_account_info(dict(st.secrets['db_key']))
    db = firestore.Client.from_service_account_json('firestore_key.json')
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