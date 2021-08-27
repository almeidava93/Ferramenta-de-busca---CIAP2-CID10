import pandas as pd
from google.cloud import firestore
import pickle

class Database():
    TESAURO_DF = pd.read_parquet("text.parquet", engine="pyarrow")
    CIAP_LIST = list(TESAURO_DF[['CIAP2_Código1', 'titulo original']].agg(" | ".join, axis=1).drop_duplicates())
    DB = firestore.Client.from_service_account_json("firestore_key.json")
    search_history = DB.collection('search_history').stream()
    search_history_list = [x for x in search_history]
    SEARCH_COUNTER = len(search_history_list)
    N_RECORDS = len(TESAURO_DF)

    with open('CIAP_CID_indexed_data.pkl', 'rb') as pickle_file:
        bm25 = pickle.load(pickle_file)
    BM25 = bm25