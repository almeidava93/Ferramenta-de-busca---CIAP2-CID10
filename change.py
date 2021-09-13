import pandas as pd
from google.cloud import firestore
import streamlit as st
import uuid
from tqdm import tqdm

from database import *


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

firestore_database_client = firestore.Client.from_service_account_info(service_account_info)
tesauro = pd.read_parquet("/Users/viniciusanjosdealmeida/Documents/GitHub/Busca-CID10/cid10_df_search.parquet", engine="pyarrow")


def load_tesauro_dataframe(tesauro, firestore_database_client):
    tesauro_dict = tesauro.to_dict('records')
    for row in tqdm(tesauro_dict):
        id = str(uuid.uuid4())
        condition_id = "_".join(["condition_id", id])
        doc_ref = firestore_database_client.collection('tesauro_cid').document(condition_id)
        doc_ref.set(row)



load_tesauro_dataframe(tesauro, firestore_database_client)


"""
doc_ref = firestore_database_client.collection('tesauro').select(field_paths=['`CID10_Código1`']).get()
data = [doc.to_dict() for doc in doc_ref]
print(data)
df = pd.DataFrame.from_records(data)
print(df)


"/Users/viniciusanjosdealmeida/Documents/GitHub/Busca-CID10/cid10_df_search.parquet"



    df = df.to_dict('records')
    row_of_interest = row_of_interest.to_dict('records')
    df = df + row_of_interest
    df = pd.DataFrame.from_records(df)


        ##Saving data:
        doc_ref = db.collection('search_history').document(search_id)
        doc_ref.set({
            'search id': search_id,
            'text input': input,
            'timestamp': datetime,
            'search time': search_time,
            'n records searched': n_records,
            'n results shown': n_results,
            'selected code': selected_code
        })
"""
