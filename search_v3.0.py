#Inspiration: https://towardsdatascience.com/how-to-build-a-search-engine-9f8ffa405eac
#Next improvements: https://towardsdatascience.com/create-a-simple-search-engine-using-python-412587619ff5

import pandas as pd
from tqdm import tqdm
from rank_bm25 import BM25Okapi
import time
import pickle
from unidecode import unidecode
import streamlit as st

df = pd.read_parquet("text.parquet", engine="pyarrow")

with open('CIAP_CID_indexed_data.pkl', 'rb') as pickle_file:
    bm25 = pickle.load(pickle_file)



st.title('Codificação CIAP2')
with st.container():
    st.header('Ferramenta de busca')
    st.write('Digite abaixo a condição clínica que deseja codificar e nós encontraremos para você os melhores códigos CIAP2.')
    input = st.text_input('Condição clínica ou motivo de consulta:')
    n_results = st.number_input('Quantos códigos devemos mostrar?', value = 5, min_value=1, max_value=20, step=1, key=None, help='help arg')

if input == "":
    pass
else:
    #Querying this index just requires a search input which has also been tokenized:
    input = unidecode(input) #remove acentos e caracteres especiais
    tokenized_query = input.lower().split(" ")

    t0 = time.time()
    results = bm25.get_top_n(tokenized_query, df.text.values, n=n_results)
    t1 = time.time()
    n_records = len(df)
    results = [i for i in results]
    search_time = round(t1-t0,3)

    st.text(f'Searched {n_records} records in {search_time} seconds \n')
    selected_code = st.radio('Códigos encontrados:', results, index=0, key=None, help='help arg')

    #Configuring database
    from google.cloud import firestore
    import uuid
    from datetime import datetime
    import json
    import requests
    from requests.exceptions import HTTPError

    FIREBASE_REST_API = "https://identitytoolkit.googleapis.com/v1/accounts"

    # Use the google verify password REST api to authenticate and generate user tokens
    def sign_in_with_email_and_password(api_key, email, password):
        request_url = "%s:signInWithPassword?key=%s" % (FIREBASE_REST_API, api_key)
        headers = {"content-type": "application/json; charset=UTF-8"}
        data = json.dumps({"email": email, "password": password, "returnSecureToken": True})

        resp = requests.post(request_url, headers=headers, data=data)
        # Check for errors
        try:
            resp.raise_for_status()
        except HTTPError as e:
            raise HTTPError(e, resp.text)

        return resp.json()
    
    #Building the credential object
    from google.oauth2.credentials import Credentials

    # We use the sign_in_with_email_and_password function from https://gist.github.com/Bob-Thomas/49fcd13bbd890ba9031cc76be46ce446
    response = sign_in_with_email_and_password(st.secrets['db_api_key'], st.secrets['db_email'], st.secrets['db_password'])
    # Use google.oauth2.credentials and the response object to create the correct user credentials
    creds = Credentials(response['idToken'], response['refreshToken'])
    
    #Connecting to database
    from google.cloud.firestore import Client

    # using the credentials build in https://gist.github.com/Bob-Thomas/4d9370c6b5432fb5150d3618e0ae71ba
    db = Client("busca-ciap2", creds)
    
    #Saving search history
    ##Relevant variables:
    search_id = 'search_id_' + str(uuid.uuid4()) #id for document name
    datetime = datetime.now() #date and time of search
    
    #input -> text input for code search
    #search_time -> time spent on search
    #n_records -> number of records searched
    #n_results -> number of results shown
    #selected_code -> selected code in radio button
    

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
