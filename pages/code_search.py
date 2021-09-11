import pandas as pd
from tqdm import tqdm
from rank_bm25 import BM25Okapi
import time
import pickle
from unidecode import unidecode
import streamlit as st
from google.cloud import firestore
import uuid
from datetime import datetime as dt
from database import Database

#Custom packages
from database import *
from update_search import *

def app():
    DB = Database()
    db = DB.DB
    df = DB.TESAURO_DF

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
        n_records = Database.N_RECORDS
        results = [i for i in results]
        search_time = round(t1-t0,3)

        st.text(f'Searched {n_records} records in {search_time} seconds \n')
        selected_code = st.radio('Códigos encontrados:', results, index=0, key=None, help='help arg')

        #Saving search history
        ##Relevant variables:
        search_id = 'search_id_' + str(uuid.uuid4()) #id for document name
        datetime = dt.now() #date and time of search
        
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
