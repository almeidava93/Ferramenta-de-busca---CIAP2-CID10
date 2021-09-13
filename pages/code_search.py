import time
import streamlit as st
import uuid
from datetime import datetime as dt

#Custom packages
from database import *

def app():
    with st.container():
        st.header('Ferramenta de busca')
        st.write('Digite abaixo a condição clínica que deseja codificar e nós encontraremos para você os melhores códigos CIAP2.')
        col1, col2 = st.columns([3,1])
        with col1:
            input = st.text_input('Condição clínica ou motivo de consulta:')
        with col2:
            n_results = st.number_input('Número de resultados mostrados', value = 5, min_value=1, max_value=20, step=1, key=None, help='Número de resultados mostrados')

    if input != "":
        t0 = time.time()
        selected_code = search_code(input, n_results)
        t1 = time.time()
        n_records = len(search_code_data)
        search_time = round(t1-t0,3)

        st.text(f'Searched {n_records} records in {search_time} seconds \n')

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
        doc_ref = firestore_client.collection('search_history').document(search_id)
        doc_ref.set({
            'search id': search_id,
            'text input': input,
            'timestamp': datetime,
            'search time': search_time,
            'n records searched': n_records,
            'n results shown': n_results,
            'selected code': selected_code
        })
