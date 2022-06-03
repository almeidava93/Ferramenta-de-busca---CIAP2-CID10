import time
import streamlit as st
import uuid
from datetime import datetime as dt

#Custom packages
from database import *


#Funções que renderizam a busca por CID ou CIAP
def cid_search():
    with st.container():
            #st.header('Ferramenta de busca de CID 10')
            st.write('Digite abaixo a condição clínica que deseja codificar. Mostraremos para você os códigos CID10 mais adequados.')
            col1, col2 = st.columns([3,1])
            with col1:
                input = st.text_input('Condição clínica ou motivo de consulta:')
            with col2:
                n_results = st.number_input('Número de resultados mostrados', value = 5, min_value=1, max_value=1000, step=1, key=None, help='Número de resultados mostrados')
    if input != "":
            t0 = time.time()
            selected_code = search_code(input, n_results, data = search_code_data_cid_multiselect, bm25=bm25_cid)
            t1 = time.time()
            n_records = len(search_code_data_cid)
            search_time = round(t1-t0,3)
            st.text(f'Searched {n_records} records in {search_time} seconds \n')
            save_search(input, search_time, n_records, n_results, selected_code, collection_name='search_history_cid')


def ciap_search():
    with st.container():
        #st.header('Ferramenta de busca de CIAP 2')
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
        save_search(input, search_time, n_records, n_results, selected_code, collection_name='search_history')


def app():
    classification_system = st.sidebar.selectbox('Selecione a classificação que deseja utilizar:', options=['CIAP 2', 'CID 10'], index=0)
    st.header(f'Ferramenta de busca de {classification_system}')
    if classification_system=='CIAP 2': ciap_search()
    elif classification_system=='CID 10': cid_search()
