import pandas as pd
import streamlit as st
from datetime import datetime as dt
import uuid
from google.cloud import firestore
from database import Database

def app():
    DB = Database()
    ciap_list = DB.CIAP_LIST
    db = DB.DB
    st.header('Não encontrou o que buscava?')
    with st.form('Não encontrou o que buscava?'):
            st.write('Digite abaixo a expressão que representa a condição que queria encontrar e o código que esperava encontrar. Vamos usar essas informações para melhorar a sua experiência :)')
            text = st.text_input('Termo buscado:')       
            code_expected = st.multiselect('Código(s) esperado(s):', ciap_list)
            sugestion = st.text_input('Tem alguma outra sugestão para nos dar?')
            submitted = st.form_submit_button("Enviar")
            feedback_id = 'feedback_id_' + str(uuid.uuid4()) #id for document name
            datetime = dt.now() #date and time of search
            if submitted:
                st.write('Sua sugestão foi recebida! Obrigado por contribuir!')
                doc_ref = db.collection('feedback').document(feedback_id)
                doc_ref.set({
                    'feedback id': feedback_id,
                    'text input': text,
                    'timestamp': datetime,
                    'expected code': code_expected,
                    'sugestion': sugestion,
                })
                text = None
                code_expected = None
                sugestion = None
            st.write(code_expected)