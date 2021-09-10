"""
Done:
1. Acessar dataframe principal
2. Acessar linha mais próxima da que se deseja adicionar
3. Copiar essa linha e editar para colocar a expressão que se deseja
4. Gerar novo índice bm25 para busca
5. Salvar arquivos na base de dados

To do:
6. Eliminar duplicatas
7. Corrigir páginas para que tudo acesse as informações direto da base de dados e não mais dos arquivos parquet e pkl
8. Implementar Busca CID
"""

import streamlit as st
import pandas as pd
from unidecode import unidecode
from rank_bm25 import BM25Okapi
import spacy
import uuid

#Custom packages
from database import *


#Função que retorna o código escolhido
def search_code(input, n_results, data = search_code_data):
    if input != "":
        #Generate search index
        #bm25 = bm25_index()
        #Querying this index just requires a search input which has also been tokenized:
        input = unidecode(input) #remove acentos e caracteres especiais
        tokenized_query = input.lower().split(" ")
        results = bm25.get_top_n(tokenized_query, data.text.values, n=n_results)
        results = [i for i in results]
        selected_code = st.radio('Esses são os códigos que encontramos. Selecione um para prosseguir.', results, index=0, help='Selecione um dos códigos para prosseguir.')
        return selected_code


def where(collection_name='tesauro', field_path='`Termo Português`', op_string='==', firestore_client = firestore_client, value=None):
    #Função que retorna uma lista com os documentos de uma coleção no firebase conforme as condições de busca
    firestore_collection = firestore_client.collection(collection_name) #Acessa tesauro no firestore
    doc_ref = firestore_collection.where(value=value, field_path=field_path, op_string=op_string) #Encontra o documento mais próximo
    doc_list = doc_ref.get() #Devolve uma lista contendo os document snapshots
    doc_list = [doc.to_dict() for doc in doc_list] #Transforma em lista de dicionários no formato {'field':'value'}
    return doc_list


def check_duplicates(collection_name='tesauro', field_path='`Termo Português`', op_string='==', firestore_client=firestore_client, value=None):
    #Função que retorna True quando já existe um termo idêntico registrado no tesauro e False para quando não existe
    doc_list = where(collection_name=collection_name, field_path=field_path, op_string=op_string, value=value, firestore_client=firestore_client)
    if len(doc_list)>0:
        return True
    else:
        return False


#Função que atualiza o tesauro, o índice de busca para expansão e aumento da precisão da busca
def update_search(new_description, code_reference, firestore_client = firestore_client):
    #Checar se já existe um registro com a mesma descrição no banco de dados. Se já existir, comunicar o usuário e encerrar.
    if check_duplicates(value=new_description):
        st.info('Já existe uma descrição idêntica registrada em nosso tesauro :)')
    else:
        #Acessar registro no tesauro mais próximo do item que se deseja atualizar
        firestore_collection = firestore_client.collection('tesauro') #Acessa tesauro no firestore
        tesauro_reference = firestore_collection.where(field_path='text', op_string='==', value=code_reference) #Encontra o documento mais próximo
        tesauro_reference = tesauro_reference.get() #Devolve uma lista contendo os document snapshots
        tesauro_reference = [doc.to_dict() for doc in tesauro_reference] #Transforma em lista de dicionários no formato {'field':'value'}
        tesauro_reference = pd.DataFrame.from_records(tesauro_reference) #Transforma dados em um dataframe para permitir a atualização que se deseja
        
        #Copiar linha de interesse e atualizar com a nova descrição adicionada
        row_of_interest = tesauro_reference.iloc[[0]] #Seleciona a linha com os dados
        row_of_interest['Termo Português'] = new_description #descrição que eu quero adicionar para o código em questão
        row_of_interest['text with special characters'] = row_of_interest[['CIAP2_Código1', 'titulo original','Termo Português']].agg(" | ".join, axis=1)
        row_of_interest['text'] = row_of_interest['text with special characters'].apply(lambda x: unidecode(x)) #removendo caracteres especiais

        #Adicionar novo registro no tesauro dentro do firestore
        condition_id = str(uuid.uuid4()) #Cria um id para registro na coleção tesauro
        condition_id = "_".join(["condition_id", condition_id])
        row_of_interest = row_of_interest.to_dict('records') #Transforma linha de dataframe em uma lista com um único dicionário
        row_of_interest = row_of_interest[0] #Seleciona o único dicionário dentro da lista
        firestore_new_document = firestore_collection.document(condition_id) #Cria documento na coleção 'tesauro' dentro do firestore com um novo id único
        firestore_new_document.set(row_of_interest) #Salva o novo registro no tesauro no firebase

        #Comunicar usuário de que o novo registro foi salvo no banco
        st.success('A atualização foi realizada com sucesso!')


#Função que gera a página streamlit
def app():
    with st.container():
        st.header('Atualização do tesauro')
        password = st.text_input('Para atualizar o tesauro, digite a senha de administrador', type='password')
        if password == st.secrets['update_search_password']:
            input = st.text_input('Condição clínica ou motivo de consulta:')
            n_results = st.number_input('Quantos códigos devemos mostrar?', value = 5, min_value=1, max_value=20, step=1, key=None, help='help arg')
            n_results = int(n_results)
            selected_code = search_code(input, n_results)
            new_description = st.text_input('Escreva aqui a descrição que deseja adicionar ao código escolhido.')
            save_button = st.button('Salvar')
            if save_button:
                update_search(new_description, selected_code)
