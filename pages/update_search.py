"""
1. Acessar dataframe principal
2. Acessar linha mais próxima da que se deseja adicionar
3. Copiar essa linha e editar para colocar a expressão que se deseja
4. Gerar novo índice bm25 para busca
"""

import streamlit as st
import pandas as pd
from unidecode import unidecode
import pickle
from rank_bm25 import BM25Okapi
import spacy

from database import Database

#DEFININDO AS FUNÇÕES NECESSÁRIAS
#Função que salva objetos em disco via pickle, substituindo arquivo antigo, se houver
def save_object(obj, filename):
    with open(filename, 'wb') as outp:  # Overwrites any existing file.
        pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)


#Função que gera o índice BM25 para a busca e atualiza o arquivo
def bm25_index(df):
    #Launch the language object
    nlp = spacy.load("pt_core_news_lg")
    #Preparing for tokenisation
    text_list = df['text'].str.lower().values
    tok_text=[] # for our tokenised corpus
    #Tokenising using SpaCy:
    for doc in nlp.pipe(text_list, disable=["tagger", "parser","ner"]):
        tok = [t.text for t in doc]
        tok_text.append(tok)
    #Building a BM25 index
    bm25 = BM25Okapi(tok_text)
    save_object(bm25,'CIAP_CID_indexed_data.pkl')


#Função que salva objetos no disco e substitui caso o arquivo já exista
def save_object(obj, filename):
    with open(filename, 'wb') as outp:  # Overwrites any existing file.
        pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)


#Função que atualiza o tesauro, o índice de busca para expansão e aumento da precisão da busca
def update_search(new_description, code_reference):
    #df = database.Database.TESAURO_DF
    df = pd.read_parquet("text.parquet", engine="pyarrow") #abre tesauro; a linha acima pode também puxar direto do tesauro já carregado
    row_of_interest = df.loc[df['text']==code_reference] #descrição mais próxima do termo que eu quero adicionar
    row_of_interest['Termo Português'] = new_description #descrição que eu quero adicionar para o código em questão
    row_of_interest['text with special characters'] = row_of_interest[['CIAP2_Código1', 'titulo original','Termo Português']].agg(" | ".join, axis=1)
    row_of_interest['text'] = row_of_interest['text with special characters'].apply(lambda x: unidecode(x)) #removendo caracteres especiais
    #Adicionar linha de interesse no dataframe. Via dict é mais eficiente do que via append.
    df = df.to_dict('records')
    row_of_interest = row_of_interest.to_dict('records')
    df = df + row_of_interest
    df = pd.DataFrame.from_records(df)
    df = df.drop_duplicates() #remove linhas duplicadas se houver
    #Salvar arquivo atualizado para uso imediato.
    df.to_parquet("text.parquet", engine="pyarrow")
    #Atualizar índice BM25 para uso imediato.
    bm25_index(df)


#Função que retorna o código escolhido
def search_code(input, n_results):
    DB = Database()
    db = DB.DB
    df = DB.TESAURO_DF
    if input == "":
        pass
    else:
        #Querying this index just requires a search input which has also been tokenized:
        input = unidecode(input) #remove acentos e caracteres especiais
        tokenized_query = input.lower().split(" ")
        results = Database.BM25.get_top_n(tokenized_query, df.text.values, n=n_results)
        results = [i for i in results]
        selected_code = st.radio('Esses são os códigos que encontramos. Selecione um para prosseguir.', results, index=0, help='Selecione um dos códigos para prosseguir.')
        return selected_code


#Função que gera a página streamlit
def app():
    with st.container():
        st.header('Atualização do tesauro')
        password = st.text_input('Para atualizar o tesauro, digite a senha de administrador', type='password')
        if password == st.secrets['update_search_password']:
            input = st.text_input('Condição clínica ou motivo de consulta:')
            n_results = st.number_input('Quantos códigos devemos mostrar?', value = 5, min_value=1, max_value=20, step=1, key=None, help='help arg')
            selected_code = search_code(input, n_results)
            new_description = st.text_input('Escreva aqui a descrição que deseja adicionar ao código escolhido.')
            save_button = st.button('Salvar')
            if save_button:
                update_search(new_description, selected_code)
                st.success('A atualização foi realizada com sucesso!')
        else:
            pass