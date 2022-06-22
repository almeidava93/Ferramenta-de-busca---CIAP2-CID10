#source: https://newscatcherapi.com/blog/how-to-annotate-entities-with-spacy-phrase-macher

import streamlit as st
from unidecode import unidecode

import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Span

import pandas as pd
import pickle
import time

@st.cache(hash_funcs={spacy.vocab.Vocab: id}, ttl=None, show_spinner=True)
def load_data():
    pickle_in = open("matcher.pickle","rb")
    matcher = pickle.load(pickle_in)

    pickle_in = open("ciap_dict.pickle","rb")
    ciap_dict = pickle.load(pickle_in)
    
    nlp = spacy.blank("pt", vocab=matcher.vocab) # vocab from reloaded PhraseMatcher

    return nlp, ciap_dict, matcher

nlp, ciap_dict, matcher = load_data()


@st.cache(ttl=None, show_spinner=True)
def join_columns(dataframe, column_names, delimiter=' | ', drop_duplicates=False):
  df = dataframe[column_names].agg(delimiter.join, axis=1)
  if drop_duplicates==True: df.drop_duplicates()
  return df

def test_matcher(text):
    text = unidecode(text)
    t0 = time.time()
    doc = nlp(text)
    t1 = time.time()
    matches = matcher(doc)
    t2 = time.time()
    ents = []
    for match_id, start, end in matches:
        span = doc[start:end]
        #print(span.text, match_id, start, end, nlp.vocab.strings[match_id])
        ents.append({'text': span.text, 'match_id': match_id, 'start': start, 'end': end, 'ciap': nlp.vocab.strings[match_id]})
    t3 = time.time()
    # Organizing results for visualization and grouping by code
    results_df = pd.DataFrame(columns=['ciap', 'titulo original', 'text'])
    for ent in ents:
        row = pd.DataFrame.from_dict({'ciap': [ent['ciap']], 'titulo original': [ciap_dict.get(ent['ciap']).get('titulo original')], 'text': [ent['text']]})
        results_df = pd.concat([results_df, row])
    results_df = results_df.drop_duplicates().groupby(['ciap'], as_index=False, sort=False).agg({'titulo original': 'first', 'text': ' | '.join})
    results_df['ciap'] = join_columns(results_df, ['ciap','titulo original'], delimiter=' | ', drop_duplicates=False)
    st.write(f'Para o motivo de consulta acima, os códigos mais compatíveis que encontramos são:')
    for row in results_df[['ciap', 'text']].to_numpy().tolist():
      with st.expander(f"{row[0]}"):
        st.write(f"_{row[1]}_")

def app():
    with st.container():
        st.header('Classificador de motivos de consulta')
        st.write('Digite abaixo o texto referente ao motivo de consulta de um atendimento. Nós te ajudaremos a encontrar a melhor codificação para o seu atendimento.')
        input = st.text_area('Motivo de consulta:')
        if input != "":
            t0 = time.time()
            test_matcher(input)
            t1 = time.time()
            search_time = round(t1-t0,3)
            st.text(f'Texto analisado em {search_time} seconds \n')
