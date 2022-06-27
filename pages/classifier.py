#source: https://newscatcherapi.com/blog/how-to-annotate-entities-with-spacy-phrase-macher

import streamlit as st
from unidecode import unidecode

import spacy
from spacy.matcher import PhraseMatcher
from spacy.matcher import Matcher
from spacy.tokens import Span

import pandas as pd
import pickle
import time

#Custom packages
from database import *

# Next steps:
# - combine phrase matcher with matcher for more accuracy and generalizability in specific cases


@st.cache(hash_funcs={spacy.vocab.Vocab: id, spacy.lang.pt.Portuguese: id}, ttl=None, show_spinner=True)
def load_data():
    pickle_in = open("matcher.pickle","rb")
    matcher1 = pickle.load(pickle_in)

    pickle_in = open("ciap_dict.pickle","rb")
    ciap_dict = pickle.load(pickle_in)
    
    nlp = spacy.load('pt_core_news_lg')

    return nlp, ciap_dict, matcher1

nlp, ciap_dict, matcher1 = load_data()




#Adding some customised patterns to the matcher
matcher2 = Matcher(matcher1.vocab)

label = '-47' # Ex.: encaminhamento para ginecologia, deseja/quer/gostaria ser encaminhado para cardio
pattern = [[{'POS': 'VERB', 'LEMMA': {'IN': ['gostar', 'querer', 'desejar']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'},
            {'LEMMA': {'IN':['encaminhamento','encaminhar']}}, {'POS': 'ADP', 'OP': '?'}, {'POS': 'NOUN'}]
    ]
matcher2.add(label, pattern)

label = '-60' # Ex.: resultados de um exame qualquer ou de exames
patterns = [
    [{'LEMMA': 'resultar', 'POS': 'NOUN'}, {'POS': 'ADP', 'OP': '?'}, {'POS': 'NOUN'}]
]
matcher2.add(label, patterns)

label = 'L01' # Ex.: dor pescoço, desconforto, incômodo, tensão
patterns = [
    [{'LEMMA': {'IN':['dor', 'desconforto', 'incomodo', 'tensão']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'}, {'LEMMA': {'IN':['pescoço']}}]
]
matcher2.add(label, patterns)

label = 'L02' # Ex.: dor nas costas, na dorsal, no dorso
patterns = [
    [{'LEMMA': {'IN':['dor', 'desconforto', 'incomodo', 'tensão']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'}, {'LEMMA': {'IN':['costas', 'dorsal', 'dorso']}}]
]
matcher2.add(label, patterns)

label = 'L03' # Ex.: dor lombar, desconforto na lombar, na coluna lombar
patterns = [
    [{'LEMMA': {'IN':['dor', 'desconforto', 'incomodo']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'}, {'LEMMA': {'IN':['lombar']}}]
]
matcher2.add(label, patterns)

label = 'L08' # Ex.: dor ombro, desconforto ombro
patterns = [
    [{'LEMMA': {'IN':['dor', 'desconforto', 'incomodo']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'}, {'LEMMA': {'IN':['ombro']}}]
]
matcher2.add(label, patterns)

label = 'L15' # Ex.: dor joelho, desconforto no joelho
patterns = [
    [{'LEMMA': {'IN':['dor', 'desconforto', 'incomodo', 'tensão']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'}, {'LEMMA': {'IN':['joelho', 'patela']}}]
]
matcher2.add(label, patterns)

label = 'L80' # Ex.: luxação/luxou patela, ombro etc
patterns = [
    [{'LEMMA': {'IN':['luxar', 'luxacao', 'luxação']}}, {'OP':'?'}, {'OP':'?'}, {'OP':'?'}, {'POS': 'NOUN'}]
]
matcher2.add(label, patterns)

label = 'N07' # Ex.: crise convulsiva, convulsão, crise de ausência
patterns = [
    [{'LEMMA': 'crise', 'POS': 'NOUN'}, {'POS': 'ADP', 'OP': '?'}, {'POS': 'NOUN', 'LEMMA': {'IN':['ausencia', 'ausência']}}],
    [{'LEMMA': 'crise', 'POS': 'NOUN', 'OP': '?'}, {'LEMMA': {'IN':['convulsivo', 'convulsão']}}],
    [{'LEMMA': {'IN': ['crise', 'convulsão', 'convulsao', 'convulsoes']}}, {'LEMMA': {'IN': ['tonico', 'tonico-clonica', 'tonico-clonico', 'tonico-clonicos', 'tonico-clonicas']}}, {'LEMMA': {'IN': ['clonica', 'clonico']}, 'OP': '?'}, {'LEMMA': {'IN': ['generalizar']}, 'OP': '?'}]
]
matcher2.add(label, patterns)

label = 'P77' # Ex.: tentativa de suicídio
patterns = [
    [{'LEMMA': {'IN': ['tentativo', 'tentar', 'pensar', 'pensamento', 'desejar', 'querer']}, 'POS': {'IN': ['NOUN', 'VERB']}}, { 'OP': '?'}, { 'OP': '?'}, {'LEMMA': 'se', 'OP': '?'}, {'LEMMA':{'IN': ['suicidio', 'suicidios', 'suicidar-se', 'suicidar', 'matar', 'morrer', 'morte']}}],
    [{'LEMMA': {'IN': ['ideacao', 'ideacoes', 'ideação']}, 'POS': 'NOUN'}, {'LEMMA': 'suicidar', 'POS': 'ADJ'}]
]
matcher2.add(label, patterns)

label = 'S08' # Ex.: manchas na pele, nos pés, no rosto
patterns = [
    [{'LEMMA': 'manchar', 'POS': 'NOUN'}, {'POS': 'ADP', 'OP': '?'}, {'POS': 'NOUN'}]
]
matcher2.add(label, patterns)

label = 'X18' # Ex.: dor nas mamas
patterns = [
    [{'LEMMA': 'dor', 'POS': 'NOUN'}, {'OP': '?'}, {'OP': '?'}, {'OP': '?'}, {'LEMMA': {'IN': ['mamar', 'mamário']},'POS':{'IN': ['ADJ', 'VERB', 'NOUN']}}]
]
matcher2.add(label, patterns)




@st.cache(ttl=None, show_spinner=True)
def join_columns(dataframe, column_names, delimiter=' | ', drop_duplicates=False):
  df = dataframe[column_names].agg(delimiter.join, axis=1)
  if drop_duplicates==True: df.drop_duplicates()
  return df

def test_matcher(text):
    text = unidecode(text)
    doc = nlp(text)
    matches1 = matcher1(doc)
    ents = []
    for match_id, start, end in matches1:
        span = doc[start:end]
        #print(span.text, match_id, start, end, nlp.vocab.strings[match_id])
        ents.append({'text': span.text, 'match_id': match_id, 'start': start, 'end': end, 'ciap': matcher1.vocab.strings[match_id]})
    matches2 = matcher2(doc)
    for match_id, start, end in matches2:
        span = doc[start:end]
        #print(span.text, match_id, start, end, nlp.vocab.strings[match_id])
        ents.append({'text': span.text, 'match_id': match_id, 'start': start, 'end': end, 'ciap': matcher2.vocab.strings[match_id]})
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
