#source: https://newscatcherapi.com/blog/how-to-annotate-entities-with-spacy-phrase-macher

import streamlit as st
from unidecode import unidecode
from google.cloud import firestore

import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Span
from spacy import displacy

import pandas as pd
import pickle
from gensim.models.keyedvectors import KeyedVectors
import time

from tqdm import tqdm

#Próximos passos aqui:
# 1. Criar um PhraseMatcher que se atualize a partir da mesma base de dados da ferramenta de busca ciap2
# 2. Criar uma interface no streamlit que a pessoa possa inserir o texto e abaixo apareçam as sugestões de codificaçãoas
# 3. Tornar o PhraseMatcher não sensível à capitalização 
# 4. Melhorar performance do app otimizando caches e importando as funções que já rodam em outros scripts

#IMPORTANT VARIABLES TO BE USED
service_account_info = st.secrets["gcp_service_account_firestore"]

@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True)
def load_firestore_client(service_account_info = service_account_info):
  firestore_client = firestore.Client.from_service_account_info(service_account_info)
  return firestore_client

firestore_client = load_firestore_client() #Carrega a conexão com a base de dados com cache.


@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True, allow_output_mutation=True)
def firestore_query(firestore_client = firestore_client, field_paths = [], collection = 'tesauro'):
  #Load dataframe for code search
  print("Retrieving database documents...")
  firestore_collection = firestore_client.collection(collection)
  filtered_collection = firestore_collection.select(field_paths)#Fields containing useful data for search engine
  filtered_collection = filtered_collection.get() #Returns a list of document snapshots, from which data can be retrieved
  filtered_collection_dict = []

  print("Converting data to dataframe...")
  for doc in tqdm(filtered_collection):
    filtered_collection_dict.append(doc.to_dict()) #Returns list of dictionaries 
  filtered_collection_dataframe = pd.DataFrame.from_records(filtered_collection_dict) #Returns dataframe
  return filtered_collection_dataframe

nlp = spacy.load("/Users/viniciusanjosdealmeida/Documents/GitHub/Ferramenta-de-busca---CIAP2-CID10/word_vectors")
matcher = PhraseMatcher(nlp.vocab, attr="LOWER")

@st.cache(hash_funcs={firestore.Client: id}, ttl=None, show_spinner=True)
def loading_data():
    df_matcher = firestore_query(field_paths=['`CIAP2_Código1`', '`titulo original`','`Termo Português`'])
    df_matcher['text'] = df_matcher['Termo Português'].copy().apply(lambda x: unidecode(x))
    df = df_matcher[['CIAP2_Código1','text']]
    df.columns = ['ciap', 'text']
    return df, df_matcher
df, df_matcher = loading_data()

print("Building matcher labels and patterns...")
labels = df['ciap'].unique()
for label in tqdm(labels):
    phrases = []
    for row in df[df['ciap']==label].to_numpy().tolist():
        phrases.append(row[1])
    patterns = [nlp(text) for text in phrases]
    matcher.add(label, patterns)

#Saving matcher data for future use
pickle_out = open("matcher.pickle","wb")
pickle.dump(matcher, pickle_out)
pickle_out.close()

#Saving ciap dictionary for future use
df = pd. read_excel('/Users/viniciusanjosdealmeida/python projects/docanno/ciap_cid.xls')
df = df[['codigo', 'titulo original']]
df = df.set_index('codigo')
ciap_dict = df.to_dict('index')
pickle_out = open("ciap_dict.pickle","wb")
pickle.dump(ciap_dict, pickle_out)
pickle_out.close()



# Testing matcher performance
pickle_matcher = open("matcher.pickle","rb")
matcher = pickle.load(pickle_matcher)

pickle_ciap_dict = open("ciap_dict.pickle","rb")
ciap_dict = pickle.load(pickle_ciap_dict)

nlp = spacy.blank("pt", vocab=matcher.vocab) # vocab from reloaded PhraseMatcher


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
    # if len(matches) == 0:
    #     #gen_similarity(text)
    #     print("Spellcheck:", doc._.performed_spellCheck) #Should be True
    #     print("Sugestion:", doc._.outcome_spellCheck)
    print('Para este motivo de consulta, as sugestões de CIAP identificadas são:')
    for ent in ents:
        print(f"{ent['ciap']} {ciap_dict.get(ent['ciap']).get('titulo original')}: {ent['text']}")
    t4 = time.time()

    print(f'Tempo para aplicar a pipeline no doc: {round(t1-t0,3)} seconds')
    print(f'Tempo para aplicar o matcher no doc: {round(t2-t1,3)} seconds')
    print(f'Tempo para separar os resultados do matcher: {round(t3-t2,3)} seconds')
    print(f'Tempo para imprimir os resultados do matcher: {round(t4-t3,3)} seconds')
    print(f'Tempo total: {round(t4-t0,3)} seconds')
    #return ents

def gen_similarity(word):
    model = KeyedVectors.load_word2vec_format("word_vectors/word2vec_ciap_w2v_1.txt", binary=False)
    results = model.most_similar(positive=[word])
    print(results)

test_matcher("""
mal estar
""")