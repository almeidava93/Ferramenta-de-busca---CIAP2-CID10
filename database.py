import pandas as pd
from google.cloud import firestore
import pickle
import streamlit as st

service_account_info = {
  "type": "service_account",
  "project_id": "busca-ciap2",
  "private_key_id": "502754dfe531fbc8927521a1261a6ca4c6b17ee6",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCjqH7riKDS/dl1\nUQsOY6zHkqyLjy01PILAi7/M9S9ecqVkUceNsTjPvrkqOyQX2f6nRzqlHRCVKLaT\ni7J//DQltaWhQbU4lV7D1o0uojpnX36j6gO7Ayfdzl5rwFkmm6BXhdv9flOdD7m5\n5N8fnYk0zSTHyVaNzlM5ChOXJGAW8VjdFzD2ZFrcYFKS4iMDCt7vh8ArDbTmcNiC\neMf8H+4eOIDdJ2heG4MhR63nT8Ikxb1yK+FfE/qGesNpHdGZR3uCnZFCt5rsQP1a\nlgiK6D7FwiPaOqFi1VbOOGM06yyRZK+n9hacyxe6YThVhjpmjNFM+VBAOOPb63bP\nWXF2rrIdAgMBAAECggEAC/IPWykDEPBPRrOxqoBpniLKNJP9nFzCvN2QFGlZGcjy\nFY56kfcVMUseJnaTcQ8L+jD5MNzpCibvz9+eGdI412YycfP8qRkOBh4xxyBqqINd\nGDqYJx0l2pgAZr3VdzuPMPqn6HenqfEtGn2zY5IsiUDoDF6/0jEn/FcMWWqusNd/\n260cMeyQdMISJBC1GGCKHnZ4JlizljuvXpEUuEzGGfeh7WkaTdmzFnWLVNiW8jVv\n1qZjqAT+5HNwM9HCrTTuRo2DH4EmkZRiB9OEyJgl6Tj0ctVi5HpPKTglMYJt9eIn\nUQNDJogCK/N2kFme+dNO2hP023wvOwNzrEWzPi4OKQKBgQDTl8vARtDd5phjcFyI\nz80c/E//p5Kdc2MQkCztAZsdhWJf0N0K5g0nKTk/f2x7tIUx5CAWKa19oLilGLmr\nIXy6lMEsV5sEks9HeaY87Jz4niQoIGPH/c/FFswDTHsE7nqTcvxadpmpJegFVZHb\n/NAyvwpbzqkd0CS2lI1okgDCHwKBgQDGAVH0Yyg+JZ4fOlg+gEKGr6Ycxk8gU27A\nkbUmMPYOFF2zsNRoyOh72yqn/V+X+c+fm6t4PJRVpW9/Tb0hv+N8AaxaobczzDey\nyPFYybiBcQms6yspnKg2qD2O2XPNBwbGAT938BRo88XvbflUD35KT1yiZqyXW8zQ\npEzdCa+cQwKBgCXXqygbQjW9jRmTuej6CTwa4A/gH3ercFdBAdfthpl0BpEOYMoX\nNNkBJWz21CcXtQ6kNxzfnVivivZ0ApjZp14TRq0widf6jbnBxvp0cuqAFgVbLZ1S\nBnvARQJ+Bi5unFuMoBdpyLeYwPNbR3fpsi6xPiAHSPW9CSPiU5wVKK+PAoGAcLlD\nrPveZzMHSAPxRPPD6+WHjg4f8elNvfe1x15MVkul5Kyg4F6wbAKj0CthBqZDWzxG\nbBcg43lhdnoNWG9j1K8nLhmusKBsfS4EqGV+0sJFndnsIq563VL+aJrHYEvFwWfo\neCGHnRBa+SoEraR8Y1W3CQWm8Cxk98rxr0zaWv0CgYEAyLxqn2x6Br0OM4aq1dUy\nyk4BEMWY+IXiShupeC923oAmRJmqTg/SQ45ZjFUytz0Fx0FMn73Lhrl6AXaLuH89\nYpBYXDuw4+g4+/F+mTJt8iJarbACx+SdbVGwQ/QDhnKD+YRd8E5fmZP1hpLFGiSN\nV+MbolTf8AhAky2adMFh9OE=\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-egj8b@busca-ciap2.iam.gserviceaccount.com",
  "client_id": "105202881649691110347",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-egj8b%40busca-ciap2.iam.gserviceaccount.com"
}

def load_data():
    df = pd.read_parquet("text.parquet", engine="pyarrow")
    ciap_list = list(df[['CIAP2_Código1', 'titulo original']].agg(" | ".join, axis=1).drop_duplicates())
    db = firestore.Client.from_service_account_info(service_account_info)
    #db = firestore.Client.from_service_account_json('firestore_key.json')
    with open('CIAP_CID_indexed_data.pkl', 'rb') as pickle_file:
        bm25 = pickle.load(pickle_file)
    search_history = db.collection('search_history').stream()
    return df, ciap_list, db, bm25, search_history

df, ciap_list, db, bm25, search_history = load_data()

class Database():
    TESAURO_DF = df
    CIAP_LIST = ciap_list
    DB = db
    N_RECORDS = len(TESAURO_DF)
    BM25 = bm25
    SEARCH_HISTORY = search_history
