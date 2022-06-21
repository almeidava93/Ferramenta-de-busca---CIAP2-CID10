import streamlit as st

# Custom imports 
from multipage import MultiPage
from pages import code_search, feedback, update_search, search_v4_test # import your pages here

# Create an instance of the app 
app = MultiPage()

# Title of the main page
st.title("Codificação de condições clínicas")

# Add all your applications (pages) here
app.add_page("CIAP2 - Nova versão", search_v4_test.app)
app.add_page("Ferramenta de busca", code_search.app)
app.add_page("Não encontrou?", feedback.app)
app.add_page("Atualize o tesauro", update_search.app)

# The main app
app.run()