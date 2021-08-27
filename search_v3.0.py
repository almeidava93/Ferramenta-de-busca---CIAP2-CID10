import streamlit as st

# Custom imports 
from multipage import MultiPage
from pages import code_search, feedback # import your pages here

# Create an instance of the app 
app = MultiPage()

# Title of the main page
st.title("Codificação CIAP2")

# Add all your applications (pages) here
app.add_page("Ferramenta de busca", code_search.app)
app.add_page("Não encontrou?", feedback.app)

# The main app
app.run()