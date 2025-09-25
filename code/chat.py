import streamlit as st
from rag_pipeline import rag

st.set_page_config(page_title="RAG Chat", page_icon="💬")
st.title("RAG Chat App")

# Show instructions
st.info("💡 You can ask questions about Edgar 10-K files. Try typing a question!")

# Initialize chat history
if "history" not in st.session_state:
    st.session_state.history = []

# User input
user_input = st.text_input("You:", "")

if st.button("Send") and user_input:
    answer = rag(user_input)
    st.session_state.history.append({"user": user_input, "bot": answer})

# Display chat history
for chat in st.session_state.history:
    st.markdown(f"**You:** {chat['user']}")
    st.markdown(f"**Bot:** {chat['bot']}")
    st.markdown("---")
