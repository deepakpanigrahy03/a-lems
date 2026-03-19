import graphviz
import streamlit as st

graph = graphviz.Digraph()

graph.edge("Session", "gsm8k_basic")
graph.edge("gsm8k_basic", "cloud")
graph.edge("cloud", "experiment_3")
graph.edge("gsm8k_basic", "local")
graph.edge("local", "experiment_4")

st.graphviz_chart(graph)
