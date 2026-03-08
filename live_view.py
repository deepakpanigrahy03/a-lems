import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import time

st.set_page_config(layout="wide", page_title="A-LEMS Live")

# Server URL
SERVER_URL = "http://localhost:8501"

# Auto-refresh
refresh_rate = st.sidebar.slider("Refresh (sec)", 0.5, 2.0, 1.0)

# Get active run from user
run_id = st.sidebar.number_input("Run ID", min_value=1, value=631)

# Fetch live data
try:
    response = requests.get(f"{SERVER_URL}/api/live/{run_id}")
    if response.status_code == 200:
        samples = response.json().get('samples', [])
        
        if samples:
            df = pd.DataFrame(samples)
            df['time_rel'] = df['timestamp'] - df['timestamp'].iloc[-1]
            
            # Create 3 charts
            col1, col2 = st.columns(2)
            
            with col1:
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(x=df['time_rel'], y=df['cpu_busy_mhz'], 
                                         name='CPU Busy MHz', line=dict(color='blue')))
                fig1.update_layout(title='CPU Frequency', height=300)
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=df['time_rel'], y=df['package_temp'], 
                                         name='Temperature', line=dict(color='red')))
                fig2.update_layout(title='Package Temperature', height=300)
                st.plotly_chart(fig2, use_container_width=True)
            
            # Power and Interrupts
            col3, col4 = st.columns(2)
            
            with col3:
                fig3 = go.Figure()
                fig3.add_trace(go.Scatter(x=df['time_rel'], y=df['pkg_power'], 
                                         name='Package Power', line=dict(color='green')))
                fig3.update_layout(title='Package Power (W)', height=250)
                st.plotly_chart(fig3, use_container_width=True)
            
            with col4:
                fig4 = go.Figure()
                fig4.add_trace(go.Scatter(x=df['time_rel'], y=df['interrupt_rate'], 
                                         name='Interrupts', line=dict(color='orange')))
                fig4.update_layout(title='Interrupt Rate (/sec)', height=250)
                st.plotly_chart(fig4, use_container_width=True)
            
            # Latest stats
            st.subheader("Latest Values")
            last = df.iloc[-1]
            cols = st.columns(5)
            cols[0].metric("CPU Busy", f"{last['cpu_busy_mhz']:.0f} MHz")
            cols[1].metric("Temperature", f"{last['package_temp']:.1f}°C")
            cols[2].metric("Power", f"{last['pkg_power']:.2f} W")
            cols[3].metric("Interrupts", f"{last['interrupt_rate']:.0f}/sec")
            cols[4].metric("Avg MHz", f"{last['cpu_avg_mhz']:.0f} MHz")
            
    time.sleep(refresh_rate)
    st.rerun()
    
except Exception as e:
    st.error(f"Error: {e}")
