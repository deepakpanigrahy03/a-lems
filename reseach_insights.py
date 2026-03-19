# pages/2_Research_Insights.py
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml


# Load config
@st.cache_data
def load_research_questions():
    with open("config/research_questions.yaml", "r") as f:
        return yaml.safe_load(f)


@st.cache_resource
def get_db_connection():
    return sqlite3.connect("data/experiments.db", uri=True)


# Page config
st.set_page_config(page_title="A-LEMS Research Insights", page_icon="📊", layout="wide")

st.title("🔬 A-LEMS Research Insights")
st.markdown("---")

# Load questions
questions_config = load_research_questions()
conn = get_db_connection()

# Create tabs
tab1, tab2, tab3 = st.tabs(
    ["🎯 Guided Insights", "🔧 Custom Query Lab", "📖 Schema Reference"]
)

# ============================================
# TAB 1: Guided Insights
# ============================================
with tab1:
    st.header("Guided Research Questions")

    # Category selection
    categories = [c["name"] for c in questions_config["categories"]]
    selected_category = st.selectbox(
        "Select Category", categories, help="Choose a research category"
    )

    # Get questions for selected category
    category_data = next(
        c for c in questions_config["categories"] if c["name"] == selected_category
    )

    # Question selection
    question_names = [q["name"] for q in category_data["questions"]]
    selected_question = st.selectbox(
        "Select Research Question",
        question_names,
        help="Choose a specific question to analyze",
    )

    # Get question details
    question_data = next(
        q for q in category_data["questions"] if q["name"] == selected_question
    )

    # Filters
    with st.expander("🔍 Filters", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            providers = pd.read_sql(
                "SELECT DISTINCT provider FROM experiments WHERE provider IS NOT NULL",
                conn,
            )["provider"].tolist()
            selected_providers = st.multiselect(
                "Providers", providers, default=providers
            )

        with col2:
            workflows = ["linear", "agentic"]
            selected_workflows = st.multiselect(
                "Workflows", workflows, default=workflows
            )

        with col3:
            date_range = st.date_input("Date Range", [])

    # Run analysis button
    if st.button("🚀 Run Analysis", type="primary", use_container_width=True):
        with st.spinner("Running analysis..."):
            # Execute query
            df = pd.read_sql(question_data["sql_template"], conn)

            # Display results in columns
            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("📊 Insight Summary")
                if "insight_template" in question_data:
                    # Generate insight from first row
                    if not df.empty:
                        insight = question_data["insight_template"].format(
                            **df.iloc[0].to_dict()
                        )
                        st.info(insight)
                    else:
                        st.warning("No data available for selected filters")

            with col2:
                st.subheader("📈 Key Metrics")
                if not df.empty:
                    metrics = df.select_dtypes(include=["float64", "int64"]).columns
                    for metric in metrics[:3]:  # Show top 3 metrics
                        st.metric(
                            label=metric.replace("_", " ").title(),
                            value=f"{df[metric].mean():.3f}",
                        )

            # Chart
            st.subheader("📉 Visualization")
            if not df.empty:
                if question_data["chart_type"] == "bar":
                    if isinstance(question_data["y_axis"], list):
                        fig = px.bar(
                            df,
                            x=question_data["x_axis"],
                            y=question_data["y_axis"],
                            barmode="group",
                            title=question_data["name"],
                        )
                    else:
                        fig = px.bar(
                            df,
                            x=question_data["x_axis"],
                            y=question_data["y_axis"],
                            title=question_data["name"],
                        )
                        if "error_bars" in question_data:
                            fig.update_traces(
                                error_y=dict(
                                    type="data", array=df[question_data["error_bars"]]
                                )
                            )
                    st.plotly_chart(fig, use_container_width=True)

                elif question_data["chart_type"] == "grouped_bar":
                    fig = px.bar(
                        df,
                        x=question_data["x_axis"],
                        y=question_data["y_axis"],
                        color=question_data["group_by"],
                        barmode="group",
                        title=question_data["name"],
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Supporting data table
            with st.expander("📋 Supporting Data", expanded=False):
                st.dataframe(df, use_container_width=True)

                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    f"{question_data['id']}_results.csv",
                    "text/csv",
                )

            # SQL query (optional)
            with st.expander("🔍 View SQL Query", expanded=False):
                st.code(question_data["sql_template"], language="sql")

# ============================================
# TAB 2: Custom Query Lab
# ============================================
with tab2:
    st.header("Custom Query Lab")

    col1, col2 = st.columns([2, 1])

    with col1:
        # SQL Editor
        default_query = """SELECT 
    tc.category,
    r.workflow_type,
    COUNT(*) as runs,
    AVG(r.total_energy_uj/1e6) as avg_energy_j
FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
LEFT JOIN task_categories tc ON e.task_name = tc.task_id
GROUP BY tc.category, r.workflow_type
ORDER BY avg_energy_j DESC;"""

        query = st.text_area(
            "📝 Write SQL Query",
            value=default_query,
            height=200,
            help="Write your custom SQL query here",
        )

        # Run button
        if st.button("▶️ Run Query", type="primary"):
            try:
                with st.spinner("Executing query..."):
                    df = pd.read_sql(query, conn)

                    if not df.empty:
                        st.success(f"Query returned {len(df)} rows")

                        # Results table
                        st.subheader("📊 Results")
                        st.dataframe(df, use_container_width=True)

                        # Chart builder
                        st.subheader("📈 Chart Builder")

                        chart_type = st.selectbox(
                            "Chart Type", ["Bar", "Scatter", "Line", "Box", "Histogram"]
                        )

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            numeric_cols = df.select_dtypes(
                                include=["float64", "int64"]
                            ).columns.tolist()
                            x_axis = st.selectbox("X Axis", df.columns.tolist())

                        with col2:
                            y_axis = st.selectbox(
                                "Y Axis",
                                numeric_cols if numeric_cols else df.columns.tolist(),
                            )

                        with col3:
                            color_by = st.selectbox(
                                "Color By (optional)", ["None"] + df.columns.tolist()
                            )

                        # Generate chart
                        if chart_type == "Bar":
                            if color_by != "None":
                                fig = px.bar(df, x=x_axis, y=y_axis, color=color_by)
                            else:
                                fig = px.bar(df, x=x_axis, y=y_axis)

                        elif chart_type == "Scatter":
                            if color_by != "None":
                                fig = px.scatter(df, x=x_axis, y=y_axis, color=color_by)
                            else:
                                fig = px.scatter(df, x=x_axis, y=y_axis)

                        elif chart_type == "Line":
                            if color_by != "None":
                                fig = px.line(df, x=x_axis, y=y_axis, color=color_by)
                            else:
                                fig = px.line(df, x=x_axis, y=y_axis)

                        elif chart_type == "Box":
                            fig = px.box(df, x=x_axis, y=y_axis)

                        elif chart_type == "Histogram":
                            fig = px.histogram(df, x=x_axis)

                        st.plotly_chart(fig, use_container_width=True)

                        # Export options
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            csv = df.to_csv(index=False)
                            st.download_button(
                                "📥 Download CSV", csv, "query_results.csv", "text/csv"
                            )

                        with col2:
                            if "fig" in locals():
                                st.download_button(
                                    "📥 Download Chart (HTML)",
                                    fig.to_html(),
                                    "chart.html",
                                    "text/html",
                                )

                        with col3:
                            st.button(
                                "📋 Copy SQL", on_click=lambda: st.write("SQL copied!")
                            )

                    else:
                        st.warning("Query returned no results")

            except Exception as e:
                st.error(f"Error executing query: {str(e)}")

    with col2:
        st.subheader("Quick Templates")
        templates = {
            "Energy by Category": """SELECT tc.category, AVG(r.total_energy_uj/1e6) as avg_energy_j FROM runs r JOIN experiments e ON r.exp_id = e.exp_id LEFT JOIN task_categories tc ON e.task_name = tc.task_id GROUP BY tc.category;""",
            "Tax Analysis": """SELECT e.task_name, AVG(ots.tax_percent) as avg_tax FROM orchestration_tax_summary ots JOIN runs r ON ots.agentic_run_id = r.run_id JOIN experiments e ON r.exp_id = e.exp_id GROUP BY e.task_name ORDER BY avg_tax DESC;""",
            "Provider Comparison": """SELECT e.provider, AVG(r.total_energy_uj/1e6) as avg_energy, AVG(r.duration_ns/1e9) as avg_sec FROM runs r JOIN experiments e ON r.exp_id = e.exp_id GROUP BY e.provider;""",
        }

        for name, template in templates.items():
            if st.button(f"📋 {name}"):
                st.session_state["query"] = template
                st.rerun()

# ============================================
# TAB 3: Schema Reference
# ============================================
with tab3:
    st.header("Database Schema Reference")

    # Get all tables
    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", conn
    )["name"].tolist()

    for table in tables:
        with st.expander(f"📊 {table}"):
            # Get table schema
            schema = pd.read_sql(f"PRAGMA table_info({table});", conn)

            col1, col2 = st.columns([1, 3])
            with col1:
                st.markdown("**Columns:**")
                for _, row in schema.iterrows():
                    st.code(f"{row['name']}", language="text")

            with col2:
                st.markdown("**Sample Data:**")
                sample = pd.read_sql(f"SELECT * FROM {table} LIMIT 3;", conn)
                st.dataframe(sample, use_container_width=True)

            # Sample query
            st.markdown("**Sample Query:**")
            st.code(
                f"SELECT * FROM {table} WHERE run_id IN (SELECT run_id FROM runs LIMIT 5);",
                language="sql",
            )

# Footer
st.markdown("---")
st.caption("A-LEMS Research Platform v1.0 | Questions? Contact the team")
