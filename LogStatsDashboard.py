import streamlit as st
import pandas as pd


st.set_page_config(
    page_title="RouterLogsDashboard",
    page_icon=":chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Load Data
@st.cache_data
def load_execl(execl_file):
    # csv_df = pd.read_csv(csv_file).sample(frac=0.1)
    execl_df = pd.read_excel(execl_file)
    return execl_df


st.title('2024年5月のルーターログの統計')

st.subheader('level　に基づく統計データ')

# log_level_df = load_execl('/LogAnalysis/LogAnalysis_20240527/Monthly_Logs_Level_Stats_202405.xlsx')
log_level_df = load_execl('/root/PythonAutomatic/LogAnalysis/Monthly_Logs_Level_Stats_202405.xlsx')
st.dataframe(
    data=log_level_df,
    height=200,
    use_container_width=True,
    hide_index=True
)

st.subheader('logid　に基づく統計データ')

log_id_df = load_execl('/root/PythonAutomatic/LogAnalysis/Monthly_Logs_ID_Stats_202405.xlsx')
log_id_df[['logid']] = log_id_df[['logid']].astype(str)
st.dataframe(
    data=log_id_df,
    height=800,
    use_container_width=True,
    hide_index=True
)