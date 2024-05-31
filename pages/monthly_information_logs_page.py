import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import pygwalker as pyg


# Set page configuration
st.set_page_config(
    page_title="InformationLogsDashboard",
    page_icon=":chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Load Data
@st.cache_data
def load_data(csv_file):
    # csv_df = pd.read_csv(csv_file).sample(frac=0.1)
    csv_df = pd.read_csv(csv_file).sample(frac=0.1)
    return csv_df


def analysis_selected_dataframe(selected_df: pd.DataFrame) -> pd.DataFrame:
    """
    解析Dataframe表格中的 "service", "srcport", "dstport", "srcip", "dstip" 中的五项指标的计数排名及百分比
    :param selected_df:
    :return:
    """

    ls_cols = ["remip", "logid", "mac", "ip", "hostname"]
    total_column_list = ['default_1', 'default_2', 'default_3']
    total_ndarray = np.empty((20, 3))

    for col in ls_cols:
        count_col_name = col + " Counts"
        col_counts_df = selected_df.groupby(col).size().reset_index(name=count_col_name)
        sorted_df = col_counts_df.sort_values(by=count_col_name, ascending=False)
        total_counts = sorted_df[count_col_name].sum()
        sorted_df[col + " %"] = sorted_df[count_col_name].apply(lambda x: (round(x / total_counts, 4)) * 100)
        selected_df_col_counts = sorted_df.head(20)
        selected_df_col_counts_columns = selected_df_col_counts.columns.tolist()
        selected_df_col_counts_ndarray = selected_df_col_counts.to_numpy()
        # 期望的行数
        desired_rows = 20
        # 计算需要填充的行数
        padding_rows = max(0, desired_rows - selected_df_col_counts_ndarray.shape[0])
        # 使用pad函数在底部填充零行
        selected_df_col_counts_ndarray_padded = np.pad(
            selected_df_col_counts_ndarray,
            ((0, padding_rows), (0, 0)),
            mode='constant',
            constant_values=0
        )
        total_column_list = total_column_list + selected_df_col_counts_columns
        total_ndarray = np.concatenate(
            (total_ndarray, selected_df_col_counts_ndarray_padded),
            axis=1
        )  # 注意指定axis=1以进行横向拼接
    total_dataframe = pd.DataFrame(total_ndarray, columns=total_column_list)
    total_dataframe = total_dataframe.drop(['default_1', 'default_2', 'default_3'], axis=1)
    return total_dataframe


# Pages Contents Start
# alert_df = load_data('Monthly_alert_Logs_Data_202405.csv')
# infor_df = load_data('/LogAnalysis/LogAnalysis_20240527/Monthly_information_Logs_Data_202405.csv')
infor_df = load_data('/root/PythonAutomatic/LogAnalysis/Monthly_information_Logs_Data_202405.csv')
infor_df.drop(columns=[
    'devid',
    'eventtime',
    'tz',
    'devname'
], inplace=True)

# Set title and subtitle
st.title('2024年5月 Error レベルのルーターログのデータ')
# st.subheader('元のデータのサンプリング')
# st.text('元のデータの量は多いですので、10%に基づいてランダムにサンプリングした後に表示されます')

# Display PyGWalker
pyg_html_for_data = pyg.to_html(
    df=infor_df,
    theme_key='streamlit',
    default_tab='data'
)
components.html(pyg_html_for_data, height=1000, scrolling=True)

st.subheader('Error レベルの統計（前20位）')

st.text('全量データに基づいて、remip, logid, mac, ip, hostnameという５つのアイテムを統計した後に表示されます')
analysis_df = analysis_selected_dataframe(infor_df)
# pyg_html_for_stats = pyg.to_html(
#     df=analysis_df,
#     theme_key='streamlit',
#     default_tab='data'
# )
# components.html(pyg_html_for_stats, height=1000, scrolling=True)

st.dataframe(
    data=analysis_df,
    height=800,
    use_container_width=True,
    hide_index=True
)
