import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import pygwalker as pyg


# Set page configuration
st.set_page_config(
    page_title="WarningLogsDashboard",
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

    ls_cols = ["service", "srcport", "dstport", "srcip", "dstip"]
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
warning_df = load_data('C:\\Users\\Public\\PythonCodes\\PythonTools\\LogAnalysis\\pages\\Monthly_warning_Logs_Data_202405.csv')

# Set title and subtitle
st.title('2024年5月 Warning レベルのルーターログのデータ')
st.subheader('元のデータのサンプリング')
st.text('元のデータの量は多いですので、10%に基づいてランダムにサンプリングした後に表示されます')

# Display PyGWalker
vis_spec = r"""{"config":[{"config":{"defaultAggregated":true,"geoms":["auto"],"coordSystem":"generic","limit":-1,"timezoneDisplayOffset":0,"folds":["srcport"]},"encodings":{"dimensions":[{"fid":"date","name":"date","basename":"date","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"time","name":"time","basename":"time","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"logid","name":"logid","basename":"logid","semanticType":"quantitative","analyticType":"dimension","offset":0},{"fid":"type","name":"type","basename":"type","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"subtype","name":"subtype","basename":"subtype","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"level","name":"level","basename":"level","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"srcip","name":"srcip","basename":"srcip","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"dstip","name":"dstip","basename":"dstip","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"srccountry","name":"srccountry","basename":"srccountry","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"dstcountry","name":"dstcountry","basename":"dstcountry","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"action","name":"action","basename":"action","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"service","name":"service","basename":"service","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"gw_mea_key_fid","name":"Measure names","analyticType":"dimension","semanticType":"nominal"}],"measures":[{"fid":"srcport","name":"srcport","basename":"srcport","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dstport","name":"dstport","basename":"dstport","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"gw_count_fid","name":"Row count","analyticType":"measure","semanticType":"quantitative","aggName":"sum","computed":true,"expression":{"op":"one","params":[],"as":"gw_count_fid"}},{"fid":"gw_mea_val_fid","name":"Measure values","analyticType":"measure","semanticType":"quantitative","aggName":"sum"}],"rows":[{"fid":"gw_mea_val_fid","name":"Measure values","analyticType":"measure","semanticType":"quantitative","aggName":"count"}],"columns":[{"fid":"action","name":"action","basename":"action","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"dstcountry","name":"dstcountry","basename":"dstcountry","semanticType":"nominal","analyticType":"dimension","offset":0,"sort":"descending"}],"color":[{"fid":"dstcountry","name":"dstcountry","basename":"dstcountry","semanticType":"nominal","analyticType":"dimension","offset":0}],"opacity":[],"size":[],"shape":[{"fid":"action","name":"action","basename":"action","semanticType":"nominal","analyticType":"dimension","offset":0}],"radius":[],"theta":[],"longitude":[],"latitude":[],"geoId":[],"details":[{"fid":"gw_mea_key_fid","name":"Measure names","analyticType":"dimension","semanticType":"nominal"}],"filters":[],"text":[]},"layout":{"showActions":false,"showTableSummary":false,"stack":"stack","interactiveScale":false,"zeroScale":true,"size":{"mode":"full","width":320,"height":200},"format":{},"geoKey":"name","resolve":{"x":false,"y":false,"color":false,"opacity":false,"shape":false,"size":false},"scaleIncludeUnmatchedChoropleth":false,"showAllGeoshapeInChoropleth":false,"colorPalette":"","useSvg":false,"scale":{"opacity":{},"size":{}}},"visId":"gw_vxru","name":"ActionとDstCountryの統計"}],"chart_map":{},"workflow_list":[{"workflow":[{"type":"view","query":[{"op":"aggregate","groupBy":["action","dstcountry"],"measures":[{"field":"srcport","agg":"count","asFieldKey":"srcport_count"}]}]}]}],"version":"0.4.8.8"}"""
pyg_html_for_data = pyg.to_html(
    df=warning_df.sample(frac=0.1),
    spec=vis_spec,
    theme_key='streamlit',
    default_tab='data'
)
components.html(pyg_html_for_data, height=1000, scrolling=True)

st.subheader('Warning レベルの統計（前20位）')
st.text('全量データに基づいて、service, srcport, dstport, srcip, dstipという５つのアイテムを統計した後に表示されます')
analysis_df = analysis_selected_dataframe(warning_df)
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
