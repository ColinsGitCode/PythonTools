import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import pygwalker as pyg

from AccessLogAnalysis import LogAnalysis, Utils, Const

# Set page configuration
st.set_page_config(
    page_title="Router Logs Dashboard",
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


info_df = load_data('Monthly_warning_Logs_Data_202405.csv')

# Set title and subtitle
st.title('2024年5月 Warning レベルのルーターログのデータ')
st.subheader('元のデータのサンプリング')
st.text('元のデータの量は多いですので、10%に基づいてランダムにサンプリングした後に表示されます')

# st.write("DataFrame Head:")
# st.write(info_df.head())

# Display PyGWalker
vis_spec = r"""{"config":[{"config":{"defaultAggregated":true,"geoms":["auto"],"coordSystem":"generic","limit":-1,"timezoneDisplayOffset":0,"folds":["srcport"]},"encodings":{"dimensions":[{"fid":"date","name":"date","basename":"date","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"time","name":"time","basename":"time","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"logid","name":"logid","basename":"logid","semanticType":"quantitative","analyticType":"dimension","offset":0},{"fid":"type","name":"type","basename":"type","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"subtype","name":"subtype","basename":"subtype","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"level","name":"level","basename":"level","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"srcip","name":"srcip","basename":"srcip","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"dstip","name":"dstip","basename":"dstip","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"srccountry","name":"srccountry","basename":"srccountry","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"dstcountry","name":"dstcountry","basename":"dstcountry","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"action","name":"action","basename":"action","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"service","name":"service","basename":"service","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"gw_mea_key_fid","name":"Measure names","analyticType":"dimension","semanticType":"nominal"}],"measures":[{"fid":"srcport","name":"srcport","basename":"srcport","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dstport","name":"dstport","basename":"dstport","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"gw_count_fid","name":"Row count","analyticType":"measure","semanticType":"quantitative","aggName":"sum","computed":true,"expression":{"op":"one","params":[],"as":"gw_count_fid"}},{"fid":"gw_mea_val_fid","name":"Measure values","analyticType":"measure","semanticType":"quantitative","aggName":"sum"}],"rows":[{"fid":"gw_mea_val_fid","name":"Measure values","analyticType":"measure","semanticType":"quantitative","aggName":"count"}],"columns":[{"fid":"action","name":"action","basename":"action","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"dstcountry","name":"dstcountry","basename":"dstcountry","semanticType":"nominal","analyticType":"dimension","offset":0,"sort":"descending"}],"color":[{"fid":"dstcountry","name":"dstcountry","basename":"dstcountry","semanticType":"nominal","analyticType":"dimension","offset":0}],"opacity":[],"size":[],"shape":[{"fid":"action","name":"action","basename":"action","semanticType":"nominal","analyticType":"dimension","offset":0}],"radius":[],"theta":[],"longitude":[],"latitude":[],"geoId":[],"details":[{"fid":"gw_mea_key_fid","name":"Measure names","analyticType":"dimension","semanticType":"nominal"}],"filters":[],"text":[]},"layout":{"showActions":false,"showTableSummary":false,"stack":"stack","interactiveScale":false,"zeroScale":true,"size":{"mode":"full","width":320,"height":200},"format":{},"geoKey":"name","resolve":{"x":false,"y":false,"color":false,"opacity":false,"shape":false,"size":false},"scaleIncludeUnmatchedChoropleth":false,"showAllGeoshapeInChoropleth":false,"colorPalette":"","useSvg":false,"scale":{"opacity":{},"size":{}}},"visId":"gw_vxru","name":"ActionとDstCountryの統計"}],"chart_map":{},"workflow_list":[{"workflow":[{"type":"view","query":[{"op":"aggregate","groupBy":["action","dstcountry"],"measures":[{"field":"srcport","agg":"count","asFieldKey":"srcport_count"}]}]}]}],"version":"0.4.8.8"}"""
pyg_html_for_data = pyg.to_html(info_df.sample(frac=0.1), spec=vis_spec)
components.html(pyg_html_for_data, height=1000, scrolling=True)

st.subheader('Warning レベルの統計（前20位）')
st.text('全量データに基づいて、service, srcport, dstport, srcip, dstipという５つのアイテムを統計した後に表示されます')
analysis_df = LogAnalysis.analysis_selected_dataframe(info_df)
pyg_html_for_stats = pyg.to_html(analysis_df)
components.html(pyg_html_for_stats, height=1000, scrolling=True)
