import pandas as pd
from tqdm import tqdm
import numpy as np


def log_reader(log_name: str) -> pd.DataFrame:
    """
    逐行读取日志，分割并提取，最后保存为 pandas dataframe
    """
    with open(log_name, 'r', encoding='cp1252') as file:
        # Get the total number of lines in the file
        total_lines = sum(1 for _ in file)

    with open(log_name, 'r', encoding='cp1252') as file:
        split_results = []
        # for line in file:
        for line in tqdm(file, total=total_lines, desc="Reading logs by lines:"):
            words = line.split()[4:]
            word_pair_dict = {}
            for pairs in words:
                try:
                    ls_pairs = pairs.split("=")
                    word_pair_dict[ls_pairs[0]] = ls_pairs[1]
                except IndexError:
                    # ls_pairs = pairs.split("=")
                    # word_pair_dict[ls_pairs[0]] = ""
                    pass
            split_results.append(word_pair_dict)

    results_df = pd.DataFrame(split_results)
    return results_df


def count_column_with_ratios(df_name: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    根据列名分组，分组后计数，计数后进行降序排序
    """
    count_col_name = col_name + "计数"
    col_counts_df = df_name.groupby(col_name).size().reset_index(name=count_col_name)
    sorted_df = col_counts_df.sort_values(by=count_col_name, ascending=False)
    total_counts = sorted_df[count_col_name].sum()
    sorted_df[col_name + "%比"] = sorted_df[count_col_name].apply(lambda x: (round(x / total_counts, 4)) * 100)
    return sorted_df


def select_by_column_name_and_value(df_name: pd.DataFrame, col_name: str, col_value) -> pd.DataFrame:
    """
    选取某行是某值的所有数据
    :param df_name:
    :param col_name:
    :param col_value:
    :return:
    """
    filter_df = df_name.loc[df_name[col_name] == col_value]
    return filter_df


def analysis_selected_dataframe(selected_df: pd.DataFrame) -> pd.DataFrame:
    """
    解析Dataframe表格中的 "service", "srcport", "dstport", "srcip", "dstip" 中的五项指标的计数排名及百分比
    :param selected_df:
    :return:
    """
    # ls_cols=["service", "srcport", "dstport", "srcip", "dstip", "dstcountry"]:
    ls_cols = ["service", "srcport", "dstport", "srcip", "dstip"]
    total_column_list = ['default_1', 'default_2', 'default_3']
    total_ndarray = np.empty((20, 3))
    for col in ls_cols:
        selected_df_col_counts = count_column_with_ratios(selected_df, col).head(20)
        selected_df_col_counts_columns = selected_df_col_counts.columns.tolist()
        selected_df_col_counts_ndarray = selected_df_col_counts.to_numpy()
        total_column_list = total_column_list + selected_df_col_counts_columns
        total_ndarray = np.concatenate((total_ndarray, selected_df_col_counts_ndarray), axis=1)  # 注意指定axis=1以进行横向拼接
    total_dataframe = pd.DataFrame(total_ndarray, columns=total_column_list)
    total_dataframe = total_dataframe.drop(['default_1', 'default_2', 'default_3'], axis=1)
    return total_dataframe


def detail_analysis_by_column_and_value(total_logs_df: pd.DataFrame, col_name: str, col_value):
    """
    解析Dataframe表格中的 "service", "srcport", "dstport", "srcip", "dstip" 中的五项指标的计数排名及百分比
    以及 srccountry 和 dstcountry 2项指标的计数排名及百分比
    并存储为 execl表格
    :param total_logs_df:
    :param col_name:
    :param col_value:
    :return:
    """
    selected_df_by_column_name_and_value = select_by_column_name_and_value(total_logs_df, col_name, col_value)
    detail_result_total_df = analysis_selected_dataframe(selected_df_by_column_name_and_value)
    output_execl_name = ("LongLogsAnalysisResultsTables\\" + col_name + col_value.strip("\"") + "DetailAnalysisResults.xlsx")
    detail_result_total_df.to_excel(output_execl_name, index=False)
    print("DataFrame saved to " + output_execl_name)

    srccountry_col_counts_df = (count_column_with_ratios(selected_df_by_column_name_and_value, 'srccountry').head(20))
    output_execl_name = "LongLogsAnalysisResultsTables\\" + col_name + col_value.strip("\"") + "srccountryResults.xlsx"
    srccountry_col_counts_df.to_excel(output_execl_name, index=False)
    print("DataFrame saved to " + output_execl_name)

    dstcountry_col_counts_df = (count_column_with_ratios(selected_df_by_column_name_and_value, 'dstcountry').head(20))
    output_execl_name = "LongLogsAnalysisResultsTables\\" + col_name + col_value.strip("\"") + "dstcountryResults.xlsx"
    dstcountry_col_counts_df.to_excel(output_execl_name, index=False)
    print("DataFrame saved to " + output_execl_name)

    return detail_result_total_df, srccountry_col_counts_df, dstcountry_col_counts_df


if __name__ == '__main__':
    the_log_short = "message_179.170.130.210.bn.2iij.net_20230930.log"
    the_log_long = "message_179.170.130.210.bn.2iij.net_20231002.log"
    # log_df = log_reader(the_log_short)
    # log_df = log_reader(the_log_long)
    # select_df = select_by_value(log_df, 'logid', log_df.iloc[0]['logid']) # logid = "13"


