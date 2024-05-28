import pandas as pd
from tqdm import tqdm
import numpy as np
import re


class LogAnalysis:
    @staticmethod
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
                        if ls_pairs[0] == 'logid':
                            word_pair_dict[ls_pairs[0]] = re.sub(r'\D', '', ls_pairs[1])
                        else:
                            word_pair_dict[ls_pairs[0]] = ls_pairs[1]
                    except IndexError:
                        # ls_pairs = pairs.split("=")
                        # word_pair_dict[ls_pairs[0]] = ""
                        pass
                split_results.append(word_pair_dict)

        results_df = pd.DataFrame(split_results)
        return results_df

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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
            selected_df_col_counts = LogAnalysis.count_column_with_ratios(selected_df, col).head(20)
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

    @staticmethod
    def detail_analysis_by_column_and_value(total_logs_df: pd.DataFrame, col_name: str, col_value, logdate: str):
        """
        解析Dataframe表格中的 "service", "srcport", "dstport", "srcip", "dstip" 中的五项指标的计数排名及百分比
        以及 srccountry 和 dstcountry 2项指标的计数排名及百分比
        也含有action
        并存储为 execl表格
        :param total_logs_df:
        :param col_name:
        :param col_value:
        :return:
        """
        selected_df_by_column_name_and_value = LogAnalysis.select_by_column_name_and_value(
            total_logs_df,
            col_name,
            col_value
        )
        detail_result_total_df = LogAnalysis.analysis_selected_dataframe(selected_df_by_column_name_and_value)
        output_execl_name = "LongLogsAnalysisResultsTables\\" + logdate + "\\" + col_name + col_value.strip("\"") + "DetailAnalysisResults.xlsx"
        detail_result_total_df.to_excel(output_execl_name, index=False)
        print("DataFrame saved to " + output_execl_name)

        srccountry_col_counts_df = LogAnalysis.count_column_with_ratios(
            selected_df_by_column_name_and_value,
            'srccountry'
        ).head(20)
        output_execl_name = "LongLogsAnalysisResultsTables\\" + logdate + "\\" + col_name + col_value.strip("\"") + "srccountryResults.xlsx"
        srccountry_col_counts_df.to_excel(output_execl_name, index=False)
        print("DataFrame saved to " + output_execl_name)

        dstcountry_col_counts_df = LogAnalysis.count_column_with_ratios(
            selected_df_by_column_name_and_value,
            'dstcountry'
        ).head(20)
        output_execl_name = "LongLogsAnalysisResultsTables\\" + logdate + "\\" + col_name + col_value.strip("\"") + "dstcountryResults.xlsx"
        dstcountry_col_counts_df.to_excel(output_execl_name, index=False)
        print("DataFrame saved to " + output_execl_name)

        action_col_counts_df = LogAnalysis.count_column_with_ratios(
            selected_df_by_column_name_and_value,
            'action'
        ).head(20)
        output_execl_name = "LongLogsAnalysisResultsTables\\" + logdate + "\\" + col_name + col_value.strip("\"") + "actionResults.xlsx"
        action_col_counts_df.to_excel(output_execl_name, index=False)
        print("DataFrame saved to " + output_execl_name)

        return detail_result_total_df, srccountry_col_counts_df, dstcountry_col_counts_df

    @staticmethod
    def statics_for_log_levels(total_logs_df: pd.DataFrame, logdate: str):
        """
        Result for sheet: LogLevelRatios
        :param total_logs_df:
        :param logdate:
        :return:
        """
        df_loglevel_counts = LogAnalysis.count_column_with_ratios(total_logs_df, "level").head(20)
        output_execl_name = "LongLogsAnalysisResultsTables\\" + logdate + "\\LogLevelRatios_Results.xlsx"
        df_loglevel_counts.to_excel(output_execl_name, index=False)
        print("DataFrame saved to " + output_execl_name)
        return df_loglevel_counts

    @staticmethod
    def statics_all_logid_ratio(total_logs_df: pd.DataFrame, logdate: str):
        """
        Result for sheet: AllLogidRatios: Logid, Logid计数， Logid百分比
        :param total_logs_df:
        :param logdate:
        :return:
        """
        df_logid_counts = LogAnalysis.count_column_with_ratios(total_logs_df, "logid").head(20)
        output_execl_name = "LongLogsAnalysisResultsTables\\" + logdate + "\\AllLogidRatios_Results.xlsx"
        df_logid_counts.to_excel(output_execl_name, index=False)
        print("DataFrame saved to " + output_execl_name)
        return df_logid_counts


if __name__ == '__main__':
    # 0,1,53,220
    # the_log_name = "message_179.170.130.210.bn.2iij.net_20231208.log"
    # logdate = "LogAnalysis_1208"

    # 0,1,2,220
    # the_log_name = "message_179.170.130.210.bn.2iij.net_20231209.log"
    # logdate = "LogAnalysis_1209"

    # 0,1,131,220
    the_log_name = "message_179.170.130.210.bn.2iij.net_20231210.log"
    logdate = "LogAnalysis_1210"

    all_logs_df = LogAnalysis.log_reader(the_log_name)

    print("index = 0 value = " + all_logs_df.iloc[0]['logid'])
    print("index = 1 value = " + all_logs_df.iloc[1]['logid'])
    print("index = 2 value = " + all_logs_df.iloc[2]['logid'])
    print("index = 220 value = " + all_logs_df.iloc[220]['logid'])

    LogAnalysis.statics_all_logid_ratio(all_logs_df, logdate)
    LogAnalysis.statics_for_log_levels(all_logs_df, logdate)
    LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[0]['logid'], logdate)
    LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[1]['logid'], logdate)
    LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[2]['logid'], logdate)
    LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[220]['logid'], logdate)
