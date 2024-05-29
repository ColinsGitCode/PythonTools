import os
# import modin.pandas as modin_pd
import pandas as pd
from tqdm import tqdm
import numpy as np
import re
import ray
import pyarrow

os.environ["MODIN_ENGINE"] = "ray"


class Const:
    HEAD_NUMBER = 100
    SELECT_COLUMNS = [
        'date',
        'time',
        # 'devname',
        # 'devid',
        # 'eventtime',
        # 'type',
        # 'subtype',
        'level',
        'srcip',
        'srcport',
        'dstip',
        'dstport',
        'srccountry',
        'dstcountry',
        # 'action',
        # 'service',
        # 'app',
        # 'duration',
        # 'sentbyte',
        # 'rcvdbyte'
        # 'logdesc',
        # 'user',
        'logid'
    ]


class Utils:
    @staticmethod
    def creat_dirs(dir_path: str):
        # 检查目录是否存在
        if not os.path.exists(dir_path):
            # 如果目录不存在，则创建目录
            os.makedirs(dir_path)
            # print(f"目录 '{dir_path}' 已创建")
        else:
            pass
            # print(f"目录 '{dir_path}' 已存在")

    @staticmethod
    def get_all_files_in_directory(directory):
        # 初始化一个空列表用于存储文件相对路径
        file_paths = []

        # 使用os.walk遍历目录及其子目录
        for root, _, files in os.walk(directory):
            for file in files:
                # 构建文件的相对路径
                relative_path = os.path.relpath(os.path.join(root, file), directory)
                file_paths.append(directory + "/" + relative_path)

        return file_paths


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
                        if ls_pairs[0] in Const.SELECT_COLUMNS:
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
        count_col_name = col_name + " Counts"
        col_counts_df = df_name.groupby(col_name).size().reset_index(name=count_col_name)
        sorted_df = col_counts_df.sort_values(by=count_col_name, ascending=False)
        total_counts = sorted_df[count_col_name].sum()
        sorted_df[col_name + " %"] = sorted_df[count_col_name].apply(lambda x: (round(x / total_counts, 4)) * 100)
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
    def statics_for_log_levels(total_logs_df: pd.DataFrame, output_execl_name_prefix: str, to_execl=False):
        """
        Result for sheet: LogLevelRatios
        :param total_logs_df:
        :param output_execl_name:
        :return:
        """
        output_execl_name = output_execl_name_prefix + "/" + output_execl_name_prefix + "_Log_Level_Stats.xlsx"
        df_loglevel_counts = LogAnalysis.count_column_with_ratios(total_logs_df, "level").head(20)
        if to_execl:
            df_loglevel_counts.to_excel(
                excel_writer=output_execl_name,
                sheet_name='Log Level Stats',
                float_format='%.2f',
                index=False
            )
            print("DataFrame saved to " + output_execl_name)
        else:
            pass

        return df_loglevel_counts

    @staticmethod
    def statics_all_logid_ratio(total_logs_df: pd.DataFrame, output_execl_name_prefix: str, to_execl=False):
        """
        Result for sheet: AllLogidRatios: Logid, Logid计数， Logid百分比
        :param total_logs_df:
        :param output_execl_name:
        :return:
        """
        output_execl_name = output_execl_name_prefix + "/" + output_execl_name_prefix + "_Log_ID_Stats.xlsx"
        df_log_id_counts = LogAnalysis.count_column_with_ratios(total_logs_df, "logid")
        if to_execl:
            df_log_id_counts.to_excel(
                excel_writer=output_execl_name,
                sheet_name='Log ID Stats',
                float_format='%.2f',
                index=False
            )
            print("DataFrame saved to " + output_execl_name)
        else:
            pass

        return df_log_id_counts

    @staticmethod
    def combine_dataframe_by_set_index(df1: pd.DataFrame, df2: pd.DataFrame, index_col: str, count_col: str):
        select_columns = [index_col, count_col]
        print("df1.columns: ", df1.columns)
        print("df2.columns: ", df2.columns)
        # df1 = df1[select_columns].set_index(index_col)
        df1 = df1.set_index(index_col)
        df2 = df2.set_index(index_col)
        # df2 = df2[select_columns].set_index(index_col)
        print("df1.columns: ", df1.columns)
        print("df2.columns: ", df2.columns)

        # 确保索引列为字符串
        df1.index = df1.index.astype(str)
        df2.index = df2.index.astype(str)

        # 合并两个DataFrame，按索引列处理，缺失值填充为0
        df1 = df1.apply(pd.to_numeric, errors='coerce').fillna(0)
        df2 = df2.apply(pd.to_numeric, errors='coerce').fillna(0)
        # 按索引合并并将相同列相加
        add_df = df1.add(df2, fill_value=0).fillna(0)
        sorted_df = add_df.sort_values(by=count_col, ascending=False)
        print("sorted_df.columns: ", sorted_df.columns)
        # sorted_df = sorted_df[count_col].astype(int)
        total_counts = sorted_df[count_col].sum()
        sorted_df[index_col + " %"] = sorted_df[count_col].apply(lambda x: (round(x / total_counts, 10)) * 100)
        print("sorted_df.columns: ", sorted_df.columns)

        return sorted_df

    @staticmethod
    def analysis_logs_monthly_by_dataframe(logs_path: str):
        log_files = Utils.get_all_files_in_directory(logs_path)

        monthly_log_id_stats_df = pd.DataFrame({'logid': [], 'logid Counts': []})
        monthly_log_level_stats_df = pd.DataFrame({'level': [], 'level Counts': []})

        for the_log_name in log_files:
            daily_log_stats_execl_prefix = "LogAnalysis_" + the_log_name.split('_')[-1].split('.')[0]
            Utils.creat_dirs(daily_log_stats_execl_prefix)
            daily_logs_df = LogAnalysis.log_reader(the_log_name)
            log_id_stats_df = LogAnalysis.statics_all_logid_ratio(
                total_logs_df=daily_logs_df,
                output_execl_name_prefix=daily_log_stats_execl_prefix
            )
            print("monthly_log_id_stats_df.columns: ", monthly_log_id_stats_df.columns)
            print("log_id_stats_df.columns: ", log_id_stats_df.columns)
            monthly_log_id_stats_df = LogAnalysis.combine_dataframe_by_set_index(monthly_log_id_stats_df, log_id_stats_df, 'logid', 'logid Counts')

            log_level_stats_df = LogAnalysis.statics_for_log_levels(
                total_logs_df=daily_logs_df,
                output_execl_name_prefix=daily_log_stats_execl_prefix
            )
            print("monthly_log_level_stats_df.columns: ", monthly_log_level_stats_df.columns)
            print("log_level_stats_df.columns: ", log_level_stats_df.columns)
            monthly_log_level_stats_df = LogAnalysis.combine_dataframe_by_set_index(monthly_log_level_stats_df, log_level_stats_df, 'level', 'level Counts')

        return monthly_log_id_stats_df, monthly_log_level_stats_df

    @staticmethod
    def analysis_logs_monthly(logs_path: str, month_str: str):
        log_id_stats_dict = {}
        log_level_stats_dict = {}
        log_files = Utils.get_all_files_in_directory(logs_path)

        for the_log_name in log_files:
            logs_date = the_log_name.split('_')[-1].split('.')[0]
            reading_desc = "Reading logs on " + logs_date + " by lines: "
            with open(the_log_name, 'r', encoding='cp1252') as file:
                # Get the total number of lines in the file
                total_lines = sum(1 for _ in file)

            with open(the_log_name, 'r', encoding='cp1252') as file:
                # for line in file:
                for line in tqdm(file, total=total_lines, desc=reading_desc):
                    words = line.split()[4:]
                    # word_pair_dict = {}
                    for pairs in words:
                        try:
                            ls_pairs = pairs.split("=")

                            if ls_pairs[0] == 'logid':
                                clean_log_id_str = re.sub(r'\D', '', ls_pairs[1])
                                if clean_log_id_str in log_id_stats_dict.keys():
                                    log_id_stats_dict[clean_log_id_str] = log_id_stats_dict[clean_log_id_str] + 1
                                else:
                                    log_id_stats_dict[clean_log_id_str] = 1

                            elif ls_pairs[0] == 'level':
                                log_level_str = ls_pairs[1]
                                if log_level_str in log_level_stats_dict.keys():
                                    log_level_stats_dict[log_level_str] = log_level_stats_dict[log_level_str] + 1
                                else:
                                    log_level_stats_dict[log_level_str] = 1

                                if log_level_str == ''

                        except IndexError:
                            # ls_pairs = pairs.split("=")
                            # word_pair_dict[ls_pairs[0]] = ""
                            pass

        log_id_stats_df = pd.DataFrame(list(log_id_stats_dict.items()), columns=['logid', 'logid_count'])
        log_id_stats_df['%(percentage)'] = (log_id_stats_df['logid_count'] / log_id_stats_df['logid_count'].sum()) * 100
        log_id_stats_df = log_id_stats_df.sort_values(by='%(percentage)', ascending=False)
        log_id_stats_execl_name = 'Monthly_Logs_ID_Stats_' + month_str + ".xlsx"
        log_id_stats_df.to_excel(
            excel_writer=log_id_stats_execl_name,
            sheet_name='Logs ID Stats',
            float_format='%.6f',
            engine='openpyxl',
            index=False
        )
        print("DataFrame saved to " + log_id_stats_execl_name)

        log_level_stats_df = pd.DataFrame(list(log_level_stats_dict.items()), columns=['level', 'level_count'])
        log_level_stats_df['%(percentage)'] = (log_level_stats_df['level_count'] / log_level_stats_df['level_count'].sum()) * 100
        log_level_stats_df = log_level_stats_df.sort_values(by='%(percentage)', ascending=False)
        log_level_stats_execl_name = 'Monthly_Logs_Level_Stats_' + month_str + ".xlsx"
        log_level_stats_df.to_excel(
            excel_writer=log_id_stats_execl_name,
            sheet_name='Logs Level Stats',
            float_format='%.6f',
            engine='openpyxl',
            index=False
        )
        print("DataFrame saved to " + log_level_stats_execl_name)

        return log_id_stats_df, log_level_stats_df


if __name__ == '__main__':
    pass

    # print(log_id_stats_df.iloc[0]['logid'])
    # print(log_id_stats_df.iloc[1]['logid'])
    # print(log_id_stats_df.iloc[2]['logid'])
    # print(log_id_stats_df.iloc[3]['logid'])
    # LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[0]['logid'], logdate)
    # LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[1]['logid'], logdate)
    # LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[2]['logid'], logdate)
    # LogAnalysis.detail_analysis_by_column_and_value(all_logs_df, 'logid', all_logs_df.iloc[220]['logid'], logdate)

    # the_log_name = "LogPlainTxt/message_179.170.130.210.bn.2iij.net_20240527.log"
    # daily_log_stats_execl_prefix = "LogAnalysis_" + the_log_name.split('_')[-1].split('.')[0]
    #
    # Utils.creat_dirs(daily_log_stats_execl_prefix)
    #
    # all_logs_df = LogAnalysis.log_reader(the_log_name)
    #
    # log_id_stats_df = LogAnalysis.statics_all_logid_ratio(
    #     total_logs_df=all_logs_df,
    #     output_execl_name_prefix=daily_log_stats_execl_prefix
    # )
    # log_id_stats_df_mem = log_id_stats_df.memory_usage(deep=True).sum()
    # print(log_id_stats_df_mem/(1024))
    #
    # log_level_df = LogAnalysis.statics_for_log_levels(
    #     total_logs_df=all_logs_df,
    #     output_execl_name_prefix=daily_log_stats_execl_prefix
    # )
    # log_level_df_mem = log_level_df.memory_usage(deep=True).sum()
    # print(log_level_df_mem/(1024))
