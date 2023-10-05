import pandas as pd
from tqdm import tqdm
import numpy as np


def log_reader(log_name):
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


def count_column(df_name, col_name):
    """
    根据列名分组，分组后计数，计数后进行降序排序
    """
    count_col_name = col_name + "计数"
    col_counts_df = df_name.groupby(col_name).size().reset_index(name=count_col_name)
    sorted_df = col_counts_df.sort_values(by=count_col_name, ascending=False)
    return sorted_df


def select_by_value(df_name, col_name, col_value):
    filter_df = df_name.loc[df_name[col_name] == col_value]
    return filter_df


def create_np(df_name, col_name):
    df_col_count = count_column(df_name, col_name).head(20)
    columns = df_col_count.columns.tolist()
    df_col_count_np = df_col_count.to_numpy()
#     df_col_count_np = [columns] + df_col_count_np.tolist()
    return columns, np.transpose(df_col_count_np)


if __name__ == '__main__':
    the_log_short = "message_179.170.130.210.bn.2iij.net_20230930.log"
    # the_log_long = "message_179.170.130.210.bn.2iij.net_20231002.log"
    log_df = log_reader(the_log_short)
    select_df = select_by_value(log_df, 'logid', log_df.iloc[0]['logid']) # logid = "13"
    np_cols, array_2 = create_np(select_df, 'service')
    print(array_2)

