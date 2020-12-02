import ast
import itertools
import os
import pathlib
import time
import pandas as pd
import vk_api
from shutil import copyfile
from matplotlib import pyplot
from numpy import full

import constant

MONITORING_MATCH_DEPTH = int(os.getenv("MONITORING_MATCH_DEPTH"))


def initialize_vk(token_key):
    vk_session = vk_api.VkApi(token=os.getenv(token_key))
    return vk_session.get_api()


def get_id_dump_filepath():
    return os.path.join(pathlib.Path(__file__).parent.absolute(), constant.ID_DUMP_FILENAME)


def initialize_id_dump():
    return pd.DataFrame(columns=['date', 'idsOnline'])


def get_id_dump_df():
    return pd.read_csv(get_id_dump_filepath(), sep='\t', parse_dates=[0])


def get_ids_closest_by_date(target_date, df):
    closest_lines = df.iloc[
        (-(df['date'] - target_date)
         .where(lambda x: x < pd.Timedelta(0, 'm'))
         .dropna())
        .argsort()[:MONITORING_MATCH_DEPTH]
    ]

    if len(closest_lines) == 0:
        return []

    closest_lines_ints = list([ast.literal_eval(x) for x in closest_lines['idsOnline']])
    return list(set(itertools.chain(*closest_lines_ints)))


def backup_group_members_file(path):
    backup_filename = "{0}_{2}{1}".format(*os.path.splitext(path) + (time.strftime("%Y-%m-%d_%H-%M-%S"),))
    copyfile(path, backup_filename)


def plot_people_online(members_df, ids_matches_df):
    if ids_matches_df.size == 0:
        return
    top_matches = ids_matches_df['matches'][0]
    is_top_match = ids_matches_df['matches'] == top_matches
    ids_matches_top_df = ids_matches_df[is_top_match]
    for index, row in ids_matches_top_df.iterrows():
        dates = members_df[members_df['idsOnline'].str.contains(str(row['id']))]['date']
        pyplot.scatter(dates, full(dates.size, index), label=str(row['id']))
    pyplot.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=4)


def plot_post_dates_as_v_lines(series):
    for xc in series:
        pyplot.axvline(x=xc)
