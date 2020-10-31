import ast
import itertools
import os
import pathlib
import pandas as pd
import vk_api

import constant

MONITORING_MATCH_DEPTH = int(os.getenv("MONITORING_MATCH_DEPTH"))


def initialize_vk(token_key):
    vk_session = vk_api.VkApi(token=os.getenv(token_key))
    return vk_session.get_api()


def get_id_dump_filepath():
    return os.path.join(pathlib.Path(__file__).parent.absolute(), constant.ID_DUMP_FILENAME)


def initialize_id_dump():
    data_file_path = get_id_dump_filepath()
    if not os.path.isfile(data_file_path):
        pd.DataFrame(columns=['date', 'idsOnline']).to_excel(constant.ID_DUMP_FILENAME)
    return pd.read_excel(get_id_dump_filepath(), index_col=0)


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
