import os
import datetime
from time import sleep
import pandas as pd
import util
import constant


def dump_group_members_online(members_df):
    members = vk.groups.getMembers(group_id=os.getenv("GROUP_ID"), fields="online")
    members_online = list(filter(lambda x: x['online'] == 1, members['items']))
    ids_online = list(map(lambda x: x['id'], members_online))
    now = datetime.datetime.now()
    print('Dumping online members ' + str(now))
    id_dump_row = pd.Series({'date': now, 'idsOnline': ids_online})
    members_df.loc[len(members_df)] = id_dump_row
    members_df.to_excel(constant.ID_DUMP_FILENAME)


vk = util.initialize_vk(constant.GROUP_TOKEN_KEY)
id_dump = util.initialize_id_dump()

while True:
    try:
        dump_group_members_online(id_dump)
    except Exception as e:
        print('error during dump, skipping: ' + str(e))
    if datetime.datetime.now().minute == 0 and datetime.datetime.now().second <= 30:
        util.backup_group_members_file(constant.ID_DUMP_FILENAME)
    sleep(int(os.getenv("WAIT_INTERVAL_SECONDS")))
