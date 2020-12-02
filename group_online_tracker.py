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


vk = util.initialize_vk(constant.GROUP_TOKEN_KEY)
id_dump = util.initialize_id_dump()

save_count = 0
backup_count = 0
MAX_SAVE_COUNT = 0

while True:
    save_count += 1
    backup_count += 1
    try:
        dump_group_members_online(id_dump)
    except Exception as e:
        print('error during dump, skipping: ' + str(e))
    if save_count > MAX_SAVE_COUNT:
        save_count = 0
        id_dump.to_csv(constant.ID_DUMP_FILENAME, sep='\t', encoding='utf-8', mode='a', index=False, header=False)
        print('Saved  ' + str(datetime.datetime.now()))
        id_dump = util.initialize_id_dump()
    if backup_count > 60:
        backup_count = 0
        util.backup_group_members_file(constant.ID_DUMP_FILENAME)
    sleep(int(os.getenv("WAIT_INTERVAL_SECONDS")))
