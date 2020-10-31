import datetime
import os
import pandas as pd
import constant
import util

GROUP_ID = -int(os.getenv("DUPLICATE_GROUP_ID"))
ANALYSIS_START_DATE = datetime.datetime(2020, 10, 26)

postsDf = pd.DataFrame(columns=['date', 'id', 'text', 'link', 'userIds'])

# get posts
vk = util.initialize_vk(constant.APP_TOKEN_KEY)
posts = vk.wall.get(owner_id=GROUP_ID, extended=1, count=20, filter="owner")['items']

# map to [date, id, text, link]
for post in posts:
    post_date = datetime.datetime.utcfromtimestamp(post['date'])

    if post_date < ANALYSIS_START_DATE:
        break

    postRow = pd.Series({
        'date': post_date,
        'id': post['id'],
        'text': post['text'],
        'link': 'https://vk.com/wall' + str(GROUP_ID) + '_' + str(post['id'])
    })
    postsDf.loc[len(postsDf)] = postRow

# add user ids from online status with the same date
onlineMembersDf = util.initialize_id_dump()

for index, row in postsDf.iterrows():
    postsDf.at[index, 'userIds'] = util.get_ids_closest_by_date(row.date, onlineMembersDf)

# save posts to excel file
postsDf.to_excel(constant.PLAGIARIZED_POSTS_FILENAME)

# count user id matches through all posts
idsWithDuplicates = sum(postsDf.userIds.tolist(), [])
id_counts = {i: idsWithDuplicates.count(i) for i in idsWithDuplicates}
sorted_id_counts = {k: v for k, v in sorted(id_counts.items(), key=lambda item: item[1], reverse=True)}
sorted_id_counts_df = pd.DataFrame(list(sorted_id_counts.items()), columns=['id', 'matches'])

sorted_id_counts_df['link'] = sorted_id_counts_df['id'].apply(lambda x: 'https://vk.com/id' + str(x))

# save and print the counts with ids
sorted_id_counts_df.to_excel(constant.POSSIBLE_POST_AUTHORS_FILENAME)
print(sorted_id_counts_df)
