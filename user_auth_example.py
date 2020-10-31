import vk_api
import os
from dotenv import load_dotenv
load_dotenv()

LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
vk_session = vk_api.VkApi(LOGIN, PASSWORD)
vk_session.auth()

vk = vk_session.get_api()

members = vk.groups.getMembers(group_id=os.getenv("GROUP_ID"), fields="online")

membersOnline = list(filter(lambda x: x['online'] == 1, members['items']))

print(members)
