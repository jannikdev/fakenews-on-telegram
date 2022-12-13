# import Telegram API submodules
from unittest import result
from api import *
from utils import (
	get_config_attrs, JSONEncoder, create_dirs, cmd_request_type,
	write_collected_chats, get_forward_attrs
)
import asyncio
import argparse

# Parse arguments
config_attrs = get_config_attrs()

args = {**config_attrs}

# if all(i is not None for i in args.values()):
# 	parser.error('Select either --telegram-channel or --batch-file options only.')

'''

Variables

'''

# Telegram API credentials

'''

FILL API KEYS
'''
sfile = 'session_file'
api_id = args['api_id']
api_hash = args['api_hash']
phone = args['phone']
counter = {}

# event loop
loop = asyncio.get_event_loop()

'''

> Get Client <API connection>

'''

# Get `client` connection
client = loop.run_until_complete(
	get_connection(sfile, api_id, api_hash, phone)
)

# request type
# req_type, req_input = cmd_request_type(args)
req_input = [
	i.rstrip() for i in open(
		"channel_ids.txt", encoding='utf-8', mode='r'
	)
]


async def makerequest(channelname):
    print(channel*-100)
    channel_id=channel*-100
    result = await client(functions.contacts.SearchRequest(
        q=channel,
        limit=1
    ))
    if len(result.chats) > 0 and result.chats[0].username!="":
        print(channel +" -- "+ result.chats[0].username)
    else:
        print(channel + " -- NOTFOUND!!")

for channel in req_input:
    loop.run_until_complete(makerequest(channel))

