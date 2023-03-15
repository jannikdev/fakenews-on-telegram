# import Telegram API submodules
from unittest import result
from api import *
from utils import (
	get_config_attrs, JSONEncoder, create_dirs, cmd_request_type,
	write_collected_chats, get_forward_attrs
)
import asyncio
import argparse
from pandas import DataFrame
import pandas as pd
import atexit

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
		"channelnames.txt", encoding='utf-8', mode='r'
	)
]

df = pd.DataFrame(columns=['channelname','channel_id','participants_count'])

def postprocessing():
    df.to_csv(
                    './output/data/channel_participants_2.csv',
                    encoding='utf-8',
                    index=False,
                    sep='\t'
                )
atexit.register(postprocessing)

def makerequest(channelname):
    try:
        channel_request = loop.run_until_complete(
	    	full_channel_req(client, channelname)
	    )

	    # save full channel request
        if(channel_request != None):
            full_channel_data = channel_request.to_dict()
        else:
            print("channel_request was None, skipping to next")
        list_row = [channelname, full_channel_data['full_chat']['id'], full_channel_data['full_chat']['participants_count']]
        df.loc[len(df)] = list_row
        print(df)
    except:
        print("an error has occured")
        return
    

for channel in req_input:
    makerequest(channel)
    time.sleep(10)


