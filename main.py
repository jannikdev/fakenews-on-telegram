# -*- coding: utf-8 -*-

# import modules
import pandas as pd
import argparse
import asyncio
import json
import time
import sys
import os
import atexit

# import Telegram API submodules
from api import *
from utils import (
	get_config_attrs, JSONEncoder, create_dirs, cmd_request_type,
	write_collected_chats, get_forward_attrs
)

'''

Arguments

'''
chats_file = None

def postprocessing():
	global chats_file
	# close chat file
	chats_file.close()

	# get collected chats
	collected_chats = list(set([
		i.rstrip() for i in open(chats_path, mode='r', encoding='utf-8')
	]))

	# re write collected chats
	chats_file = open(chats_path, mode='w', encoding='utf-8')
	for c in collected_chats:
		chats_file.write(f'{c}\n')

	# close file
	chats_file.close()


	# Process counter
	counter_df = pd.DataFrame.from_dict(
		counter,
		orient='index'
	).reset_index().rename(
		columns={
			'index': 'id'
		}
	)

	# save counter
	counter_df.to_csv(
		f'{output_folder}/counter.csv',
		encoding='utf-8',
		index=False
	)

	# merge dataframe
	df = pd.read_csv(
		f'{output_folder}/collected_chats.csv',
		encoding='utf-8'
	)

	del counter_df['username']
	df = df.merge(counter_df, how='left', on='id')
	df.to_csv(
		f'{output_folder}/collected_chats.csv',
		index=False,
		encoding='utf-8'
	)


	# log results
	text = f'''
	End program at {time.ctime()}

	'''
	print (text)

atexit.register(postprocessing)

parser = argparse.ArgumentParser(description='Arguments.')
parser.add_argument(
	'--telegram-channel',
	type=str,
	required='--batch-file' not in sys.argv,
	help='Specifies a Telegram Channel.'
)
parser.add_argument(
	'--batch-file',
	type=str,
	required='--telegram-channel' not in sys.argv,
	help='File containing Telegram Channels, one channel per line.'
)
parser.add_argument(
	'--limit-download-to-channel-metadata',
	action='store_true',
	help='Will collect channels metadata only, not posts data.'
)

'''

Output
'''
parser.add_argument(
	'--output',
	'-o',
	type=str,
	required=False,
	help='Folder to save collected data. Default: `./output/data`'
)

'''

Updating data
'''
parser.add_argument(
	'--min-id',
	type=int,
	help='Specifies the offset id. This will update Telegram data with new posts.'
)

# Parse arguments
args = vars(parser.parse_args())
config_attrs = get_config_attrs()

args = {**args, **config_attrs}

if all(i is not None for i in args.values()):
	parser.error('Select either --telegram-channel or --batch-file options only.')


# log results
text = f'''
Init program at {time.ctime()}

'''
print (text)


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
req_type, req_input = cmd_request_type(args)
if req_type == 'batch':
	req_input = [
		i.rstrip() for i in open(
			req_input, encoding='utf-8', mode='r'
		)
	]
else:
	req_input = [req_input]

# Reading | Creating an output folder
if args['output']:
	output_folder = args['output']
	if output_folder.endswith('/'):
		output_folder = output_folder[:-1]
	else:
		pass
else:
	output_folder = './output/data'

# Create dirs
create_dirs(output_folder)

handled_channels_counter = 0
max_channels = 20000
starttime = time.time()
# iterate channels
for channel in req_input:
	# print(req_input)
	# print(req_input.__contains__('radio_bielefeld_online'))

	if(handled_channels_counter >= max_channels):
		break

	'''


	Process arguments
	-> channels' data

	-> Get Entity <Channel's attrs>
	-> Get Full Channel request.
	-> Get Posts <Request channels' posts>

	'''

	# new line
	print ('')
	print (f'> Collecting data from Telegram Channel -> {channel}')
	print ('> ...')
	print ('')
	try:
		# Channel's attributes
		entity_attrs = loop.run_until_complete(
			get_entity_attrs(client, channel)
		)
	except Exception as e:
		print(e)
		print('Channel could not be scraped, going to next after 5secs')
		time.sleep(5)
		continue

	# Get Channel ID | convert output to dict
	channel_id = entity_attrs.id
	entity_attrs_dict = entity_attrs.to_dict()

	# Collect Source -> GetFullChannelRequest
	channel_request = loop.run_until_complete(
		full_channel_req(client, channel_id)
	)

	# save full channel request
	full_channel_data = channel_request.to_dict()

	# JsonEncoder
	full_channel_data = JSONEncoder().encode(full_channel_data)
	full_channel_data = json.loads(full_channel_data)

	# save data
	print ('> Writing channel data...')
	create_dirs(output_folder, subfolders=channel)
	file_path = f'{output_folder}/{channel}/{channel}.json'
	channel_obj = json.dumps(
		full_channel_data,
		ensure_ascii=False,
		separators=(',',':')
	)
	writer = open(file_path, mode='w', encoding='utf-8')
	writer.write(channel_obj)
	writer.close()
	print ('> done.')
	print ('')

	# collect chats
	chats_path = f'{output_folder}/chats.txt'
	chats_file = open(chats_path, mode='a', encoding='utf-8')

	# channel chats
	counter = write_collected_chats(
		full_channel_data['chats'],
		chats_file,
		channel,
		counter,
		'channel_request',
		client,
		output_folder
	)

	if not args['limit_download_to_channel_metadata']:

		# Collect posts
		if not args['min_id']:
			posts = loop.run_until_complete(
				get_posts(client, channel_id)
			)
		else:
			min_id = args['min_id']
			posts = loop.run_until_complete(
				get_posts(client, channel_id, min_id=min_id)
			)

		data = posts.to_dict()

		# Get offset ID | Get messages
		# offset_id = min([i['id'] for i in data['messages']])
		offset_id = 0

		while len(posts.messages) > 0:
			
			if args['min_id']:
				posts = loop.run_until_complete(
					get_posts(
						client,
						channel_id,
						min_id=min_id,
						offset_id=offset_id
					)
				)	
			else:
				posts = loop.run_until_complete(
					get_posts(
						client,
						channel_id,
						offset_id=offset_id
					)
				)

			# Update data dict
			if posts.messages:
				tmp = posts.to_dict()
				# Adding unique messages
				all_messages = [i['id'] for i in data['messages']]
				messages = [
					i for i in tmp['messages']
					if i['id'] not in all_messages
				]
				data['messages'].extend(messages)
				# Adding unique chats objects
				all_chats = [i['id'] for i in data['chats']]
				chats = [
					i for i in tmp['chats']
					if i['id'] not in all_chats
				]

				# channel chats in posts
				counter = write_collected_chats(
					tmp['chats'],
					chats_file,
					channel,
					counter,
					'from_messages',
					client,
					output_folder
				)

				# Adding unique users objects
				all_users = [i['id'] for i in data['users']]
				users = [
					i for i in tmp['users']
					if i['id'] not in all_users
				]

				# extend UNIQUE chats & users
				data['chats'].extend(chats)
				data['users'].extend(users)

				for message in tmp['messages']:
					# Forward attrs
					response = {
						'channel_id': message['peer_id']['channel_id']
					}
					forward_attrs = message['fwd_from']
					response['is_forward'] = 1 if forward_attrs != None else 0
					response['forward_msg_date'] = None
					response['forward_msg_date_string'] = None
					response['forward_msg_link'] = None
					response['from_channel_id'] = None
					response['from_channel_name'] = None
					# print(data['chats'])
					if forward_attrs:
						response = get_forward_attrs(
							forward_attrs,
							response,
							pd.DataFrame(data['chats'])
					)
					forwarder = response['from_channel_name']
					if forwarder != None and not req_input.__contains__(forwarder):
						print("Adding new channels:")
						print(forwarder)
						req_input.append(forwarder)
						

				# Get offset ID
				offset_id = min([i['id'] for i in tmp['messages']])

		# JsonEncoder
		data = JSONEncoder().encode(data)
		data = json.loads(data)

		# save data
		print ('> Writing posts data...')
		file_path = f'{output_folder}/{channel}/{channel}_messages.json'
		obj = json.dumps(
			data,
			ensure_ascii=False,
			separators=(',',':')
		)
		
		# writer
		writer = open(file_path, mode='w', encoding='utf-8')
		writer.write(obj)
		writer.close()
		print ('> done.')
		print ('')
	
		# sleep program for a few seconds
		if len(req_input) > 1:
			print('sleeping for 20 secs')
			# time.sleep(1)
			time.sleep(20)

	
	
	handled_channels_counter += 1
	print("Handled Channels: " + str(handled_channels_counter) + " of " + str(max_channels) + " in " + str(time.time() - starttime) + "s")


'''

Clean generated chats text file

'''

# # close chat file
# chats_file.close()
# # get collected chats
# collected_chats = list(set([
# 	i.rstrip() for i in open(chats_path, mode='r', encoding='utf-8')
# ]))
# # re write collected chats
# chats_file = open(chats_path, mode='w', encoding='utf-8')
# for c in collected_chats:
# 	chats_file.write(f'{c}\n')
# # close file
# chats_file.close()
# # Process counter
# counter_df = pd.DataFrame.from_dict(
# 	counter,
# 	orient='index'
# ).reset_index().rename(
# 	columns={
# 		'index': 'id'
# 	}
# )
# # save counter
# counter_df.to_csv(
# 	f'{output_folder}/counter.csv',
# 	encoding='utf-8',
# 	index=False
# )
# # merge dataframe
# df = pd.read_csv(
# 	f'{output_folder}/collected_chats.csv',
# 	encoding='utf-8'
# )
# del counter_df['username']
# df = df.merge(counter_df, how='left', on='id')
# df.to_csv(
# 	f'{output_folder}/collected_chats.csv',
# 	index=False,
# 	encoding='utf-8'
# )
# # log results
# text = f'''
# End program at {time.ctime()}
# '''
# print (text)