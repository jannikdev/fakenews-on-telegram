# -*- coding: utf-8 -*-

# import Telethon API modules
import imp
from telethon import TelegramClient, types
from telethon.tl.functions.channels import GetChannelsRequest, \
	GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetHistoryRequest, \
	GetDiscussionMessageRequest, GetWebPageRequest, SearchRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.photos import GetUserPhotosRequest
from telethon.tl.functions.stats import GetBroadcastStatsRequest
from telethon.sync import TelegramClient
from telethon import functions, types
from datetime import datetime
import time

'''

Client-side

'''

# get connection
async def get_connection(session_file, api_id, api_hash, phone):
	'''
	Connects to Telegram API
	'''
	# client = TelegramClient(session_file, api_id, api_hash)
	client = TelegramClient(session_file, api_id, api_hash)
	await client.connect()
	if await client.is_user_authorized():
		print ('> Authorized!')
	else:
		print ('> Not Authorized! Sending code request...')
		await client.send_code_request(phone)
		await client.sign_in(
			phone,
			input('Enter the code: ')
		)
	# await client.start(bot_token='5759577145:AAGHA7opPAf2dJGzqFiwuNUQnUS7svHRQrY')

	return client


'''

Channels

'''

# get telegram channel main attrs 
async def get_entity_attrs(client, source):
	# async with client.takeout() as takeout:
		'''
		Source: entity (str | int | Peer | InputPeer)
			More on InputPeer: https://tl.telethon.dev/types/input_peer.html

		Reference:
			Telethon: https://docs.telethon.dev/en/latest/modules/client.html#telethon.client.users.UserMethods.get_entity
			Output attrs: https://core.telegram.org/constructor/channel

		'''
		time.sleep(1)
		return await client.get_entity(source)

# get channel request
async def get_channel_req(client, source):
	# async with client.takeout() as takeout:
		'''
		Source: <ChannelInput>

		Reference:
			Telethon: https://tl.telethon.dev/methods/channels/get_channels.html
			Output attrs: https://core.telegram.org/constructor/chat
		'''
		if type(source) != list:
			source = [source]
		print("GetChannelsRequest")
		time.sleep(1)
		return await client(
			GetChannelsRequest(source)
		)

# get full channel request
async def full_channel_req(client, source):
	# async with client.takeout() as takeout:
		'''
		Source: <ChannelInput>

		Reference:
			Telethon: https://tl.telethon.dev/methods/channels/get_full_channel.html
			Output attrs: https://core.telegram.org/constructor/messages.chatFull
		'''
		print("GetFullChannelRequest")
		time.sleep(1)
		try:
			return await client(
				GetFullChannelRequest(source)
			)
		except Exception:
			print(Exception)

# get participants request
async def get_participants_request(client, source):
	# async with client.takeout() as takeout:
		'''
		'''
		print("GetParticipantsRequest")
		return await client(
			GetParticipantsRequest(
				channel=source,
				filter=types.ChannelParticipantsRecent(),
				offset=1,
				limit=10,
				hash=0
			)
		)


'''

Messages

'''

# get posts
async def get_posts(client, source, min_id=0, offset_id=0):
	# async with client.takeout() as takeout:
		'''
		Source: entity (str | int | Peer | InputPeer)
			More on InputPeer: https://tl.telethon.dev/types/input_peer.html

		Reference:
			Telethon: https://tl.telethon.dev/methods/messages/get_history.html
			Output attrs: https://core.telegram.org/constructor/messages.channelMessages
		'''
		print("SearchRequest")
		return await client(
				SearchRequest(
				peer=source,
				hash=0,
				q='Ivermectin',
				limit=100,
				max_id=0,
				min_id=min_id,
				offset_id=offset_id,
				add_offset=0,
				min_date=datetime(2010, 6, 25),
				max_date=datetime(2023, 1, 1),
				filter=types.InputMessagesFilterEmpty()
				)
		)
		# results=[]
		# async for message in client.iter_messages(
		# 		# peer=source,
		# 		# hash=0,
		# 		wait_time=1,
		# 		search='Ivermectin',
		# 		limit=100,
		# 		max_id=0,
		# 		min_id=min_id,
		# 		offset_id=offset_id,
		# 		add_offset=0,
		# 		entity=source,
		# 		# min_date=datetime(2018, 6, 25),
		# 		# max_date=datetime(2022, 6, 25),
		# 		filter=types.InputMessagesFilterEmpty()):
		# 		results.append(message)
		# return results
	

# get full chat request
async def get_discussion_message(client, source, msg_id):
	# async with client.takeout() as takeout:
		'''
		Source: entity (str | int | Peer | InputPeer)
			More on InputPeer: https://tl.telethon.dev/types/input_peer.html
		msg_id: <message id>

		Reference:
			Telethon: https://tl.telethon.dev/methods/messages/get_discussion_message.html
			Output attrs: https://core.telegram.org/constructor/messages.discussionMessage
		'''
		print("GetDiscussionMessageRequest")
		return await client(
			GetDiscussionMessageRequest(
				peer=source,
				msg_id=msg_id
			)
		)

# get webpage
async def get_web_page(client, url, hash):
	'''
	url: <web url>
	hash: <pagination> adding 0 by default.

	Reference:
		Telethon: https://tl.telethon.dev/methods/messages/get_web_page.html
		Output attrs: https://core.telegram.org/constructor/webPage
	'''
	print("GetWebPageRequest")
	return await client(
		GetWebPageRequest(url, hash)
	)


'''

Users

'''

# get full user request
async def full_user_req(client, source, channel):
	'''
	Source: <InputUser>

	Reference:
		Telethon: https://tl.telethon.dev/methods/users/get_full_user.html
		Output attrs:
	'''
	try:
		print("GetFullUserRequest")
		user = await client(
			GetFullUserRequest(source)
		)
		return user
	except ValueError:
		users = await client.get_participants(channel, aggressive=True)
		return users


'''

Photos

'''

# photos request
async def photos_request(client, user_input):
	'''
	'''
	print("GetUserPhotosRequest")
	return await client(
		GetUserPhotosRequest(
			user_id=user_input,
			offset=0,
			max_id=0,
			limit=5
		)
	)


'''

Stats

'''

async def broadcast_stats_req(client, source):
	'''

	Source: <InputChannel>

	Reference:
		Telethon: https://tl.telethon.dev/methods/stats/get_broadcast_stats.html
		Output attrs: https://core.telegram.org/constructor/stats.broadcastStats
	'''
	print("GetBroadcastStatsRequest")
	return await client(
		GetBroadcastStatsRequest(
			channel=source
		)
	)

async def get_public_forwards(client, msg_id, channel):
    return await client(functions.stats.GetMessagePublicForwardsRequest(
        msg_id=msg_id,
		channel=channel,
		offset_rate=0,
		offset_id=0,
		offset_peer=channel,
        limit=100
    ))


    result = client(functions.stats.GetMessagePublicForwardsRequest(
        channel='username',
        msg_id=42,
        offset_rate=42,
        offset_peer='username',
        offset_id=42,
        limit=100
    ))