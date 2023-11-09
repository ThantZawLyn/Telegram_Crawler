import pandas as pd
import numpy as np
import os
import sqlalchemy as db
from random import randint
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import VARCHAR, Boolean, Column, DateTime, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import BYTEA, ENUM
from sqlalchemy.orm import relationship
from sqlalchemy import update,func
# import modules
import asyncio
import json
import time
#from datetime import datetime, timedelta, timezone
from pytz import timezone
import pytz
# import submodules
from tqdm import tqdm

# import local submodules
from utils import (
	msgs_dataset_columns, chats_dataset_columns, clean_msg,
	msg_attrs, get_forward_attrs, get_reply_attrs, get_url_attrs,
	get_document_attrs, get_poll_attrs, get_contact_attrs,
	get_geo_attrs, timestamp_attrs
)
# import Telegram API submodules
from api import *
from utils import (
	get_config_attrs, JSONEncoder, create_dirs, cmd_request_type,
	write_collected_chats
)

from build import *
from data_base import *

#engine = create_engine(SQLALCHEMY_DATABASE_URI)
engine = create_engine('postgresql://fbs:yah7WUy1Oi8G@172.32.253.129:5432/fbs', echo=False)

Base = declarative_base(bind=engine)
session_creator = sessionmaker()
DBSession = session_creator()

class Tg(Base):
    __tablename__ = 'tele_content'
    id = Column(Integer, primary_key=True)
    username = Column("username", VARCHAR(1024))
    channel_id = Column("channel_id", VARCHAR(32))
    msg_id = Column('msg_id', Integer)
    message = Column('message', VARCHAR(16384)) 
    nlp_id = Column(Integer, ForeignKey('nlp.id'),  nullable=True)
    #message = Column('message', VARCHAR CHECK(length(message) <=500)
    date = Column('date', DateTime)
    #date = Column('date', VARCHAR(1024))
    signature = Column("signature", VARCHAR(1024))
    msg_link = Column('msg_link', VARCHAR(1024))
    views = Column('views', VARCHAR(32))
    number_replies = Column('number_replies', VARCHAR(32))
    number_forwards  = Column('number_forwards', VARCHAR(32))
    is_forward =  Column('is_forward', VARCHAR(32))
    forward_msg_date = Column('forward_msg_date', VARCHAR(32))
    forward_msg_date_string =Column('forward_msg_date_string', VARCHAR(32))
    forward_msg_link = Column('forward_msg_link', VARCHAR(1024))
    from_channel_id = Column("from_channel_id", VARCHAR(32))
    from_channel_name = Column("from_channel_name", VARCHAR(1024))
    is_reply =  Column("is_reply", VARCHAR(1024))
    reply_to_msg_id = Column("reply_to_msg_id", VARCHAR(32))
    reply_msg_link = Column('reply_msg_link', VARCHAR(1024))
    contains_media  = Column('contains_media', VARCHAR(1024))
    media_type = Column('media_type', VARCHAR(1024))
   
class Tg_channel(Base):
    __tablename__ = 'tele_channel'
    id = Column(Integer, primary_key=True)
    username = Column("username", VARCHAR(1024))
    channel_id = Column("channel_id", VARCHAR(32))
    active = Column("active", VARCHAR(32))
    owner = Column('owner', Integer)
    max_id = Column('max_id', Integer)
    craw_id = Column('craw_id', Integer)


sfile = 'session_file'
api_id = 28578689
api_hash = '2c1013cffd21c7c2f5129e1fa1b46622'
phone = '+959676885111'
counter = {}

# event loop
loop = asyncio.get_event_loop()
'''
> Get Client <API connection>
'''
# get `client` connection
client = loop.run_until_complete(
	get_connection(sfile, api_id, api_hash, phone)
)

output_folder = './output/data'

# h=int(input("Input time(h) to crawl : "))
#retrieved from database
result=DBSession.query(Tg_channel.username,Tg_channel.craw_id).filter(Tg_channel.active == 'true').all()
for i in result:
    channel=i.username
    crawid= i.craw_id
    req_input = list(channel.split(" "))

    for channel in req_input:
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

        # Channel's attributes
        entity_attrs = loop.run_until_complete(
            get_entity_attrs(client, channel)
        )
        if entity_attrs:

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
            min_id = crawid
            posts = loop.run_until_complete(
                get_posts(client, channel_id, min_id=min_id)
                    )

            data = posts.to_dict()

            # Get offset ID | Get messages
            offset_id = min([i['id'] for i in data['messages']])

            while len(posts.messages) > 0:
                    
                if min_id:
                    posts = loop.run_until_complete(
                        get_posts(
                            client,
                            channel_id,
                            min_id=min_id,
                            offset_id=offset_id
                            )
                        )	
                    # Update data dict
                if posts.messages:
                    tmp = posts.to_dict()
                    data['messages'].extend(tmp['messages'])

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
                time.sleep(2)
        else:
            '''
            Channels not found
            '''
            exceptions_path = f'{output_folder}/_exceptions-channels-not-found.txt'
            w = open(exceptions_path, encoding='utf-8', mode='a')
            w.write(f'{channel}\n')
            w.close()

    # build dataset
    build()

    # delete blank message of photo content
    dataset = "./output/data/msgs_dataset.csv"
    d = pd.read_csv(dataset)
    #column_name = 'message'  # replace 'ColumnName' with the name of the column
    rows_to_keep = d[d['message'].str.len() > 3]
    rows_to_keep.to_csv('./output/data/msgs_dataset.csv', index=False)

    # save to db
    savetodb()
    '''
    Clean generated chats text file
    '''
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


    #date_obj = datetime.now()
    last_id =session.query(func.max(Tg.msg_id)).filter(Tg.username == channel ).first()
    #print(last_id[0])
    date_last_id = session.query(Tg.date).filter(Tg.msg_id == last_id[0],Tg.username == channel).first()
    #print(date_last_id[0])
    date_lastid_1 = date_last_id[0] - timedelta(hours=48)
    #print(date_lastid_1)
    craw_id = session.query(func.max(Tg.msg_id)).filter(Tg.date < date_lastid_1,Tg.username == channel ).first()
    #print(craw_id[0])
    stmt = (update(Tg_channel).where(Tg_channel.username == channel).values(max_id= last_id[0]))
    session.execute(stmt, execution_options={"synchronize_session": False})
    stmt1 = (update(Tg_channel).where(Tg_channel.username == channel).values(craw_id= craw_id[0]))
    session.execute(stmt1, execution_options={"synchronize_session": False})
    session.commit() 
    session.rollback()
        
    # delete json file
    target =  f'{output_folder}/{channel}/'
    for x in os.listdir(target):
        if x.endswith('.json'):
            os.unlink(target + x)  

    os.unlink('./output/data/msgs_dataset.csv') # delete csv file