import csv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import VARCHAR, Boolean, Column, DateTime, ForeignKey, Integer
from sqlalchemy import update
from sqlalchemy import func, delete
Base = declarative_base()

class Tg(Base):
    __tablename__ = 'tele_content'
    id = Column(Integer, primary_key=True)
    username = Column("username", VARCHAR(1024))
    channel_id = Column("channel_id", Integer)
    msg_id = Column('msg_id', Integer)
    #nlp_id = Column(Integer, ForeignKey('nlp.id'),  nullable=True)
    message = Column('message', VARCHAR(16384)) 
    #message = Column('message', VARCHAR CHECK(length(message) <=500)
    date = Column('date', DateTime)
    #date = Column('date', VARCHAR(1024))
    signature = Column("signature", VARCHAR(1024))
    msg_link = Column('msg_link', VARCHAR(1024))
    views = Column('views', VARCHAR(32))
    number_replies = Column('number_replies', VARCHAR(32))
    number_forwards  = Column('number_forwards', VARCHAR(32))
    is_forward =  Column('is_forward', VARCHAR(32))
    forward_msg_date = Column('forward_msg_date', DateTime)
    forward_msg_date_string =Column('forward_msg_date_string', VARCHAR(32))
    forward_msg_link = Column('forward_msg_link', VARCHAR(1024))
    from_channel_id = Column("from_channel_id", Integer)
    from_channel_name = Column("from_channel_name", VARCHAR(1024))
    is_reply =  Column("is_reply", VARCHAR(32))
    reply_to_msg_id = Column("reply_to_msg_id", VARCHAR(32))
    reply_msg_link = Column('reply_msg_link', VARCHAR(1024))
    contains_media  = Column('contains_media', VARCHAR(32))
    media_type = Column('media_type', VARCHAR(1024))
    # has_url  = Column('has_url', VARCHAR(1024))
    # url  = Column('url', VARCHAR(1024))
    # domain  = Column('domain', VARCHAR(1024))
    # url_title  = Column('url_title', VARCHAR(1024))
    # url_description  = Column('url_description', VARCHAR(1024))
    # document_type = Column('document_type', VARCHAR(1024))
    # video_duration_secs = Column('video_duration_secs', VARCHAR(1024))
    # poll_question = Column('poll_question', VARCHAR(1024))
    # poll_number_results = Column('poll_number_results', VARCHAR(1024))
    # contact_phone_number = Column('contact_phone_number', VARCHAR(1024))
    # contact_name = Column('contact_name', VARCHAR(1024))
    # contact_userid = Column('contact_userid', VARCHAR(1024))
    # geo_shared_lat = Column('geo_shared_lat', VARCHAR(1024))
    # geo_shared_lng = Column('geo_shared_lng', VARCHAR(1024))
    # geo_shared_title = Column('geo_shared_title', VARCHAR(1024))
    # geo_shared_address = Column('geo_shared_address', VARCHAR(1024))

class Tg_channel(Base):
    __tablename__ = 'tele_channel'
    id = Column(Integer, primary_key=True)
    username = Column("username", VARCHAR(1024))
    channel_id = Column("channel_id", VARCHAR(32))
    max_id = Column('max_id', Integer)
    craw_id = Column('craw_id', Integer)

class NLP(Base):
    __tablename__ = 'nlp'
    id = Column('id', Integer, primary_key=True)
    Category = Column('category', VARCHAR(255))

class All_content(Base):
    __tablename__ = 'all_content'
    id = Column(Integer, primary_key=True)
    content_id = Column("content_id", Integer)
    network_id = Column("network_id", Integer)
    nlp_id = Column('nlp_id', Integer)
    ht_check = Column('ht_check', VARCHAR(32))
    keyword_check = Column('keyword_check', VARCHAR(32))



engine = create_engine('postgresql://fbs:yah7WUy1Oi8G@172.32.253.129:5432/fbs', echo=False)
#Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()
global count 
global count1
def savetodb():
    with open('./output/data/msgs_dataset.csv', 'r', encoding="utf8") as file:
    # Create a CSV reader object
        reader = csv.reader(file)
        next(reader)
    # Iterate over each row in the CSV file
        count = 0
        count1 = 0
        for row in reader:
            Username = row[0]  # Assuming username is in the first column
            Channel_id = row[1]
            Message = row[2]
            Date = row[3]
            Signature = row[4]
            Msg_link = row[5]
            Msg_id = row[6]  
            Views = row[7] 
            Number_replies = row[8] 
            Number_forwards  = row[9] 
            Is_forward = row[10] 
            Forward_msg_date = row[11] 
            Forward_msg_date_string = row[12] 
            Forward_msg_link = row[13] 
            From_channel_id = row[14] 
            From_channel_name = row[15] 
            Is_reply = row[16] 
            Reply_to_msg_id = row[17] 
            Reply_msg_link = row[18] 
            Contains_media  = row[19] 
            Media_type = row[20] 

            same_id =session.query(Tg.id).filter(Tg.username == Username,Tg.msg_id == Msg_id ).first()  # check msg_id 
            #print(same_id)
            if same_id:
                #print(Msg_id)
                stmt = (update(Tg).where(Tg.msg_id == Msg_id).values(views=Views)) # update views
                session.execute(stmt, execution_options={"synchronize_session": False})
                session.commit() 
                #session.rollback()
                count += 1
            else:
                #same_username =session.query(Tg_channel.id).filter(Tg_channel.username == Username).first()
                new_ct= Tg(username=Username, channel_id=Channel_id, message=Message, date=Date, signature=Signature, msg_link=Msg_link, msg_id=Msg_id, views=Views,
                            number_replies=Number_replies, number_forwards=Number_forwards, is_forward=Is_forward, forward_msg_date=Forward_msg_date,
                            forward_msg_date_string=Forward_msg_date_string, forward_msg_link=Forward_msg_link, from_channel_id=From_channel_id,
                            from_channel_name=From_channel_name, is_reply=Is_reply, reply_to_msg_id=Reply_to_msg_id, reply_msg_link=Reply_msg_link,
                            contains_media=Contains_media, media_type=Media_type )
                session.add(new_ct)
                session.commit() 
                #session.rollback()
                count1 += 1
                latest_id =session.query(func.max(Tg.id)).first()
                add_id = All_content(content_id = latest_id[0], network_id =3)
                session.add(add_id)
                session.commit() 
                #session.rollback()
                # session.commit() 
                # session.rollback()
                # last_id =session.query(func.max(Tg.id)).first()
                # add_all_contents = all_content(content_id=last_id,network_id=2)
                # session.add(add_all_contents)
                # session.commit() 
                # session.rollback()
        print("Updated Content : " + str(count))
        print("Added Content : " + str(count1))
        print("Sucessfully Added to Db")
    session.commit() 
    session.rollback()

 