import googleapiclient.discovery
from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import datetime
from datetime import datetime
import isodate
import streamlit as st


# CONNECTING API KEY
def Api_Connect():
    api_key = "AIzaSyAxeUMmXaU9H2f_GGCfMiY82TwkpcgjjbQ"
    
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    
    return youtube

youtube = Api_Connect()


# COLLECTING CHANNEL INFO

def Channel_Info(channel_id):
    request = youtube.channels().list(
            part= "snippet,contentDetails,statistics",
            id= channel_id
        )
    response = request.execute()

    for i in response["items"]:
        info = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    Subscribers_Count = i["statistics"]["subscriberCount"],
                    Channel_Views = i["statistics"]["viewCount"],
                    Videos_Count = i["statistics"]["videoCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return info


# COLLECTING VIDEO IDS
def Video_Ids(channel_id):
    video_ids = []
    request = youtube.channels().list(part = "contentDetails",
                                     id = channel_id )
    response = request.execute()
    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None

    while True:
        request1 = youtube.playlistItems().list(part = "snippet",
                                                playlistId = Playlist_Id,
                                                maxResults = 50,
                                                pageToken = next_page_token)
        response1 = request1.execute()

        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids


# COLLECTING VIDEO INFO
def Video_Info(video_ids):
    video_info = []
     
    
    for video_id in video_ids:
        request = youtube.videos().list(part = "snippet,contentDetails,statistics",
                                        id = video_id
                                        ) 
        response = request.execute()

        for i in response["items"]:
            info = dict(Channel_Name = i["snippet"]["channelTitle"],
                        Channel_Id = i["snippet"]["channelId"],
                        Video_Id = i["id"],
                        Video_Name = i["snippet"]["title"],
                        Tags = i["snippet"].get("tags"),
                        Thumbnail = i["snippet"]["thumbnails"]["default"]["url"],
                        Video_Description = i["snippet"]["description"],
                        Published_At = i["snippet"]["publishedAt"],
                        Duration = i["contentDetails"]["duration"],
                        View_Count = i["statistics"]["viewCount"],
                        Like_Count = i["statistics"]["likeCount"],
                        Favorite_Count = i["statistics"]["favoriteCount"],
                        Comment_Count = i["statistics"]["commentCount"],
                        Caption_Status = i["contentDetails"]["caption"]
                        )
            video_info.append(info)
    
     
    return video_info


# COLLECTING COMMENTS INFO
def Comment_Info(x):
    Comment_infos=[]
    try:
        for video_id in x:
            request = youtube.commentThreads().list(part = "snippet",
                                                   videoId = video_id,
                                                   maxResults = 50
            )
            response = request.execute()

            for i in response["items"]:
                infos = dict(Comment_Id = i["snippet"]["topLevelComment"]["id"],
                            Video_Id = i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            Comment_Text = i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            Comment_Author = i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            Comment_Published = i["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                Comment_infos.append(infos)
    except:
        pass
    return Comment_infos

# COLLECTING PLAYLIST INFO
def Playlist_Info(channel_id):
    next_page_token = None
    playlist_info = []
    while True:
        request = youtube.playlists().list(
                    part = "snippet, contentDetails",
                    channelId = channel_id,
                    maxResults = 50,
                    pageToken = next_page_token
                  )

        response = request.execute()

        for i in response["items"]:
            info = dict(Playlist_Id = i["id"],
                       Title = i["snippet"]["title"],
                       Channel_Id =i["snippet"]["channelId"],
                       Channel_Name = i["snippet"]["channelTitle"],
                       PublishedAt = i["snippet"]["publishedAt"],
                       Video_Count = i["contentDetails"]["itemCount"])
            playlist_info.append(info)
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    return playlist_info


#CONNECTING MONGODB
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client["youtube_data"]

def channel_Details(channel_id):
    ch_info = Channel_Info(channel_id)
    pl_info = Playlist_Info(channel_id)
    vi_ids = Video_Ids(channel_id)
    vi_info = Video_Info(vi_ids)
    com_info = Comment_Info(vi_ids)
    
    col = db["channel_details"]
    col.insert_one({"channel_info" : ch_info,
                    "playlist_info" : pl_info,
                    "video_info" : vi_info,
                    "comment_info" : com_info})
    return "completed"

# channel_t1 = channel_Details("UC5EQWvy59VeHPJz8mDALPxg") # => MICSET
# channel_t2 = channel_Details("UCMyi2FZAUNLFDUE8ZIf7YOQ") # => SEESAW 
# channel_t3 = channel_Details("UCD1CuflSWxeAGVOwicubsWw") # => SOTHANAIGAL
# channel_t4 = channel_Details("UChU0bqGmHJlYEfNnIhqCJQA") # => MC ENTERTAINMENT
# channel_t5 = channel_Details("UC0Gb9wxH8cvheXi_X9q5Isw") # => NAAKOUT
# channel_t6 = channel_Details("UCNfvY8ycvr_ffvPiBWTJp0g") # => SOUND SETTAI
# channel_t7 = channel_Details("UCS3c38DmZc_eA3wlp3uHqIw") # => HARI BASKAR
# channel_t8 = channel_Details("UCwr-evhuzGZgDFrq_1pLt_A") # => ERROR MAKES CLEVER ACADEMY
# channel_t9 = channel_Details("UCjTlypR4Pu2SqLwYjxBO0Ug") # => PARATTAI PUGAZH
# channel_t10 = channel_Details("UCJcCB-QYPIBcbKcBQOTwhiA") # => VJ SIDDHU VLOGS

# CREATING TABLE FOR CHANNELS
def channel_table():
    connection = pymysql.connect(host = "127.0.0.1", 
                                 user = "root", 
                                 passwd = "Nitheesh@24", 
                                 database = "youtube_data"
                                 )
    cursor = connection.cursor()

    drop_qu = '''drop table if exists channels'''
    cursor.execute(drop_qu)
    connection.commit()

    
    create_tab = '''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(100) primary key,
                                                        Subscribers_Count bigint,
                                                        Channel_Views bigint,
                                                        Videos_Count int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(100))'''
    cursor.execute(create_tab)
    connection.commit()


    channel_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for channel_data in coll.find({},{"_id":0, "channel_info":1}):
        channel_list.append(channel_data["channel_info"])
    df = pd.DataFrame(channel_list)


    for index, row in df.iterrows():
        insert_qu = '''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers_Count,
                                            Channel_Views,
                                            Videos_Count,
                                            Channel_Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers_Count'],
                  row['Channel_Views'],
                  row['Videos_Count'],
                  row['Channel_Description'],
                  row['Playlist_Id'])
        
        cursor.execute(insert_qu, values)
        connection.commit()

# channel_tab = channel_table()


#CREATING PLAYLIST TABLE
def playlist_table():
    connection = pymysql.connect(host = "127.0.0.1", 
                                 user = "root", 
                                 passwd = "Nitheesh@24", 
                                 database = "youtube_data"
                                 )
    cursor = connection.cursor()

    drop_qu = '''drop table if exists playlists'''
    cursor.execute(drop_qu)
    connection.commit()


    create_play = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                         Title varchar(100),
                                                         Channel_Id varchar(100),
                                                         Channel_Name varchar(100),
                                                         PublishedAt datetime,
                                                         Video_Count int)'''
    cursor.execute(create_play)
    connection.commit()

    playlist_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for playlist_data in coll.find({},{"_id":0, "playlist_info":1}):
        for i in range(len(playlist_data["playlist_info"])):
            playlist_list.append(playlist_data["playlist_info"][i])
    df1 = pd.DataFrame(playlist_list)

    for index, row in df1.iterrows():
            insert_qu1 = '''insert into playlists(Playlist_Id,
                                                 Title,
                                                 Channel_Id,
                                                 Channel_Name,
                                                 PublishedAt,
                                                 Video_Count
                                                 )

                                                values(%s,%s,%s,%s,%s,%s)'''
            values = (row['Playlist_Id'],
                      row['Title'],
                      row['Channel_Id'],
                      row['Channel_Name'],
                      datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ'),
                      row['Video_Count']
                     )

            cursor.execute(insert_qu1, values)
            connection.commit()
            
# play_tab = playlist_table()


#CREATE VIDEO TABLE
def video_tables():
    connection = pymysql.connect(host = "127.0.0.1", 
                             user = "root", 
                             passwd = "Nitheesh@24", 
                             database = "youtube_data"
                             )
    cursor = connection.cursor()

    drop_qu = '''drop table if exists videos'''
    cursor.execute(drop_qu)
    connection.commit()


    create_videos = '''create table if not exists videos(Channel_Name varchar(255),
                                                         Channel_Id varchar(255),
                                                         Video_Id varchar(255) primary key,
                                                         Video_Name varchar(255),
                                                         Tags text,
                                                         Thumbnail varchar(300),
                                                         Video_Description text,
                                                         Published_At datetime,
                                                         Duration int,
                                                         View_Count int,
                                                         Like_Count int,
                                                         Favorite_Count int,
                                                         Comment_Count int,
                                                         Caption_Status varchar(255)
                                                         )'''
    cursor.execute(create_videos)
    connection.commit()

    video_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for video_data in coll.find({},{"_id":0, "video_info":1}):
        for i in range(len(video_data["video_info"])):
            video_list.append(video_data["video_info"][i])
    df2 = pd.DataFrame(video_list)

    for index, row in df2.iterrows():
        
        insert_qu2 = '''insert into videos(Channel_Name,
                                           Channel_Id,
                                           Video_Id,
                                           Video_Name,
                                           Tags,
                                           Thumbnail,
                                           Video_Description,
                                           Published_At,
                                           Duration,
                                           View_Count,
                                           Like_Count,
                                           Favorite_Count,
                                           Comment_Count,
                                           Caption_Status
                                           )

                                          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''



        values = (str(row['Channel_Name']),
                  str(row['Channel_Id']),
                  str(row['Video_Id']),
                  str(row['Video_Name']),
                  str(row['Tags']),
                  str(row['Thumbnail']),
                  str(row['Video_Description']),
                  datetime.strptime(row['Published_At'], '%Y-%m-%dT%H:%M:%SZ'),
                  int(isodate.parse_duration(row['Duration']).total_seconds()),
                  int(row['View_Count']),
                  int(row['Like_Count']),
                  int(row['Favorite_Count']),
                  int(row['Comment_Count']),
                  str(row['Caption_Status'])
                  )

        cursor.execute(insert_qu2, values)
        connection.commit()
        
# video_tab = video_tables()


#CREATING COMMENT TABLE
def comment_table():
    connection = pymysql.connect(host = "127.0.0.1", 
                                 user = "root", 
                                 passwd = "Nitheesh@24", 
                                 database = "youtube_data"
                                 )
    cursor = connection.cursor()

    drop_qu = '''drop table if exists comments'''
    cursor.execute(drop_qu)
    connection.commit()


    create_comment = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(100),
                                                            Comment_Text text,
                                                            Comment_Author varchar(200),
                                                            Comment_Published datetime)'''
    cursor.execute(create_comment)
    connection.commit()

    comment_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for comment_data in coll.find({},{"_id":0, "comment_info":1}):
        for i in range(len(comment_data["comment_info"])):
            comment_list.append(comment_data["comment_info"][i])
    df3 = pd.DataFrame(comment_list)

    for index, row in df3.iterrows():
        insert_qu3 = '''insert into comments(Comment_Id,
                                              Video_Id,
                                              Comment_Text,
                                              Comment_Author,
                                              Comment_Published
                                              )

                                            values(%s,%s,%s,%s,%s)'''
        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ')
                  )

        cursor.execute(insert_qu3, values)
        connection.commit()
        
# comment_tab = comment_table()


def tables():
    channel_table()
    playlist_table()
    video_tables()
    comment_table()
    
    return "Tables created succesfully"

# create_tables = tables()

def view_channels_table():
    channel_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for channel_data in coll.find({},{"_id":0, "channel_info":1}):
        channel_list.append(channel_data["channel_info"])
    df = st.dataframe(channel_list)
    
    return df


def view_playlists_table():
    playlist_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for playlist_data in coll.find({},{"_id":0, "playlist_info":1}):
        for i in range(len(playlist_data["playlist_info"])):
            playlist_list.append(playlist_data["playlist_info"][i])
    df1 = st.dataframe(playlist_list)
    
    return df1


def view_videos_table():
    video_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for video_data in coll.find({},{"_id":0, "video_info":1}):
        for i in range(len(video_data["video_info"])):
            video_list.append(video_data["video_info"][i])
    df2 = st.dataframe(video_list)
    
    return df2


def view_comments_table():
    comment_list = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for comment_data in coll.find({},{"_id":0, "comment_info":1}):
        for i in range(len(comment_data["comment_info"])):
            comment_list.append(comment_data["comment_info"][i])
    df3 = st.dataframe(comment_list)
    
    return df3


# STREMLIT

with st.sidebar:
    st.title(":Green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("REQUIREMENTS")
    st.caption("Text editor")
    st.caption("MongoDB")
    st.caption("Mysql")
    st.caption("API Intergration")
    
channel_id = st.text_input("Enter the channel ID")

if st.button("UPLOAD TO MONGODB"):
    ch_ids = []
    db = client["youtube_data"]
    coll = db["channel_details"]
    for ch_data in coll.find({},{"_id":0, "channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["Channel_Id"])
    
    if channel_id in ch_ids:
        st.success("channel datails are already exists")
    
    else:
        insert = channel_Details(channel_id)
        st.success(insert)

        
if st.button("MIGRATE TO SQL"):
    Table = tables()
    st.success(Table)
    
show_table =st.radio("SELECT THE TABLE TO VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    view_channels_table()
    
elif show_table == "PLAYLISTS":
    view_playlists_table()

elif show_table == "VIDEOS":
    view_videos_table()

elif show_table == "COMMENTS":
    view_comments_table()
    

# SQL CONNECTION 

connection = pymysql.connect(host = "127.0.0.1", 
                             user = "root", 
                             passwd = "Nitheesh@24", 
                             database = "youtube_data"
                             )
cursor = connection.cursor()

question = st.selectbox("select your question", ("1. Names of all the videos and their channels name",
                                                "2. Channels with most number of videos and their counts",
                                                "3. Top 10 most viewed videos and their channel name",
                                                "4. Comments count on each videos and their video names",
                                                "5. Videos with highest likes and their channel name",
                                                "6. Total number of likes of all each videos and their channel name",
                                                "7. Total number of views of each channel and their channel name",
                                                "8. Names of all the channels which publised videos in the year 2022",
                                                "9. Average duration of all videos in each channel and their channel name",
                                                "10. Videos with highest number of comments and their channel name"))

if question == "1. Names of all the videos and their channels name":
    qns1 =  '''select video_name as title, channel_name as channelname from videos'''
    cursor.execute(qns1)
    connection.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns = ["title", "channel name"])
    st.write(df)
    
elif question == "2. Channels with most number of videos and their counts":
    qns2 =  '''select channel_name as channelname, videos_count as no_of_videos from channels order by videos_count desc'''
    cursor.execute(qns2)
    connection.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns = ["channel name", "no of videos"])
    st.write(df2)
    
elif question == "3. Top 10 most viewed videos and their channel name":
    qns3 =  '''select view_count as no_of_views, channel_name as channelname, video_name as title from videos 
                where view_count is not null order by no_of_views desc limit 10'''
    cursor.execute(qns3)
    connection.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns = ["no_of_views", "channel name", "title"])
    st.write(df3)
    
elif question == "4. Comments count on each videos and their video names":
    qns4 =  '''select comment_count as no_of_comments, video_name as title from videos where comment_count is not null'''
    cursor.execute(qns4)
    connection.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4,columns = ["no_of_comments", "title"])
    st.write(df4)
    
elif question == "5. Videos with highest likes and their channel name":
    qns5 =  '''select video_name as title, channel_name as channelname, like_count as no_of_likes 
             from videos where like_count is not null order by no_of_likes desc'''
    cursor.execute(qns5)
    connection.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns = ["title", "channelname", "no_of_likes"])
    st.write(df5)

elif question == "6. Total number of likes of all each videos and their channel name":
    qns6 =  '''select like_count as no_of_likes, video_name as title from videos'''
    cursor.execute(qns6)
    connection.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6,columns = ["no_of_likes", "title"])
    st.write(df6)
    
elif question == "7. Total number of views of each channel and their channel name":
    qns7 =  '''select channel_views as no_of_views, channel_name as channelname from channels'''
    cursor.execute(qns7)
    connection.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7,columns = ["no_of_views", "channelname"])
    st.write(df7)
    
elif question == "8. Names of all the channels which publised videos in the year 2022":
    qns8 =  '''select video_name as title, published_at as date, channel_name as channelname from videos 
            where extract(year from published_at)=2022'''
    cursor.execute(qns8)
    connection.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8,columns = ["title", "date", "channelname"])
    st.write(df8)
    
elif question == "9. Average duration of all videos in each channel and their channel name":
    qns9 =  '''select channel_name as channelname, round(avg(duration), 0) as avgseconds from videos group by channel_name'''
    cursor.execute(qns9)
    connection.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9,columns = ["channelname", "avgseconds"])
    st.write(df9)
    
elif question == "10. Videos with highest number of comments and their channel name":
    qns10 =  '''select video_name as title, channel_name as channelname, comment_count as no_of_comments from videos 
                where comment_count is not null order by no_of_comments desc'''
    cursor.execute(qns10)
    connection.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10,columns = ["title", "channelname", "no_of_comments"])
    st.write(df10)