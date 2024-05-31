from googleapiclient.discovery import build
import googleapiclient.discovery
#! pip install google-api-python-client
from pprint import pprint
#import build
import pandas as pd
#! pip install psycopg2
import psycopg2
import sqlalchemy
import streamlit as st
from sqlalchemy import create_engine

#establish api connection
api_service_name = "youtube"
api_version="v3"
api_key = 'AIzaSyCSSHeo0DolzGXVxtvoET7w_9aOkCqRDdo'
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

all_channel_data = []

#Taking user input for channel id

chx=st.text_input("Enter channel id:")
st.write('Sample channel ids and names for input')
st.write('UClLLnEeB_Shcop7zUlQo3hg -Annalakshmi')
st.write('UC7lENXBdm8TS0qyqWq69IuQ -Varun')
st.write('UC9mDoXFAhm49JJ26IohvAKQ -Synergy Decor')
        
with st.sidebar:
    st.title(":red[Welcome to Annalakshmi's YOUTUBE PROJECT]")
    st.header("Knowledge gained")
    st.header("Python")
    st.header("API connection")
    st.header("Pandas")
    st.header("PGSQL")
    
    st.header("Streamlit")
    
#Function to get Channel Stats

def get_channel_stats(chx):
    lakshmi=[]
    request = youtube.channels().list(
                part = 'snippet,contentDetails,statistics',
                id =chx)
    response=request.execute()

    for i in range(len(response['items'])):
      data1= dict(Channel_name = response['items'][0]['snippet']['title'],
                  channel_id = response["items"][0]["id"],
              Description=response['items'][0]['snippet']['description'],
              Channel_Published_Date = response["items"][0]["snippet"]["publishedAt"],
              Thumbnails= response['items'][0]['snippet']['thumbnails']['high']['url'],
                Subscribers = response['items'][0]['statistics']['subscriberCount'],
                Views = response['items'][0]['statistics']['viewCount'],
                Total_videos=response['items'][0]['statistics']['videoCount'],
                      play_list_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
      lakshmi.append(data1)
    return lakshmi



# Connecting  to the database using psycopg2
conn = psycopg2.connect(
    host='localhost',
    user='postgres',
    database='postgres',
    password='user',
    port=5432
)
cursor = conn.cursor()
# Creating an engine using SQLAlchemy
engine1 = create_engine("postgresql://postgres:user@localhost:5432/postgres")

def get_video_ids(chx):
  video_ids=[]
  request=youtube.channels().list(id =chx,
                                  part='contentDetails')
  response=request.execute()

  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token=None
  while True:
      request1=youtube.playlistItems().list(
        part='snippet',
        playlistId=Playlist_Id,
        maxResults=50,
        pageToken=next_page_token)

      response1=request1.execute()
      for i in range(len(response1['items'])):
        video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token=response1.get('nextPageToken')

      if next_page_token is None:
        break
  return video_ids


def get_video_info(video_idss):
  video_data=[]
  for video_id in video_idss:
    request=youtube.videos().list(
        part="snippet,ContentDetails,statistics",
        id=video_id
    )
    response=request.execute()

    for item in response['items']:
      data=dict(channel_name=item['snippet']['channelTitle'],
                channel_Id=item['snippet']['channelId'],
                Title=item['snippet']['title'],
                      video_id = item["id"],
                      Description=item.get('description'),
                      video_Published_Date = item["snippet"]["publishedAt"],
                      Thumbnails= item['snippet']['thumbnails']['high']['url'],
                      Views = item['statistics'].get('viewCount',0),
                      likes = item['statistics'].get('likeCount',0),
                      comments=item['statistics'].get('commentCount', 0),
                      favourite_count=item['statistics'].get('favoriteCount', 0),
                      #tag=item['contentDetails']['tags'],
                      Duration=item['contentDetails']['duration'],
                      Definition=item['contentDetails']['definition'],
                      caption_status=item['contentDetails']['caption']
                      )
      video_data.append(data)
  return video_data



def get_comment_info(video_idss):
  comment_data=[]
  try:
      for video_id in video_idss:
        request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50

        )
        response=request.execute()
        for item in response['items']:
          data1=dict(comment_id=item['snippet']['topLevelComment']['id'],
                    video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt']

                    )
          comment_data.append(data1)

  except:
    pass
  return comment_data

# To ensure code doesnt run unless id input is given
if(chx!=''):
  chann_det=get_channel_stats(chx)
  cha_dff=pd.DataFrame(chann_det)
  
  # Insert data into the database table "vid"
  cha_dff.to_sql("table1", engine1, if_exists='replace', index=False)
  df1 = pd.read_sql("SELECT * FROM table1", conn)
  df1.columns = ['Channel_name', 'channel_id', 'Description', 'Channel_Published_Date', 'Thumbnails', 'Subscribers', 'Views', 'Total_videos', 'play_list_id']

  st.write('Channel details')
  st.dataframe(df1)
  video_idss=get_video_ids(chx)
  
  video_details=get_video_info(video_idss)
  
  video_df=pd.DataFrame(video_details)   #by passing channel id should get this video details. user input is needed
  #video_df
  video_df['Views'] = video_df['Views'].astype(int)
  video_df['likes'] = video_df['likes'].astype(int)
  video_df['comments'] = video_df['comments'].astype(int)

  video_df.to_sql("table2", engine1, if_exists='replace', index=False)
  df2 = pd.read_sql("SELECT distinct * FROM table2", conn)
  conn.commit()

  df2.columns = ['channel_name', 'channel_Id', 'Title', 'video_id', 'Description', 'video_Published_Date', 'Thumbnails', 'Views', 'likes', 'comments', 'favourite_count', 'Duration', 'Definition', 'caption_status']

  video_df.to_sql("table22", engine1, if_exists='append', index=False)
  df22 = pd.read_sql("SELECT distinct * FROM table22", conn)

  df22.columns = ['channel_name', 'channel_Id', 'Title', 'video_id', 'Description', 'video_Published_Date', 'Thumbnails', 'Views', 'likes', 'comments', 'favourite_count', 'Duration', 'Definition', 'caption_status']

  if st.button('Video details'):
    st.dataframe(df2)
    
  comment_detail=get_comment_info(video_idss)
  comments_details=pd.DataFrame(comment_detail)

  comments_details.to_sql("table3", engine1, if_exists='replace', index=False)
  conn.commit()

  df3 = pd.read_sql("SELECT distinct * FROM table3", conn)
  df3.columns = ['comment_id', 'video_id', 'comment_text', 'comment_author', 'comment_published']
  comments_details.to_sql("table33", engine1, if_exists='append', index=False)
  conn.commit()

  

  if st.button('Comment details'):
    st.dataframe(df3)
    
  conn.commit()

  if st.button('All Channel Details'):
      cha_dff.to_sql("table4", engine1, if_exists='append', index=False)
      df4 = pd.read_sql("SELECT distinct * FROM table4", conn)
      df4.columns = ['Channel_name', 'channel_id', 'Description', 'Channel_Published_Date', 'Thumbnails', 'Subscribers', 'Views', 'Total_videos', 'play_list_id']
      st.dataframe(df4)


    
selectedoption = st.selectbox('Select your SQL query ? ', [
'Choose Option from dropdown',
'1. What are the names of all the videos and their corresponding channels?', 
'2. Which channels have the most number of videos, and how many videos do they have?', 
'3. What are the top 10 most viewed videos and their respective channels?', 
'4. How many comments were made on each video, and what are their corresponding video names?', 
'5. Which videos have the highest number of likes, and what are their corresponding channel names?', 
'6. What is the total number of likes and dislikes for each video, and what are  their corresponding video names?', 
'7. What is the total number of views for each channel, and what are their corresponding channel names?', 
'8 What are the names of all the channels that have published videos in the year 2022?', 
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?', 
'10. Which videos have the highest number of comments, and what are their corresponding channel names?']) 

def executequery(qr,col):
    cursor = conn.cursor()
    cursor.execute(qr)
    rows=cursor.fetchall()
    
    cha_df=pd.DataFrame(rows)
    cha_df.columns=col
    st.dataframe(cha_df) 
    return
    
    

if selectedoption=='Choose Option from dropdown':
    st.write()
if selectedoption == '1. What are the names of all the videos and their corresponding channels?':
    select_qry = """
    select distinct "channel_name" , "Title" from public.table22  
    """
    col=["channel name","Title"]
    executequery(select_qry,col)
    
  

if selectedoption == '2. Which channels have the most number of videos, and how many videos do they have?':
    
    select_qry = """
    select "channel_name",count("Title") as Videocount
    from (select distinct * from public.table22)
    group by "channel_name"
    order by Videocount desc;
    """
    col=["channel name","Max_videos"]
    executequery(select_qry,col)
    
    
if selectedoption == '3. What are the top 10 most viewed videos and their respective channels?':
    select_qry = """
    select "channel_name", "Title","Views" 
    from (select distinct * from public.table22)
    order by "Views" desc
    limit 10;
    """
    col=["channel name","Title","Most_viewed"]
    executequery(select_qry,col)

if selectedoption == '4. How many comments were made on each video, and what are their corresponding video names?':
    
    select_qry = """
    select "Title",cast("comments" as integer) as allcomn from (select distinct * from public.table22)
    order by allcomn desc
    limit 5;

    """
    col=["Video_name","Total_comments"]
    executequery(select_qry,col)
    
if selectedoption == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    
    
    select_qry = """  
    Select "channel_name", "Title","likes" from (select distinct * from  public.table22)
    order by "likes" desc 
    LIMIT 5; 
    """
    
    col=["channel name","Title","Highest_likes"]
    executequery(select_qry,col)
    
if selectedoption =='6. What is the total number of likes and dislikes for each video, and what are  their corresponding video names?':
    
    select_qry = """   
    Select "Title", "likes" from(select distinct * from public.table22) 
    order by "likes" desc;
    """
    col=["Video_names","Total__likes"]
    executequery(select_qry,col)
    
if selectedoption=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    
    select_qry = """   
    Select "Channel_name", "Views" from (select distinct * from public.table4) 
    order by "Views" desc
    """
    col=["channel name","Views"]
    executequery(select_qry,col)
  
  
if selectedoption =='8 What are the names of all the channels that have published videos in the year 2022?':
    
    select_qry = """   
    Select "Channel_name", "Channel_Published_Date" from(select distinct * from public.table4)
    where "Channel_Published_Date" >= '2022-01-01' and "Channel_Published_Date" < '2023-01-01'; 
    """
    col=["channel name","Published _Date"]
    executequery(select_qry,col)
    

    
if selectedoption =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    
    select_qry = """   
    select "channel_name","Title","comments" from(select distinct * from public.table22)
    order by "comments" desc
    limit 1; 
    """
    col=["channel name","Title","Highest_comments"]
    executequery(select_qry,col)
