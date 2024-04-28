import googleapiclient.discovery
from pymongo import MongoClient
import streamlit as st
import mysql.connector as SQLC
import os
import google_auth_oauthlib.flow
import googleapiclient.errors
import pandas as pd
import ast
from streamlit_autorefresh import st_autorefresh
from isodate import parse_duration

# Fetch channel details from YouTube API
api_service_name = "youtube"
api_version = "v3"
key = "AIzaSyDGcVSB3M22muPbLVmD_j4bvmmRFiPFExA"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=key)

st.header(" Youtube Data Harvesting ")
st.subheader("")
name = st.text_input("Channel ID")
st.write("Hint: Go to channel's page >> Right click >> view page source >> Find channel_id")
button_ext=st.button("Extract Data")
button_upload=st.button("Upload to MongoDB")



# Connect to MongoDB
client = MongoClient("mongodb+srv://thiru:12345ABC@cluster0.dfeend7.mongodb.net/?retryWrites=true&w=majority")
db = client.proj1
records = db.ydh
 
# Connect to MySQL
mydb = SQLC.connect(
    host="localhost",
    user="root",
    password="",
    database="project01"
)
mycursor = mydb.cursor()

def details_extraction():
    # Fetch channel details from YouTube API
    request = youtube.channels().list(
              part="id,snippet,contentDetails,brandingSettings,statistics",
              id=name)
    response = request.execute()

    channel_id = response['items'][0]['id']
    channel_title = response['items'][0]['snippet']['title']
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    view_count = response['items'][0]['statistics']['viewCount']
    subscribers_count = response['items'][0]['statistics']['subscriberCount']
    video_count = response['items'][0]['statistics']['videoCount']
    channel_details = {'channel details': {'channel_id': channel_id, 'channel_name': channel_title, 'videos': video_count,
                       'channel_views': view_count, 'subscribers': subscribers_count, 'playlist': playlist_id}}

    st.write(channel_details)
    
def mongo_migration():

    request = youtube.channels().list(
                    part="id,snippet,contentDetails,brandingSettings,statistics",
                    id=name)
    response = request.execute()

    channel_id = response['items'][0]['id']
    channel_title = response['items'][0]['snippet']['title']
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    view_count = response['items'][0]['statistics']['viewCount']
    subscribers_count = response['items'][0]['statistics']['subscriberCount']
    video_count = response['items'][0]['statistics']['videoCount']
    channel_details = {'channel details': {'channel_id': channel_id, 'channel_name': channel_title, 'videos': video_count,
                       'channel_views': view_count, 'subscribers': subscribers_count, 'playlist': playlist_id}}
    
    #Playlistitems

    playlistid = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    request1 = youtube.playlistItems().list(part ='id,contentDetails',playlistId=playlistid,maxResults=50)
    response1 = request1.execute()
    #st.write(response1) #it prints playlist details
    
    playlistitems_list=[]
    
    for i in range(0,50): 
        videoid = response1['items'][i]['contentDetails']['videoId']
        playlistitems_list.append(videoid)
        playlistitems_dict = {str(i): playlistitems_list[i] for i in range(len(playlistitems_list))}
    #st.write(playlistitems_list)
    playListids={"video_ids": playlistitems_list}
    df_playListids = pd.DataFrame(playListids)
    
    
    #videoIds
    videoidlist=[]
    videodetailsid=playlistitems_dict.values()
    new=videoidlist.append(videodetailsid)
        
    #videoDetails
    videodetaillist=[]
    for j in videodetailsid:
        request2 = youtube.videos().list(part="snippet,contentDetails,statistics",
                                           id=j)
        response2 = request2.execute()
        videoid=response2['items'][0]['id']
        videopublished = response2['items'][0]['snippet']['publishedAt']
        videoname = response2['items'][0]['snippet']['title']
        videodescription = response2['items'][0]['snippet']['description']
        videotumbnail = response2['items'][0]['snippet']['thumbnails']
        videoviews = response2['items'][0]['statistics']['viewCount']
        videolikes = response2['items'][0]['statistics'].get('likeCount',0)
        videofavorite = response2['items'][0]['statistics']['favoriteCount']
        videocomments = response2['items'][0]['statistics'].get('commentCount',0)
        videoduration = response2['items'][0]['contentDetails']['duration']
        
        
        videoinfo ={'video_details':{'VideoId':videoid,'VideoPublished':videopublished,'VideoTitle':videoname,
                                        'VideoDescription':videodescription,'VideoTumbnail':videotumbnail,'VideoViews':videoviews,
                                        'VideoLikes':videolikes,'VideoFavorite':videofavorite,'VideoComments':videocomments,
                                        'VideoDuration':videoduration}}
        
        videodetaillist.append(videoinfo)
        df_videoinfo = pd.DataFrame(videoinfo)
        df_videoinfo_swap = df_videoinfo.transpose()
        #st.write(df_videoinfo_swap)
    #st.write(videodetaillist)
    
    #CommentDetails
    commentdetailslist = []

    # Initialize a set to keep track of added comment IDs
    added_comment_ids = set()

    for k in playlistitems_list:
        try:
            request3 = youtube.commentThreads().list(part='snippet', videoId=k, maxResults=50)
            response3 = request3.execute()

            if 'items' in response3 and response3['items']:
                for item in response3['items']:
                    commentid = item['id']
                    # Check if the comment ID is already added and if the number of comments is less than 50
                    if commentid in added_comment_ids or len(commentdetailslist) >= 50:
                        continue  # Skip adding the comment if it's already in the list or if there are already 50 comments
                    else:
                        added_comment_ids.add(commentid)  # Add the comment ID to the set

                    commentvideoid = item['snippet']['videoId']
                    commenttext = item['snippet']['topLevelComment']['snippet']['textOriginal']
                    commentauthor = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    commentpublished = item['snippet']['topLevelComment']['snippet']['publishedAt']
                    commentlike = item['snippet']['topLevelComment']['snippet']['likeCount']
                    commentreplycount = item['snippet']['totalReplyCount']

                    commentinfo = {
                        'CommentId': commentid,
                        'VideoId': commentvideoid,
                        'Comment': commenttext,
                        'Author': commentauthor,
                        'PublishedAt': commentpublished,
                        'Likes': commentlike,
                        'ReplyCount': commentreplycount
                    }
                    commentdetailslist.append(commentinfo)
                    # Stop fetching comments if 50 comments are reached
                    if len(commentdetailslist) >= 50:
                        break
            else:
                # Handle the case when there are no comments in response['items']
                print("No comments found for video ID:", k)
        except Exception as e:
            # Handle the case when comments are disabled for the video
            print(f"Comments are disabled for video ID {k}: {e}")

    st.write(commentdetailslist)


    combined_data = {'Channel Detail':channel_details,'PlaylistIds':playListids,'VideoDetails':videodetaillist,'CommentDetails':commentdetailslist}
    # Check if the channel already exists in MongoDB
    existing_doc = records.find_one({'Channel Detail.channel details.channel_id': channel_id})
    if existing_doc:
        st.write(f"The channel '{channel_title}' already exists in the collection!")
        print("Channel Exists")
    else:
        records.insert_one(combined_data)
        st.write("Channel uploladed")
        st_autorefresh(limit = 1)

def dropdown():
    channel = records.find({})
    channel_id = set()
    for i in channel:
        channelid = i['Channel Detail']['channel details']['channel_id']
        channel_id.add(channelid)

    selectbox_chatid = st.selectbox('Select the channel you want to migrate to SQL',
                       sorted(channel_id))

    
    submit = st.button("Submit")
    if submit:
        cursor = records.find_one({"Channel Detail.channel details.channel_id": selectbox_chatid})
        
        #CHANNEL DETAILS:
        mycursor.execute("CREATE TABLE IF NOT EXISTS details (channelid VARCHAR (50),channelname VARCHAR(50),description VARCHAR(100),playlistid VARCHAR(50),channelviews VARCHAR(50),subscribers VARCHAR(50),videos VARCHAR (50))")
        mycursor.execute("CREATE TABLE IF NOT EXISTS videodetails (channelid VARCHAR (50),channelname VARCHAR(50),videoid VARCHAR(50),videopublished TIMESTAMP,videotitle VARCHAR(100),videodiscription VARCHAR(100),videotumbnail VARCHAR(100),videoviews INT(10),videolikes INT(10),videofavorite INT(10),videocomments INT(10),videoduration VARCHAR(10))")
        mycursor.execute("CREATE TABLE IF NOT EXISTS commentdetails (commentid VARCHAR(50),videoid VARCHAR(50),comment VARCHAR(70),author VARCHAR(50),published TIMESTAMP,likes INT(5),replycount INT(5))")

        if cursor:
            channel_id = cursor['Channel Detail']['channel details']['channel_id']
            channel_name = cursor['Channel Detail']['channel details']['channel_name']
            playlist_id = cursor['Channel Detail']['channel details']['playlist']
            channel_views = cursor['Channel Detail']['channel details']['channel_views']
            subscribers = cursor['Channel Detail']['channel details']['subscribers']
            videos = cursor['Channel Detail']['channel details']['videos']
            channel_views_int = int(channel_views)
            subscribers_int = int(subscribers)
            videos_int = int(videos)

            channel_sql = "INSERT INTO details (channelid, channelname, playlistid, channelviews, subscribers, videos) VALUES (%s, %s, %s, %s, %s, %s)"
            channel_val = (channel_id, channel_name, playlist_id, channel_views_int, subscribers_int, videos_int)
            mycursor.execute(channel_sql, channel_val)

            #VIDEO DETAILS:
            for o in range(0,50): 
                channel_id = cursor['Channel Detail']['channel details']['channel_id']
                channel_name = cursor['Channel Detail']['channel details']['channel_name']
                video_id = cursor['VideoDetails'][o]['video_details']['VideoId']
                video_published = cursor['VideoDetails'][o]['video_details']['VideoPublished']
                video_title = cursor['VideoDetails'][o]['video_details']['VideoTitle']
                video_description = cursor['VideoDetails'][o]['video_details']['VideoDescription']
                video_thumbnail = cursor['VideoDetails'][o]['video_details']['VideoTumbnail']['default']['url']
                video_views = int(cursor['VideoDetails'][o]['video_details']['VideoViews'])
                video_likes = int(cursor['VideoDetails'][o]['video_details']['VideoLikes'])
                video_favorite = int(cursor['VideoDetails'][o]['video_details']['VideoFavorite'])
                video_comments = int(cursor['VideoDetails'][o]['video_details']['VideoComments'])
                video_duration = cursor['VideoDetails'][o]['video_details']['VideoDuration']

                video_sql = "INSERT INTO videodetails (channelid,channelname, videoid, videopublished, videotitle, videodiscription, videotumbnail, videoviews, videolikes, videofavorite, videocomments, videoduration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s)"
                video_val = (channel_id,channel_name, video_id, video_published, video_title, video_description, video_thumbnail, video_views, video_likes, video_favorite, video_comments, video_duration)
                
                mycursor.execute(video_sql, video_val)
            
            for p in range(0,50):
                commentid = cursor['CommentDetails'][p]['CommentId']
                commentvideoid = cursor['CommentDetails'][p]['VideoId']
                commenttext = cursor['CommentDetails'][p]['Comment']
                commentauthor = cursor['CommentDetails'][p]['Author']
                commentpublished = cursor['CommentDetails'][p]['PublishedAt']
                commentlikes = int(cursor['CommentDetails'][p]['Likes'])
                commentreplycount = int(cursor['CommentDetails'][p]['ReplyCount'])

                comment_sql = " INSERT INTO commentdetails (commentid,videoid,comment,author,published,likes,replycount) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                comment_val = (commentid,commentvideoid,commenttext,commentauthor,commentpublished,commentlikes,commentreplycount)

                mycursor.execute(comment_sql,comment_val)

            # Insert channel details into the database
      
            mydb.commit()
            mycursor.close()
        

    return selectbox_chatid

dropdown()
if button_ext:
    details_extraction()
if button_upload:
    mongo_migration()

def queries():
    Qns = ['Q1. Title of all videos and their channel',
            'Q2. Which channel have most number of videos and their count',
            'Q3. Top 10 most viewed videos and their channel',
            'Q4. Comments made on each video and video titles',
            'Q5. Videos with high number of likes and their channel',
            'Q6. Likes for each video and its video titles',
            'Q7. Views obtained by each channel with channel name',
            'Q8. Names of all channels published videos in 2024',
            'Q9. Avg duration of all videos in each channel and channel names',
            'Q10. Videos with highest number of comments and their channel names' ]
    selectbox_chatid = st.selectbox('Select the channel you want to migrate to SQL',Qns)
    
    qnssubmit = st.button("Query Submit")
    if qnssubmit:
        if selectbox_chatid == Qns[0]: 
            #Q1
            mycursor.execute("SELECT channelid,channelname,videotitle FROM videodetails;")
            Table = mycursor.fetchall()
            channels = pd.DataFrame(Table,columns=['ChannelID','Channel Name','Video Name'])
            st.write(channels)

        elif selectbox_chatid == Qns[1]:
            #Q2 
            mycursor.execute("SELECT * FROM details WHERE videos = (SELECT MAX(videos) FROM details);")
            value =mycursor.fetchall()
            sqlmaxvideos = pd.DataFrame(value,columns=['Channelid','Channel Name','Description','PlaylistId','Channelviews','Subscribers','Videos'])
            st.write(sqlmaxvideos)

        elif selectbox_chatid == Qns[2]:
            #Q3
            mycursor.execute("SELECT videoviews,channelname FROM videodetails ORDER BY videoviews DESC,channelname LIMIT 10;")
            views = mycursor.fetchall()
            sqlvideoviews = pd.DataFrame(views,columns=['Views','Channel Name'])
            st.write(sqlvideoviews)

        elif selectbox_chatid == Qns[3]:
            #Q4
            mycursor.execute("SELECT videotitle,videocomments FROM videodetails;")
            sqlcommentcount = mycursor.fetchall()
            pd_sqlcommentcount = pd.DataFrame(sqlcommentcount,columns=['Video Title','Comment Count'])
            st.write(pd_sqlcommentcount)

        elif selectbox_chatid == Qns[4]:
            #Q5
            mycursor.execute("SELECT channelname,videolikes FROM videodetails ORDER BY videolikes DESC,channelname;")
            sqlvideolikes = mycursor.fetchall()
            pd_sqlvideolikes = pd.DataFrame(sqlvideolikes,columns=['Channel Name','Likes'])
            st.write(pd_sqlvideolikes)

        elif selectbox_chatid == Qns[5]:
            #Q6
            mycursor.execute("SELECT videotitle,videolikes FROM videodetails")
            sqlvideolikesandnames = mycursor.fetchall()
            pd_sqlvideolikesandnames = pd.DataFrame(sqlvideolikesandnames,columns=['Video Title','Likes'])
            st.write(pd_sqlvideolikesandnames)

        elif selectbox_chatid == Qns[6]:
            #Q7
            mycursor.execute("SELECT channelviews,channelname FROM details")
            sqlchannelviews = mycursor.fetchall()
            pd_sqlchannelviews = pd.DataFrame(sqlchannelviews,columns=['Channel Views','Channel Name'])
            st.write(pd_sqlchannelviews)

        elif selectbox_chatid == Qns[7]:
            #Q8
            mycursor.execute("SELECT videopublished,channelname FROM videodetails WHERE YEAR(videopublished)=2024")
            sqlvideopublished = mycursor.fetchall()
            pd_sqlvideopublished = pd.DataFrame(sqlvideopublished,columns=['Year','Channel Name'])
            st.write(pd_sqlvideopublished)

        elif selectbox_chatid == Qns[8]:
            #Q9
            def dur(duration_str):
                duration_seconds = parse_duration(duration_str).total_seconds()
                return duration_seconds

            mycursor.execute("SELECT channelname,videoduration FROM videodetails")
            sqldata = mycursor.fetchall()

            df = pd.DataFrame(sqldata,columns=['Channel Name','Duration'])
            df["dur_alter"]=df["Duration"].apply(dur)
            df

        elif selectbox_chatid == Qns[9]:
            #Q10
            mycursor.execute("SELECT videocomments,channelname FROM videodetails ORDER BY videocomments DESC,channelname;")
            sqldesccomments = mycursor.fetchall()
            pd_sqldesccomments = pd.DataFrame(sqldesccomments,columns=['Comment Count','Channel Name'])
            st.write(pd_sqldesccomments)
queries()
