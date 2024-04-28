# Youtube-Data-Harvesting
## Overview
This project is designed to extract data from YouTube channels using the YouTube Data API, store it in both MongoDB and MySQL databases, and perform various queries on the data.

## Features
- **Channel Details Extraction:** Extracts details such as channel ID, title, view count, subscriber count, video count, and related playlists.
- **MongoDB Migration:** Stores channel details, playlist IDs, video details, and comments in MongoDB.
- **MySQL Migration:** Stores channel details, video details, and comment details in MySQL.
- **Query Functionality:** Provides various predefined queries to retrieve specific information from the databases.

## Dependencies
- `googleapiclient.discovery`: For interacting with the YouTube Data API.
- `pymongo`: For MongoDB interaction.
- `mysql.connector`: For MySQL interaction.
- `streamlit`: For creating the web interface.
- `google_auth_oauthlib.flow`: For Google authentication.
- `pandas`: For data manipulation.
- `ast`: For handling abstract syntax trees.
- `streamlit_autorefresh`: For automatic refreshing of the Streamlit app.

## How to Run
1. Install the required dependencies using `pip install -r requirements.txt`.
2. Obtain a YouTube Data API key from the Google Developer Console.
3. Replace the placeholder API key in the code with your own key.
4. Run the script, and a Streamlit web interface will launch in your browser.
5. Enter the desired channel ID and click "Extract Data" to retrieve channel details.
6. Click "Upload to MongoDB" to store the extracted data in MongoDB.
7. Use the dropdown menus to perform predefined queries on the stored data.

## File Structure
- `youtube_data_harvesting.py`: Main Python script containing the project code.
- `requirements.txt`: Text file listing all dependencies required for the project.
- `README.md`: This file, providing an overview of the project.

## Contributors
- S.Thiruppugazhan: thiruppugazhan27@gmail.com
