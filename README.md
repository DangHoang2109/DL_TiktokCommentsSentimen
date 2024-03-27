# DL_TiktokCommentsSentimen
This project is a comprehensive sentiment analysis system designed to classify comments from TikTok videos, utilizing a robust tech stack that includes PySpark, Streamlit, and Firebase, with Kafka as the messaging backbone.

## Overview
With the rise of social media content, understanding user sentiment has become crucial for content creators and marketers alike. This project focuses on analyzing sentiments of comments on trending TikTok videos, identifying the general mood and feedback of the audience. Our system is divided into four distinct modules, each responsible for a specific aspect of the sentiment analysis pipeline.

## Modules

**Module 1: TikTok Comment Fetching**
Utilizes the unofficial TikTok API to fetch comments from trending videos, providing a rich dataset for sentiment analysis.

**Module 2: Sentiment Classification with PySpark**
Employs PySpark to develop a simple yet effective sentiment classification model. This model is trained to distinguish between positive, neutral, and negative sentiments, enabling us to gauge the general sentiment of TikTok comments accurately.

**Module 3: Data Visualization Dashboard with Streamlit**
Implements a Streamlit dashboard for intuitive and interactive data visualization. Users can explore the sentiment analysis results, trends, and insights through a user-friendly web interface.

**Module 4: Firebase Realtime Database Integration**
Negative comments and the corresponding user information are uploaded and saved on Firebase Realtime Database. This feature allows for further analysis and potentially actionable insights regarding the content's reception.

**Communication**
All modules communicate seamlessly with each other through Kafka, ensuring a fluid and scalable data pipeline. This architecture not only enhances the system's efficiency but also provides flexibility for future enhancements and module integrations.
