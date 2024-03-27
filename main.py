import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import required modules
import firebase_admin
from firebase_admin import db, credentials


#db = initialize_firebase()
# Đổi tên tab và icon
st.set_page_config(
    layout="wide",
    page_title="Tiktok Comment Sentiment Dashboard",  # Đổi tên tab
    page_icon="📈"  # Đổi icon tab
)

# Số lượng điểm dữ liệu
num_points = 10

# Tạo danh sách các mảng ngẫu nhiên
data_arrays = []
user_ids_negative = []
user_ids_neutral = []
user_ids_positive = []
count_negative = 0
count_neutral = 0
count_positive = 0
    
# Tính toán Tích cực và Tiêu cực

import json 

store_array = []
header_placeholder = st.empty()

# Create two big columns
BIG_col1_placeholder, BIG_col2_placeholder = st.columns([1, 1])

# Inside the left big column, create two smaller columns and a chart placeholder
with BIG_col1_placeholder:
    col1_placeholder, col2_placeholder = st.columns([1, 1])
    with col1_placeholder:
        title1_placeholder = st.empty()
        content1_placeholder = st.empty()
    with col2_placeholder:
        title2_placeholder = st.empty()
        content2_placeholder = st.empty()
    chart_placeholder = st.empty()

# Inside the right big column, create placeholders for the
with BIG_col2_placeholder:
    positive_expander_placeholder = st.empty()
    neutral_expander_placeholder = st.empty()
    negative_expander_placeholder = st.empty()
    
# Process Array Comment
def ProcessArrayComment(data_byte):
    global data_arrays, user_ids_negative,count_negative,count_neutral,count_positive

    # Decode byte message to string
    decoded_message_str = data_byte.decode("utf-8")
    
    # Convert string to JSON
    decoded_message_json = json.loads(decoded_message_str)
    
    # Extract "comments" field
    comments_array = decoded_message_json['comments']
    #print("Debug decoded_message_json:", comments_array)
    # Initialize counters
    temp_count_negative = 0
    temp_count_neutral = 0
    temp_count_positive = 0
    # Initialize an empty list to store UserIDs with prediction -1
    
    for comment in comments_array:
            # Ensure the comment is a dictionary
        if isinstance(comment, str):
            comment = json.loads(comment)
            
        data_arrays.append(comment)
        prediction = comment.get("prediction", None)  # Safely get the prediction value; default to None if not found
        
        if prediction is not None:  # Only proceed if prediction value exists
            #print(comment)
            
            # Compare with a small tolerance to account for floating-point arithmetic
            if prediction == 2:
                temp_count_negative += 1
                user_ids_negative.append((comment.get("UserID"), comment.get("Sentence")))
            elif prediction == 0:
                temp_count_neutral += 1
                user_ids_neutral.append((comment.get("UserID"), comment.get("Sentence")))
            elif prediction == 1:
                temp_count_positive += 1
                user_ids_positive.append((comment.get("UserID"), comment.get("Sentence")))

    count_negative = count_negative+temp_count_negative
    count_neutral = count_neutral+temp_count_neutral
    count_positive = count_positive+temp_count_positive

    print(temp_count_positive,temp_count_neutral,temp_count_negative)        
    return temp_count_positive,temp_count_neutral,temp_count_negative,user_ids_negative,user_ids_neutral,user_ids_positive
            
# def upload_to_firebase(db, user_ids_negative):
    
#     # Store data in Firebase
#     id_array = [item[0] for item in user_ids_negative]

#     #data = {"user_ids_negative": user_ids_negative}
#     db.reference("/user_ids_negative").set(id_array)
#     #db.child("Negative").push(data)
    
from kafka import KafkaConsumer
topic_name = "Dashboard"
consumere = KafkaConsumer(topic_name)

# Initial empty DataFrame
columns = ['Time', 'Positive', 'Negative', 'Neutral']
df = pd.DataFrame(columns=columns)

from datetime import datetime

try:
    for mes in consumere:
        store = mes.value
        temp_count_positive,temp_count_neutral,temp_count_negative,user_ids_negative,user_ids_neutral,user_ids_positive = ProcessArrayComment(store)
        current_time = datetime.now().strftime('%H:%M:%S')
        new_row = {'Time': current_time, 'Positive': count_positive, 'Negative': count_negative, 'Neutral': count_neutral}
        #upload_to_firebase(db, user_ids_negative)

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        # Keep only the last 'num_points' points in the DataFrame
        df = df.tail(num_points)
        df.head()
       # Create the line chart
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(df['Time'], df['Positive'], label='Positive')
        ax.plot(df['Time'], df['Negative'], label='Negative')
        ax.plot(df['Time'], df['Neutral'], label='Neutral')
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        # Display user_ids_negative as a table below the line chart
        header_html = "<h1 style='text-align: center; color: white;'><b>📈 TIKTOK COMMENT SENTIMENT ANALYSIS DASHBOARD 📈</b></h1>"
        header_placeholder.markdown(header_html, unsafe_allow_html=True)
        
        with BIG_col1_placeholder:
            # Update the line chart
            chart_placeholder.pyplot(fig, use_container_width=True)
            # Pie chart update
            with col1_placeholder:
                with title1_placeholder:
                    st.subheader("📊 Sentiment Breakdown")
                with content1_placeholder:
                    labels = ['Positive', 'Neutral', 'Negative']
                    sizes = [count_positive, count_neutral, count_negative]
                    fig1, ax1 = plt.subplots()
                    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
                    ax1.axis('equal')
                    st.pyplot(fig1)

            with col2_placeholder:
                with title2_placeholder:
                    st.subheader("📊 Comments Data")
                with content2_placeholder:
                    st.markdown(
                        """
                        <div style="background-color:white;padding:10px;">
                            <p style="color:black;">🗨️ <strong>Total Number of Comments:</strong> {}</p>
                            <p style="color:black;">👤 <strong>Total Number of Users:</strong> {}</p>
                            <hr style="border-top: 2px solid black;">
                            <p style="color:black;">👍 <strong>Number of Positive Users:</strong> {}</p>
                            <p style="color:black;">😐 <strong>Number of Neutral Users:</strong> {}</p>
                            <p style="color:black;">👎 <strong>Number of Negative Users:</strong> {}</p>
                        </div>
                        """.format(count_positive + count_negative + count_neutral, 
                                len(user_ids_negative) + len(user_ids_neutral) + len(user_ids_positive),
                                len(user_ids_positive),
                                len(user_ids_neutral),
                                len(user_ids_negative)), 
                        unsafe_allow_html=True
                    )
                #border-radius:5px padding-bottom:100px;
        with BIG_col2_placeholder:
            # Toggle tables
            with positive_expander_placeholder.expander("Positive"):
                st.table(pd.DataFrame(user_ids_positive[-13:], columns=["User IDs", "Comment"]))
            
            with neutral_expander_placeholder.expander("Neutral"):
                st.table(pd.DataFrame(user_ids_neutral[-13:], columns=["User IDs", "Comment"]))

            with negative_expander_placeholder.expander("Negative"):
                st.table(pd.DataFrame(user_ids_negative[-13:], columns=["User IDs", "Comment"]))
        
        
except Exception as e:
    print(f"An error occurred: {e}")