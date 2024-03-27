from TikTokApi import TikTokApi
import asyncio
import os
import json
import pandas as pd
import random

#let see can we
from kafka import KafkaProducer
from json import dumps
from time import sleep
#
#!pip install kafka-python

tiktokMSToken = "2cgcxtYi2LqdYwcm0JQ6XP6dZ-tqroVHUOw3nmm06AYOYCijOfqdBiR29sFGsOlfc7Z_tXQaw7BWot4VzyPekuDft94HaJ8Mzv-gx0WAbizX4l5WoxWVCY2dWFVQOkr5lgv1nPv5D9UPE5WEloM="
ms_token = os.environ.get(tiktokMSToken, None) # get your own ms_token from your cookies on tiktok.com
timeInterval = 30 #second
amountVideo = 10#10000
amountComment = 100 
existedFile = 50
# topic_name = "quickstart"
# kafka_server = "localhost:9092"
# producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))
createDataset = True

import re

def remove_tags(text):
    """
    Remove all tags (e.g., @username) from the given text.

    Parameters:
        text (str): The original text.

    Returns:
        str: The text with all tags removed.
    """
    # Using regular expression to find all occurrences of tags and replace them with an empty string
    return re.sub(r'@\w+', '', text)


async def print_exported_data(data):
    
    #No we dont use this code, I think we only send the new data to 03 by Kafka
    #export to JSON, append to existed file if it has, else create new file
    # try:
    #     with open('export.json', 'r') as f:
    #         existing_data = json.load(f)
    # except FileNotFoundError:
    #     existing_data = []
        
          
    rawJson = []    
    for video in data:
        dataVideo = video.as_dict
        #dataVideo = {'byteVideo': await video.bytes()}  # Await and assign the result -> NotImplementedError
        rawJson.append(dataVideo) 
    
    #existing_data.extend(rawJson) 
    existing_data = process_results(rawJson)
    existing_data_df = pd.DataFrame.from_dict(existing_data, orient='index')
    
    #print(existing_data)
    existing_data_df.head(5)
    existing_data_df.to_csv("cleaned_data_in_01.csv", index=False)
    
    with open('export.json', 'w') as f:
        json.dump(existing_data, f, indent=4)
        
    #producer.send(topic_name, value=existing_data)
# Define the correction function
def correct_encoding(s):
    if isinstance(s, str):
        try:
            return s.encode('iso-8859-1').decode('utf-8')
        except UnicodeEncodeError:
            return s  # or you can choose to handle the error differently
    else:
        return s  # Non-string data remains unchanged


async def print_exported_comments(data,timeInvoke):
    # rawJson = []    
    # for comment in data:
    #     dataComment = comment.as_dict
    #     rawJson.append(dataComment) 
    
    # existing_data = process_results(rawJson)
    # existing_data_df = pd.DataFrame.from_dict(existing_data, orient='index')
    
    # #print(existing_data)
    # existing_data_df.head(5)
    # existing_data_df.to_csv("cleaned_data_in_01.csv", index=False)
    
    if createDataset:
        df = pd.DataFrame(data, columns=['User ID', 'Comment']) 
        file_name = f"Dataset/rawdata_{timeInvoke+existedFile}.csv"
        # Apply the correction to the entire DataFrame
        df_corrected = df #df.applymap(correct_encoding)
        df_corrected.to_csv(file_name, index=False, encoding='utf-8-sig')
    else:
        #cleaned_texts = [remove_tags(text) for text in data]
        with open('export_comment.json', 'w') as f:
            json.dump(data, f, indent=4)
    
    # message = {
    #     'comments' : export
    # }    
    # producer.send(topic_name, value=message)
    # producer.flush()
    
    
# Conver processing code to function
def process_results(data):
    nested_values = ['author','authorStats','challenges','contents','stats','video','duetInfo', 'textExtra', 'stickersOnItem']
    skip_values = ['contents','challenges', 'duetInfo', 'textExtra', 'stickersOnItem']

    # Create blank dictionary
    flattened_data = {}
    # Loop through each video
    for idx, value in enumerate(data): 
        flattened_data[idx] = {}
        # Loop through each property in each video 
        print(f"{idx} - {value}")
        for prop_idx, prop_value in value.items():
            # Check if nested
            if prop_idx in nested_values:
                if prop_idx in skip_values:
                    pass
                else:
                    print(prop_idx)
                    # Loop through each nested property
                    print(prop_value)
                    for nested_idx, nested_value in prop_value.items():
                        flattened_data[idx][prop_idx+'_'+nested_idx] = nested_value
            # If it's not nested, add it back to the flattened dictionary
            else: 
                flattened_data[idx][prop_idx] = prop_value

    return flattened_data

async def trending_videos(timeInvoke):
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=5)
        #export_data = []
        export_comment = []
        tag = api.hashtag(name="offb")
        async for video in tag.videos(count=amountVideo, cursor=(timeInvoke-1)*amountVideo): #+existedFile
        #async for video in api.trending.videos(count=amountVideo, cursor=(timeInvoke-1+existedFile)*amountVideo):
            if video is None:
                print("Received None video object. Skipping...")
                continue
                        
            #export_data.append(video)
            if video:
                async for comment in video.comments(count=5): 
                    if comment is None:
                        print("Received None comment object. Skipping...")
                        continue  
                    
                    t = comment.as_dict["text"]
                    t = re.sub(r'@\w+', '', t)
                    t = t.encode('utf-8',errors='ignore').decode('utf-8')
                    
                    #userID = comment.author.user_id
                    #print(f"{userID}-{t}")
                    print(f"{t}")
                    export_comment.append(("userID",t))
                
        # Run the printing coroutine concurrently with the trending videos loop
        #await asyncio.gather(print_exported_data(export_data))
        await asyncio.gather(print_exported_comments(export_comment,timeInvoke))
    
       
async def schedule_trending_videos():
    timeInvoke = 0
    while timeInvoke <= 50:
        timeInvoke +=1 
        print(timeInvoke)
        await trending_videos(timeInvoke)
        await asyncio.sleep(timeInterval)  # Sleep for 5 minutes (300 seconds)
        
if __name__ == "__main__":
    asyncio.run(schedule_trending_videos())

    
    
    #Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted

    
    
    #verify_l7qwuin0_ghLBDGxG_tHCe_4WBj_BMr8_hCrftMlTNvXl
    
 
def unnest_Data(data):
    # Conver processing code to function
    nested_values = ['author','authorStats','challenges','contents','stats','video','duetInfo', 'textExtra', 'stickersOnItem']
    skip_values = ['contents','challenges', 'duetInfo', 'textExtra', 'stickersOnItem']

    # Create blank dictionary
    flattened_data = {}
    # Loop through each video
    for idx, value in enumerate(data): 
        flattened_data[idx] = {}
        # Loop through each property in each video 
        print(f"{idx} - {value}")
        for prop_idx, prop_value in value.items():
            # Check if nested
            if prop_idx in nested_values:
                if prop_idx in skip_values:
                    pass
                else:
                    print(prop_idx)
                    # Loop through each nested property
                    print(prop_value)
                    for nested_idx, nested_value in prop_value.items():
                        flattened_data[idx][prop_idx+'_'+nested_idx] = nested_value
            # If it's not nested, add it back to the flattened dictionary
            else: 
                flattened_data[idx][prop_idx] = prop_value

    return flattened_data
