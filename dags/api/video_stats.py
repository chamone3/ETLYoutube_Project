import requests 
import json
from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv()

from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlistId():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()
        # print(json.dumps(data, indent=4))

        channel_items = data["items"][0]

        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        ##print(channel_playlistId)
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e 

@task
def get_video_ids(playlistId):
    video_ids = []
    pageToken = None
    base_url = (
        f"https://youtube.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    )

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        print("Erro na request:", e)
        print("Resposta da API:", response.text)
        raise

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(input_list, batch_size):
        for i in range(0, len(input_list), batch_size):
            yield input_list[i:i + batch_size]
    
    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)

            url = (
                "https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        print("Erro na request:", e)
        print("Resposta da API:", response.text)
        raise

@task
def save_to_json(extracted_data):
    filepath = f"./data/YT_data_{date.today()}.json"

    with open(filepath, "w", encoding="utf8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    playlistId = get_playlistId()
    video_data = extract_video_data(video_ids=get_video_ids(playlistId))
    save_to_json(video_data)