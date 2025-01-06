import boto3
import base64
import json
import os
from urllib import request, parse
from datetime import datetime


def lambda_handler(event, context):
    CLIENT_ID = os.environ['CLIENT_ID']
    CLIENT_SECRET = os.environ['CLIENT_SECRET']
    S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    PLAYLIST_ID = os.environ.get('PLAYLIST_ID', '37i9dQZEVXbMDoHDwVN2tF')  # Global Top 50

    token = get_spotify_token(CLIENT_ID, CLIENT_SECRET)
    data = fetch_playlist_tracks(PLAYLIST_ID, token)

    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    key = f'raw_data/spotify_tracks_{timestamp}.json'
    save_to_s3(data, S3_BUCKET_NAME, key)

    return {
        'statusCode': 200,
        'body': f'Saved {len(data["items"])} tracks to s3://{S3_BUCKET_NAME}/{key}'
    }


def get_spotify_token(client_id, client_secret):
    url = 'https://accounts.spotify.com/api/token'
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {'Authorization': f'Basic {credentials}'}
    data = parse.urlencode({'grant_type': 'client_credentials'}).encode()

    req = request.Request(url, data=data, headers=headers)
    with request.urlopen(req) as response:
        return json.loads(response.read().decode()).get('access_token')


def fetch_playlist_tracks(playlist_id, token):
    url = f'https://api.spotify.com/v1/playlists/{playlist_id}/tracks'
    headers = {'Authorization': f'Bearer {token}'}
    req = request.Request(url, headers=headers)
    with request.urlopen(req) as response:
        return json.loads(response.read().decode())


def save_to_s3(data, bucket_name, key):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
    print(f"Saved to s3://{bucket_name}/{key}")