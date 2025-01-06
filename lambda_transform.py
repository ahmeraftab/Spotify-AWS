import boto3
import json
from datetime import datetime
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # Read source bucket and key from S3 trigger event
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    transformed_bucket = 'spotify-transformed-data-aa'

    # Download raw data
    raw_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
    raw_content = json.loads(raw_obj['Body'].read().decode('utf-8'))

    # Transform
    transformed_data = []
    for item in raw_content.get('items', []):
        track = item.get('track')
        if not track:
            continue
        transformed_data.append({
            'track_name':   track['name'],
            'artist_name':  track['artists'][0]['name'],
            'album_name':   track['album']['name'],
            'popularity':   track['popularity'],
            'duration_ms':  track['duration_ms'],
            'release_date': track['album']['release_date']
        })

    # Upload transformed data
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    transformed_key = f'transformed_data/spotify_tracks_{timestamp}.json'

    s3.put_object(
        Bucket=transformed_bucket,
        Key=transformed_key,
        Body=json.dumps(transformed_data, indent=2),
        ContentType='application/json'
    )

    print(f"Transformed {len(transformed_data)} tracks → s3://{transformed_bucket}/{transformed_key}")
    return {
        'statusCode': 200,
        'body': f'Transformed {len(transformed_data)} tracks'
    }