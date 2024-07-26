import base64
import json
from google.cloud import storage
from google.cloud import bigquery

def process_gcs_event(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_data = json.loads(pubsub_message)

    bucket_name = message_data['bucket']
    file_name = message_data['name']

    try:
        # Read file from GCS
        file_content = read_file_from_gcs(bucket_name, file_name)

        api_key,session_id = get_apikey_sessionid(file_name)
        # Perform transformations
        transformed_data = transform_data(file_content,api_key,session_id)

        # Write transformed data to BigQuery
        write_to_bigquery(transformed_data)

        print('Data successfully processed and written to BigQuery')
    except Exception as e:
        print(f'Error processing data: {e}')

def get_apikey_sessionid(file_name):
    file_path = file_name.split("/")
    apikey = file_path[0]
    sessionId = file_path[1]
    return (apikey,sessionId)

def read_file_from_gcs(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    return content

def analyse_same_element_access(element_ids):
    
    event_rating = "good"
    current_id = None
    current_count = 0
    print("elements are --",element_ids)
    for element_id in element_ids:
        if element_id == current_id:
            current_count += 1
        else:
            if current_count > 3:
                event_rating = "bad"
                print("Event Rating set as Bad due to same element consecutive access, since access count is greater than 3 for element id  ",current_id)
                break
            current_id = element_id
            current_count = 1
    return event_rating

def detect_rapid_clicks(event_timestamps, threshold=5, interval=2000):
    timestamps = []
    event_rating = "good"
    for timestamp in event_timestamps:
        
        timestamps.append(timestamp)
        
        while timestamps and (timestamp - timestamps[0] > interval):
            timestamps.pop(0)
        
        if len(timestamps) > threshold:
            event_rating = "bad"
            print("Event Rating set as Bad due to rapid clicks.  Threshold is 5 in interval of 2 seconds, but actual clicks is ",len(timestamps))
            break
    
    return event_rating


def transform_data(data,api_key,session_id):
    
    json_data = json.loads(data)
    bigquery_rows = []
    timestamps= []
    elementIds = []
    if isinstance(json_data, list):
        
        for data in json_data:
            for entry in data:
                if 'timestamp' in entry:
                    timestamps.append(entry['timestamp']/1000) #converting nanosec to microsec since bigquery is not supporting ns
                if 'element' in entry and 'id' in entry['element']:
                    elementIds.append(entry['element']['id'])
        eventRating = "good"
        functions = [
       # lambda: analyse_same_element_access(elementIds),
        lambda: detect_rapid_clicks(timestamps)
        ]
    
        for func in functions:
            if eventRating == "good":
                eventRating = func()
            else:
                break
        row = {'APIKey':api_key,
               'SessionId':session_id,
               'EventRating':eventRating,
               'Timestamp_start':timestamps[0],
               'Timestamp_end':timestamps[-1],
               'EventData':json.dumps(data)}
        print("row is ",row)
        bigquery_rows.append(row)
    return bigquery_rows

def write_to_bigquery(data):
    client = bigquery.Client()
    dataset_id = 'eventData'
    table_id = 'event_data_table'
    table_ref = client.dataset(dataset_id).table(table_id)

    errors = client.insert_rows_json(table_ref, data)
    if errors:
        raise Exception(errors)
