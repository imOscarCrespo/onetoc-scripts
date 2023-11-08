import subprocess
import os
import boto3
import json

import urllib3

http = urllib3.PoolManager()

lambda_client = boto3.client('lambda')

# Set up AWS credentials
s3 = boto3.client('s3', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                  aws_secret_access_key='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', region_name='eu-west-1')
sqs = boto3.resource('sqs', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                     aws_secret_access_key='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', region_name='eu-west-1')

# Set up ffmpeg command


# Set up S3 upload parameters
object_name = 'output.mp4'

# Set up SQS parameters
queue_url = 'https://sqs.eu-west-1.amazonaws.com/775577554361/concatenate_media.fifo'

def update_match_media(match):

    s3_matches_url = 'https://matches-onetoc.s3.eu-west-1.amazonaws.com'
    auth_url = "https://api.onetoc.com/api/token/"

    auth_payload = json.dumps({
      "username": "internal_process",
      "password": "3Em4jH8Qrx6DCF"
    })
    
    auth_headers = {
      'Content-Type': 'application/json'
    }
    
    auth_response = http.request("POST", auth_url, headers=auth_headers, body=auth_payload.encode('utf-8'))
    auth_res = auth_response.data.decode('utf-8')
    matchId = match.split('_')
    url = f'https://api.onetoc.com/match/{matchId[1]}'

    payload = json.dumps({
        "media": f'{s3_matches_url}/{match}/output.mp4'
    })
    
    headers = {
        'Authorization': f'Bearer {json.loads(auth_res)["access"]}',
        'Content-Type': 'application/json'
    }
    http.request("PATCH", url, headers=headers, body=payload.encode('utf-8'))

def sendProgressToClient(connection_id):
    input_params = {
        'connectionId': connection_id,
        'message': 100,
    }
    lambda_client.invoke(
        FunctionName='webhook_broadcast',
        InvocationType='RequestResponse', # set to 'Event' if you don't need a response
        Payload=json.dumps(input_params)
    )

# Define a function to concatenate videos and upload to S3
def process_message(message, body):
    try:
        body_obj = json.loads(body)
        match = body_obj["matchId"]
        connection_id = body_obj["connectionId"]
        file_extension = "mp4"
        # List all the MP4 files in the directory
        response = s3.list_objects_v2(Bucket='matches-onetoc', Prefix=match)
        input_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.mp4')]
        if not input_files:
            print(f"No .mp4 files found in {match}")
            return
       
        j=0
        for input_file in input_files:
                fileName = f'{match}_{j}.{file_extension}'
                print('FILENAME', fileName)
                print('downloading...', f'{match}_{input_file}')
                s3.download_file('matches-onetoc', input_file, os.path.basename(fileName))
                j += 1
        print('heyy', input_files)
        input_files_with_no_extension = list(map(lambda x: x.replace(".mp4", ""), input_files))
        input_files_with_no_matchId = list(map(lambda x: x.replace(f'{match}/', ""), input_files_with_no_extension))
        print('111', input_files_with_no_matchId)
        input_files_nums = list(map(int, input_files_with_no_matchId))
       
        sort_list_nums = sorted(input_files_nums)
        input_files_sorted = [f'{match}_{str(item)}.mp4' for item in sort_list_nums]
        print('111', input_files_sorted)
        # Create a file list for ffmpeg to concatenate
        with open('input.txt', 'w') as f:
            for file in input_files_sorted:
                f.write(f"file '{file.replace(match + '/', '')}'\n")
        
        # Concatenate the input files using ffmpeg
        print('running ffmpeg command')
        ffmpeg_cmd = ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', 'input.txt', '-c', 'copy', f'{match}_output.mp4']
        subprocess.run(ffmpeg_cmd)
        print('finished ffmpeg command')
        # Upload the concatenated file to S3
        with open(f'{match}_output.mp4', 'rb') as f:
            s3.upload_fileobj(f, 'matches-onetoc', f'{match}/output.mp4')

        print('output uploaded to s3')
        k=0
        for file in input_files:
            match_to_delete_s3 = f'{match}/{k}.mp4'
            fileNameMp4 = f'{match}_{k}.mp4'
            os.remove(f'{fileNameMp4}')
            s3.delete_object(Bucket='matches-onetoc',Key=match_to_delete_s3)
            k += 1
        # Clean up temporary files
        os.remove(f'{match}_output.mp4')
        # updating match media
        update_match_media(match)
        sendProgressToClient(connection_id)
        # Delete the message from the queue
        message.delete()
        print('Message deleted')
    except Exception as e:
        print(f'Error concatening media for match: {body_obj["match"]}. Error: {e}')

# Get the SQS queue
queue = sqs.Queue(queue_url)

# Continuously poll the queue for messages
while True:
    messages = queue.receive_messages()
    for message in messages:
        print('message', message)
        # Process the message
        process_message(message, message.body)