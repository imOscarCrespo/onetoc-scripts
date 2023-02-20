import subprocess
import os
import boto3
import json

# Set up AWS credentials
s3 = boto3.client('s3', aws_access_key_id='xxxxx',
                  aws_secret_access_key='xxxxx')
sqs = boto3.resource('sqs', aws_access_key_id='xxxxx',
                     aws_secret_access_key='xxxxx', region_name='eu-west-1')

# Set up ffmpeg command
ffmpeg_cmd = ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', 'input.txt', '-c', 'copy', 'output.mp4']

# Set up S3 upload parameters
object_name = 'output.mp4'

# Set up SQS parameters
queue_url = 'https://sqs.eu-west-1.amazonaws.com/775577554361/concatenate_media.fifo'

# Define a function to concatenate videos and upload to S3
def process_message(message, body):
    try:
        body_obj = json.loads(body)
        bucket_match = body_obj["bucket_match"]
        # List all the MP4 files in the directory
        response = s3.list_objects_v2(Bucket='matches-onetoc', Prefix=bucket_match)
        input_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.mp4')]
        if not input_files:
            print(f"No .mp4 files found in {bucket_match}")
            return
        for input_file in input_files:
            print(f"Downloading... {input_file}")
            s3.download_file('matches-onetoc', input_file, os.path.basename(input_file))
            print(f"Downloaded {input_file}")
        # Create a file list for ffmpeg to concatenate
        with open('input.txt', 'w') as f:
            for file in input_files:
                f.write(f"file '{file.replace(bucket_match + '/', '')}'\n")
        
        # Concatenate the input files using ffmpeg
        print('running ffmpeg command')
        subprocess.run(ffmpeg_cmd)
        print('finished ffmpeg command')
        # Upload the concatenated file to S3
        with open('output.mp4', 'rb') as f:
            s3.upload_fileobj(f, 'matches-onetoc', f'{bucket_match}/output.mp4')

        print('output uploaded to s3')
        for file in input_files:
            os.remove(f"{file.replace(bucket_match + '/', '')}")
        # Clean up temporary files
        os.remove('output.mp4')

        # Delete the message from the queue
        message.delete()
        print('Message deleted')
    except Exception as e:
        print(f'Error concatening media for match: {body_obj["bucket_match"]}. Error: {e}')

# Get the SQS queue
queue = sqs.Queue(queue_url)

# Continuously poll the queue for messages
while True:
    messages = queue.receive_messages()
    for message in messages:
        # Process the message
        process_message(message, message.body)