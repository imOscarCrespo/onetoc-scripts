from datetime import date
import os
import boto3
import json

# Set up AWS credentials
s3 = boto3.client('s3', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                  aws_secret_access_key='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')
sqs = boto3.resource('sqs', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                     aws_secret_access_key='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', region_name='eu-west-1')
sqs_client = boto3.client('sqs', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                  aws_secret_access_key='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')

concatenate_media_queue_url = 'https://sqs.eu-west-1.amazonaws.com/775577554361/concatenate_media.fifo'
# Set up SQS parameters
queue_url = 'https://sqs.eu-west-1.amazonaws.com/775577554361/mp4_conversor.fifo'

# Define a function to concatenate videos and upload to S3
def process_message(message, body):
    # try:
        body_obj = json.loads(body)
        match = body_obj["match"]
        # List all the MP4 files in the directory
        response = s3.list_objects_v2(Bucket='matches-onetoc', Prefix=match)
        input_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key']]
        del input_files[0]
        if not input_files:
            print(f"No .mp4 files found in {match}")
            return
        for input_file in input_files:
                s3.download_file('matches-onetoc', input_file, os.path.basename(input_file))
        i = 0
        output_files = []
        for input_file in input_files:
            command = f'ffmpeg -i {input_file.replace(match + "/", "")} "output_{i}.mp4"'
            os.system(command)
            output_files.append(f'output_{i}.mp4')
            i += 1
        
        for output_file in output_files:
            with open(output_file, 'rb') as f:
                s3.upload_fileobj(f, 'matches-onetoc', f'{match}/{output_file}')

        print('output uploaded to s3')
        print('Message deleted')
        now = date.today()
        response = sqs_client.send_message(
            QueueUrl=concatenate_media_queue_url,
            MessageBody=body,
            MessageGroupId = now.strftime('%Y%m%d%H%M%S'),
            MessageDeduplicationId = now.strftime('%Y%m%d%H%M%S')
        )
        message.delete()
queue = sqs.Queue(queue_url)
while True:
    messages = queue.receive_messages()
    for message in messages:
        # Process the message
        process_message(message, message.body)