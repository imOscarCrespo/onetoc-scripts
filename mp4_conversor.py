from datetime import date
import os
import shlex
import boto3
import json
import subprocess

# Set up AWS credentials
s3 = boto3.client('s3', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                  aws_secret_access_key='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')
sqs = boto3.resource('sqs', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                     aws_secret_access_key='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', region_name='eu-west-1')
sqs_client = boto3.client('sqs', aws_access_key_id='AKIA3JFAGJW44LW33YEB',
                  aws_secret_access_key='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')
lambda_client = boto3.client('lambda')

concatenate_media_queue_url = 'https://sqs.eu-west-1.amazonaws.com/775577554361/concatenate_media.fifo'
# Set up SQS parameters
queue_url = 'https://sqs.eu-west-1.amazonaws.com/775577554361/mp4_conversor.fifo'


def sendProgressToClient(connection_id, video_name, progress):
    input_params = {
        'connectionId': connection_id,
        'message': progress,
    }
    lambda_client.invoke(
        FunctionName='webhook_broadcast',
        InvocationType='RequestResponse', # set to 'Event' if you don't need a response
        Payload=json.dumps(input_params)
    )

def get_video_frame_count(video_file_path):
    ffprobe_cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=nb_frames',
        '-of', 'json',
        video_file_path
    ]
    result = subprocess.run(ffprobe_cmd, capture_output=True, text=True)
    output = json.loads(result.stdout)
    frame_count = int(output['streams'][0]['nb_frames'])
    return frame_count

def process_message(message, body):
    # try:
        body_obj = json.loads(body)
        match = body_obj["match"]
        connection_id = body_obj["connectionId"]
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
        input_files = ['prueba/14_0.mov', 'prueba/4_0_IMG_4397.MOV']
        output_files = []
        for input_file in input_files:
            # frameCount = get_video_frame_count(f'/Users/oscar/code/onetoc-scripts/{input_file.replace(match + "/", "")}')
            command = f'ffmpeg -stats -i {input_file.replace(match + "/", "")} -c:v libx264 -crf 23 -preset medium -c:a aac -b:a 128k \-movflags +faststart -vf scale=-2:720,format=yuv420p "output_{i}.mp4"'
            process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)

            # read and process the command output line by line
            output = process.stdout.readline()
            while output:
                line = output.strip()
                if "Duration" in line:
                    duration = line.split(",")[0].split("Duration: ")[1]
                    hours, minutes, seconds = duration.split(":")
                    total_seconds = int(hours)*3600 + int(minutes)*60 + float(seconds)
                elif "frame=" in line:
                    current_time = line.split("time=")[1].split()[0]
                    current_hours, current_minutes, current_seconds = current_time.split(":")
                    current_seconds = int(current_hours)*3600 + int(current_minutes)*60 + float(current_seconds)
                    progress = round((current_seconds/total_seconds)*100, 2)
                    print(f"Progress: {progress}%")
                    sendProgressToClient(connection_id, None, progress)
                output = process.stdout.readline()

            output_files.append(f'output_{i}.mp4')
            i += 1

            process.stdout.close()
        
        for output_file in output_files:
            with open(output_file, 'rb') as f:
                s3.upload_fileobj(f, 'matches-onetoc', f'{match}/{output_file}')

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
