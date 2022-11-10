# Count unique number of words in a given text file
import boto3
import os

# env variables
# , , destination_folder, destination_bucket

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID_new']
AWS_SECRET_KEY_ID = os.environ['AWS_SECRET_KEY_ID_new']

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY_ID)

s3 = session.resource('s3')


def count_words(file_name):
	# Read file
	lines = open(file_name, 'r').readlines()
	words = set()

	for line in lines:
		for curr_word in line.split():
			words.add(curr_word)

	return len(words)


def lambda_handler(event, context):
	# event = {"Records": [record1, record2,,,]}
	# record1 = {'s3': {'bucket':'name', 'object': 'key'}}

	source_bucket = event['Records'][0]['s3']['bucket']['name']
	source_file = event['Records'][0]['s3']['object']['key']
	destination_folder = os.environ['destination_folder']
	destination_bucket = os.environ['destination_bucket']

	# Download from s3 using boto3
	try:
		s3.meta.client.download_file(source_bucket, source_file, f'/tmp/{source_file}')
		num_unique_words = count_words(f'/tmp/{source_file}')
		f = open(f'/tmp/{source_file}.out', 'w')
		f.write(f'Total number of Unique words in s3://{source_bucket}/{source_file} is {num_unique_words}')
		f.close()
		s3.meta.client.upload_file(f'/tmp/{source_file}.out', destination_bucket, f'{destination_folder}/{source_file}.out') 
		print("Lambda execution finished succesfully")
	except Exception as e:
		print("Exception: ", e)
		print("Lambda execution failed")