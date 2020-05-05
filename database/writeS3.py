import boto3
import os
import time

os.makedirs(os.path.dirname('logs/'), exist_ok=True)
applog_filename = "applog_filename.log"
reclog_filename = "reclog_filename.log"
logging.basicConfig(filename=applog_filename, level=logging.DEBUG)
BUCKET_NAME = 'application-logs'
		
		
def S3_helper(SOURCE_FILENAME, BUCKET_NAME, TARGET_FILENAME):
    S3 = boto3.client('s3')
    S3.upload_file(SOURCE_FILENAME, BUCKET_NAME, TARGET_FILENAME)
    
    
def write_logs_S3():
		SourceFileDict = {applog_filename: "app_log/"+applog_filename,
					reclog_filename: "request_rec/"+reclog_filename}
		try:
			for source_filename in SourceFileDict:
				now = time.time()
				S3_helper(source_filename, BUCKET_NAME, "{}_{}".format(SourceFileDict[source_filename],now))
			return "Log files have been successfully transferred into s3 bucket - {} at {}.".format(BUCKET_NAME, now)
		except:
			return "ERROR: Log files have NOT been successfully transferred into s3 bucket." 
