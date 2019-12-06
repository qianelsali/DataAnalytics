# AWS sagemaker connect to S3 bucket data 
import boto3

#https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html
def useTempCredential():  
  sts_client = boto3.client('sts')
  assumed_role_object=sts_client.assume_role(
      RoleArn="arn:aws:iam::account-of-role-to-assume:role/name-of-role",
      RoleSessionName="AssumeRoleSession1"
  )
  credentials=assumed_role_object['Credentials']
  s3_resource=boto3.resource(
      's3',
      aws_access_key_id=credentials['AccessKeyId'],
      aws_secret_access_key=credentials['SecretAccessKey'],
      aws_session_token=credentials['SessionToken'],
  ) 
  for bucket in s3_resource.buckets.all():
      print(bucket.name)
      
#directly read data from S3
def readDataFromS3(bucketName=BUCKET, 
                   filePath=DEFAULTFILE, 
                   nrows=10000):
    role = get_execution_role()
    region = boto3.Session().region_name
    bucket= bucketName
    fileName = filePath
    data_location = 's3://{}/{}'.format(bucket, fileName)
    if nrows==99999:
        return pd.read_csv(data_location, sep="|")
    return pd.read_csv(data_location, sep="|", nrows=nrows)
    
    
