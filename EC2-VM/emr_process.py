import boto3

# Configure your AWS credentials (you can also use environment variables or IAM roles)
aws_access_key_id = '<access_key>'
aws_secret_access_key = '<secret_key>'
aws_region = 'eu-north-1'

cluster_id = '<cluster_id>'
job_name = 'Spark job'



# Create a session
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

emr_client = session.client('emr')

#1) Remove s3 existing path
def remove_s3_path(job_name,command):
    response = emr_client.add_job_flow_steps(
    JobFlowId=cluster_id,
    Steps=[
        {
            'Name': job_name,
            'ActionOnFailure': 'CONTINUE',  # You can change this to suit your needs
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash',
                    '-c',
                    command,
                    ],
                },
            },
        ],
    )
    
    return response


#2) ETL
def etl_process(job_name,command):
    response = emr_client.add_job_flow_steps(
    JobFlowId=cluster_id,
    Steps=[
        {
            'Name': job_name,
            'ActionOnFailure': 'CONTINUE',  
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash',
                    '-c',
                    command,
                    ],
                },
            },
        ],
    )
    
    return response

#3) Analysis and Store insights
def analyze_and_store(job_name,command):
    response = emr_client.add_job_flow_steps(
    JobFlowId=cluster_id,
    Steps=[
        {
            'Name': job_name,
            'ActionOnFailure': 'CONTINUE',  
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash',
                    '-c',
                    command,
                    ],
                },
            },
        ],
    )
    
    return response

