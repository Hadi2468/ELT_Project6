import boto3

def lambda_handler(event, context):
    client = boto3.client("codepipeline")
    response = client.start_pipeline_execution(
        name="project6-codepipeline"
    )
    return response