import boto3
import os
import mimetypes

output_bucket = os.environ['S3_OUTPUT_BUCKET']
repository_name = os.environ['REPOSITORY_NAME']
path_prefix = os.environ['PATH_PREFIX']
branch_name = os.environ['BRANCH_NAME']
aws_region = os.environ['AWS_REGION']


# returns a list of an all files in the branch and specified path
def get_blob_list(codecommit, repository):
    print(f"\nChecking for changes after the path: /{path_prefix}")

    response = codecommit.get_differences(
        repositoryName=repository,
        afterCommitSpecifier=branch_name,
        afterPath=path_prefix
    )

    blob_list = [difference['afterBlob'] for difference in response['differences']]

    while 'nextToken' in response:
        response = codecommit.get_differences(
            repositoryName=repository,
            afterCommitSpecifier=branch_name,
            nextToken=response['nextToken']
        )
        blob_list += [difference['afterBlob'] for difference in response['differences']]

    print("\nFiles changed:")
    for blob in blob_list:
        print(f"- {blob['path']}")

    return blob_list


# lambda-function
# triggered by changes in a codecommit repository
# reads files in the repository and uploads them to s3-bucket
def lambda_handler(event, context):
    commit = event['Records'][0]['codecommit']['references'][0]['commit']

    print(f"Repository: {repository_name} Branch: {branch_name} - Commit: {commit}")
    # target bucket
    bucket = boto3.resource('s3').Bucket(output_bucket)
    # source codecommit
    codecommit = boto3.client('codecommit', region_name=aws_region)

    # reads each file in the branch and uploads it to the s3 bucket
    blob_list = get_blob_list(codecommit, repository_name)

    print(f"\nSending {len(blob_list)} files to the bucket {output_bucket}:")
    for blob in blob_list:
        path = blob['path']
        content = (codecommit.get_blob(repositoryName=repository_name, blobId=blob['blobId']))['content']
        # we have to guess the mime content-type of the files and provide it to S3 since S3 cannot do this on its own.
        content_type = mimetypes.guess_type(path)[0]

        if content_type is not None:
            bucket.put_object(Body=content, Key=path, ContentType=content_type)
        else:
            bucket.put_object(Body=content, Key=path)

        print(f"- File {path} sent")

    print(f"\nFiles copied from the repository {repository_name} to the bucket {output_bucket} successfully.")

