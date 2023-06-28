import os 
import argparse 
from cydonia.util.S3Client import S3Client 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download an S3 object")
    parser.add_argument("s3_key", help="S3 path for object to download")
    parser.add_argument("local_path", help="Path to block trace")
    args = parser.parse_args()

    # setup S3 client 
    aws_key = os.environ['AWS_KEY']
    aws_secret = os.environ['AWS_SECRET']
    aws_bucket = os.environ['AWS_BUCKET']

    s3_client = S3Client(aws_key, aws_secret, aws_bucket)
    s3_client.upload_s3_obj(args.s3_key, args.local_path)