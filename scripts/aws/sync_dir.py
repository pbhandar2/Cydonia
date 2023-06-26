import argparse 
import os 
import pathlib 
from cydonia.util.S3Client import S3Client 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download an S3 object")
    parser.add_argument("s3_prefix", help="s3 prefix to sync")
    parser.add_argument("local_dir", type=pathlib.Path, help="Local directory to download objects")
    args = parser.parse_args()

    # setup S3 client 
    aws_key = os.environ['AWS_KEY']
    aws_secret = os.environ['AWS_SECRET']
    aws_bucket = os.environ['AWS_BUCKET']
    s3 = S3Client(aws_key, aws_secret, aws_bucket)
    s3.sync_s3_prefix_with_local_dir(args.s3_prefix, args.local_dir)