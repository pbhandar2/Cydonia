import argparse 
from cydonia.util.S3Client import S3Client 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download an S3 object")
    parser.add_argument("s3_access", help="S3 key")
    parser.add_argument("s3_secret", help="S3 secret")
    parser.add_argument("s3_key", help="S3 path for object to download")
    parser.add_argument("local_path", help="Path to block trace")
    args = parser.parse_args()

    s3_client = S3Client(args.s3_access, args.s3_secret)
    s3_client.download_s3_obj(args.s3_key, args.local_path)