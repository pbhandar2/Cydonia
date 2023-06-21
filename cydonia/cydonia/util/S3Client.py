""" This class manages the data in S3 bucket 'mtcachedata'. """

import json 
import pathlib 
import boto3
from botocore.exceptions import ClientError

class S3Client:
    def __init__(self, s3_key, s3_secret):
        self.bucket_name = "mtcachedata"
        self.s3_key = s3_key
        self.s3_secret = s3_secret

        self.s3 = boto3.client('s3',
                                aws_access_key_id=s3_key, 
                                aws_secret_access_key=s3_secret)
        self.s3_resource = boto3.resource('s3')


    def download_s3_obj(self, key, local_path):
        try:
            self.s3.download_file(self.bucket_name, key, local_path)
        except ClientError as e:
            raise ValueError("{}::(Error downloading object at {} with key {})".format(e, local_path, key))


    def upload_s3_obj(self, key, local_path):
        try:
            self.s3.upload_file(local_path, self.bucket_name, key)
        except ClientError as e:
            raise ValueError("{}::(Error uploading object from {} with key {})".format(e, local_path, key))


    def delete_s3_obj(self, key):
        """ Delete the given key
            
            Parameters
            ----------
            key : str 
                the key of the object to delete 
            
            Return 
            ------
            response : JSON 
                the response from S3 API client 
        """
        try:
            return self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            raise ValueError("{}::(Error deleting key {}".format(e, key))


    def get_key_size(self, key):
        """ Get size of the S3 object with the given key. 

            Parameters
            ----------
            key : str 
                the key of the object 

            Return 
            ------
            size : int 
                size of object with the provided key  
        """
        list_api_return = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        if (list_api_return['KeyCount'] == 0):
            return 0 
        else:
            return int(list_api_return['Contents'][0]['Size'])
    

    def check_prefix_exist(self, prefix):
        """ Check if any key with given prefix exists. """
        
        self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, MaxKeys=1)
        return 'Contents' in res
    

    def get_all_s3_content(self, prefix):
        """ Get all the keys from a given bucket. 

            Parameters
            ----------
            prefix : str 
                the prefix to match all the keys to 
            
            Return 
            ------
            s3_content : list 
                the list of S3 keys matching the provided prefix 
        """
        s3_content = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page["Contents"]:
                if obj['Size'] > 0:
                    s3_content.append(obj['Key'])
        return s3_content


    def copy_object(self, source_key, destination_key):
        """ Copy an S3 object from source key to a destination key. 

            Parameters
            ----------
            source_key : str 
                the key of the source object 
            destination_key : str 
                the key where the source object will be copied to 
        """
        s3 = boto3.resource('s3',
                            aws_access_key_id=self.s3_key, 
                            aws_secret_access_key=self.s3_secret)
        copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
        s3.meta.client.copy(copy_source, self.bucket_name, destination_key)