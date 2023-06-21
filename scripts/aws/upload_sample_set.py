import os 
import argparse 
import pathlib 
from cydonia.util.S3Client import S3Client 


class SampleS3Sync:
    def __init__(self):
        self.aws_key = os.environ['AWS_KEY']
        self.aws_secret = os.environ['AWS_SECRET']
        self.aws_bucket = os.environ['AWS_BUCKET']
        self.s3 = S3Client(self.aws_key, self.aws_secret)

        self.sample_dir = pathlib.Path("/research2/mtc/cp_traces/sample/block/")
        self.sample_s3_key_prefix = "workloads/"
    

    def get_s3_key(self, sample_file_path):
        workload = sample_file_path.parent.name
        sample_file_name_split = sample_file_path.stem.split("_")
        ts_method = sample_file_name_split[0]
        sample_rate = int(sample_file_name_split[1])
        random_seed = int(sample_file_name_split[2])
        bit_str = sample_file_name_split[3]

        return "{}cp-{}/{}_{}_{}_{}.csv".format(self.sample_s3_key_prefix, ts_method, workload, sample_rate, random_seed, bit_str)

    
    def sync(self):
        sample_file_list = list(self.sample_dir.rglob("*.csv"))
        s3_key_list = [self.get_s3_key(sample_file_path) for sample_file_path in sample_file_list]

        for s3_key, sample_file_path in zip(s3_key_list, sample_file_list):
            if self.s3.get_key_size(s3_key) == 0:
                self.s3.upload_s3_obj(s3_key, str(sample_file_path.absolute()))
                print("Uploaded: {} to {}".format(sample_file_path, s3_key))
            else:
                print("S3 key {} already has file {}".format(s3_key, sample_file_path))


if __name__ == "__main__":
    s3sync = SampleS3Sync()
    s3sync.sync()
