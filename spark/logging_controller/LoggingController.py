import logging
import os
import io
from datetime import datetime
import boto3
import botocore

class LoggingController(object):
    def __init__(self, profile_name = 'default', s3_bucket = 'emr-related-files'):
        self.init_datetime_string = self.get_datetime_str()                     #Used to create a s3 directory so multiple scripts don't overwrite the same files
        self.s3_bucket = s3_bucket                                              # S3 Bucket to use for storage
        self.profile_name = profile_name                                        # Define IAM profile name (see: http://boto3.readthedocs.io/en/latest/guide/configuration.html)(config file located at user folder .aws directory)

    def boto_client(self, service):
        """
        This will return a boto_client set the service i.e. 'emr' or 's3'.
        :return: boto3.client
        """
        if self.aws_access_key and self.aws_secret_access_key:
            client = boto3.client(service,
                                  aws_access_key_id=self.aws_access_key,
                                  aws_secret_access_key=self.aws_secret_access_key,
                                  region_name=self.region_name)
            return client
        else:
            session = boto3.Session(profile_name=self.profile_name)
            return session.client(service, region_name=self.region_name)

    def get_datetime_str(self):
        """
        Gets a formated datetime string for naming purposes.
        """
        return datetime.now().strftime("%Y%m%d.%H:%M:%S.%f")

    def upload_mathplotlib_to_s3(self,plot,path, format = 'png'):
        """
        Uploads mathplotlib plot to an s3 bucket.

        :param plot: The plot object to upload to s3.
        :param path: The path and file name to upload to on s3 bucket. 'dir/plot.png'
        :param path_on_s3: The path and file it should be called on s3.
        :param format: The file type to plot.savefig as.
        :return:
        """
        img_data = io.BytesIO()
        plot.savefig(img_data, format='png')
        img_data.seek(0)

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.s3_bucket)
        bucket.put_object(Body=img_data, ContentType='image/png', Key=path)
