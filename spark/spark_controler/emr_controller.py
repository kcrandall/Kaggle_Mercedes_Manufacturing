import boto3
import botocore
import yaml
import time
import logging
import os
from datetime import datetime
import tarfile
# https://medium.com/@datitran/quickstart-pyspark-with-anaconda-on-aws-660252b88c9a

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class EMRController(object):
    def __init__(self, profile_name = 'default', aws_access_key = False, aws_secret_access_key = False, region_name = 'us-east-1',
                 cluster_name = 'Spark-Cluster', instance_count = 3, master_instance_type = 'm3.xlarge', slave_instance_type = 'm3.xlarge',
                 key_name = 'EMR_Key', subnet_id = 'subnet-50c2a327', software_version = 'emr-5.5.0', s3_bucket = 'emr-related-files' ):
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.cluster_name = cluster_name+'_'+self.get_datetime_str()            # Application name
        self.instance_count = instance_count
        self.master_instance_type = master_instance_type
        self.slave_instance_type = slave_instance_type
        self.key_name = key_name
        self.subnet_id = subnet_id
        self.software_version = software_version
        self.profile_name = profile_name                                        # Define IAM profile name (see: http://boto3.readthedocs.io/en/latest/guide/configuration.html)(config file located at user folder .aws directory)
        self.s3_bucket = s3_bucket                                              # S3 Bucket to use for storage
        self.path_script = os.path.dirname( __file__ )
        self.file_to_run = 'test.py'                                            # The file you want to run from the compressed files
        self.job_flow_id = None                                                 # AWS's unique ID

    def boto_client(self, service):
        if self.aws_access_key and self.aws_secret_access_key:
            client = boto3.client(service,
                                  aws_access_key_id=self.aws_access_key,
                                  aws_secret_access_key=self.aws_secret_access_key,
                                  region_name=self.region_name)
            return client
        else:
            session = boto3.Session(profile_name=self.profile_name)
            return session.client(service, region_name=self.region_name)

    def load_cluster(self):
        response = self.boto_client("emr").run_job_flow(
            Name=self.cluster_name,
            LogUri='s3://'+self.s3_bucket+'/logs',
            ReleaseLabel=self.software_version,
            Instances={
                'MasterInstanceType': self.master_instance_type,
                'SlaveInstanceType': self.slave_instance_type,
                'InstanceCount': self.instance_count,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
                'Ec2SubnetId': self.subnet_id
            },
            Applications=[
                {
                    'Name': 'Spark'
                },
                # {
                #     'Name': 'Hadoop'
                # }
            ],
            BootstrapActions=[
                {
                    'Name': 'Install Conda',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{s3_bucket}/temp/bootstrap_actions.sh'.format(
                            s3_bucket=self.s3_bucket),
                    }
                },
                # UNCOMMENT FOR AUTOTERMINATE BEHAVIOR
                # {
                #     'Name': 'idle timeout',
                #     'ScriptBootstrapAction': {
                #         'Path':'s3n://{}/{}/terminate_idle_cluster.sh'.format(self.s3_bucket + '/' + self.s3_path_temp_files, self.job_name),
                #         'Args': ['3600', '300']
                #     }
                # },
            ],
            Configurations=[
            #     {
            #         'Classification': 'spark-env',
            #         'Configurations': [
            #             {
            #                 "Classification": "export",
            #                 "Properties": {
            #                     "PYSPARK_PYTHON": "python34",
            #                     "PYSPARK_PYTHON": "/home/hadoop/conda/bin/python",
            #                     "PYSPARK_DRIVER_PYTHON":"/home/hadoop/conda/bin/python"
            #                 },
            #                 "Configurations": []
            #             }
            #         ],
            #         'Properties': {
            #         }
            #     },
            #     {
            #         "Classification": "hadoop-env",
            #         "Properties": {
            #
            #         },
            #         "Configurations": [
            #           {
            #             "Classification": "export",
            #             "Properties": {
            #               "HADOOP_DATANODE_HEAPSIZE": "2048",
            #               "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"
            #             },
            #             "Configurations": [
            #
            #             ]
            #           }
            #         ]
            #   },
              {
                  "Classification": "hadoop-env",
                  "Properties": {

                  },
                  "Configurations": [
                    {
                      "Classification": "export",
                      "Properties": {
                          "PYTHONHASHSEED": "123",
                      },
                      "Configurations": [

                      ]
                    }
                  ]
            }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        logger.info(response)
        return response

    def add_create_step(self, job_flow_id, master_dns):
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{s3_bucket}/temp/pyspark_quick_setup.sh'.format(
                                     s3_bucket=self.s3_bucket),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'setup pyspark with conda',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'bash', '/home/hadoop/pyspark_quick_setup.sh', master_dns]
                    }
                }
            ]
        )
        logger.info(response)
        return response

    def add_run_step(self, job_flow_id):
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'Copy_Tar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{s3_bucket}/temp/script.tar.gz'.format(
                                     s3_bucket=self.s3_bucket),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Decompress script.tar.gz',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['tar', 'zxvf', '/home/hadoop/script.tar.gz','-C','/home/hadoop/']
                    }
                },
                {
                    'Name': 'Spark Application',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "spark-submit",
                            "/home/hadoop/" + self.file_to_run
                        ]
                    }
                }
            ]
        )
        logger.info(response)
        time.sleep(1)
        return response

    def create_bucket_on_s3(self, bucket_name):
        s3 = self.boto_client("s3")
        try:
            logger.info("Bucket already exists.")
            s3.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            logger.info("Bucket does not exist: {error}. I will create it!".format(error=e))
            s3.create_bucket(Bucket=bucket_name)

    def upload_to_s3(self, path_to_file, bucket_name, path_on_s3):
        logger.info(
            "Upload file '{file_name}' to bucket '{bucket_name}'".format(file_name=path_on_s3, bucket_name=bucket_name))
        s3 = None
        if self.aws_access_key and self.aws_secret_access_key:
            s3 = self.boto_client("s3")
            s3.upload_file(path_to_file, bucket_name, path_on_s3)
        else:
            s3 = boto3.Session(profile_name=self.profile_name).resource('s3')
            s3.Object(bucket_name, path_on_s3)\
              .put(Body=open(path_to_file, 'rb'), ContentType='text/x-sh')





    def get_datetime_str(self):
        """
        Gets a formated datetime string for naming purposes.
        """
        return datetime.now().strftime("%Y%m%d.%H:%M:%S.%f")

    def generate_job_name(self):
        self.job_name = "{}.{}.{}".format(self.app_name,
                                          self.user,
                                          self.get_datetime_str())

    def tar_python_script(self):
        """

        :return:
        """
        # Create tar.gz file
        t_file = tarfile.open(os.path.dirname( __file__ )+"/files/script.tar.gz", 'w:gz')
        # Add Spark script path to tar.gz file
        files = os.listdir(self.path_script)
        for f in files:
            t_file.add(self.path_script + '/' + f, arcname=f)
        # List all files in tar.gz
        for f in t_file.getnames():
            logger.info("Added %s to tar-file" % f)
        t_file.close()

    def remove_temp_files(self, s3):
        """
        Remove Spark files from temporary bucket
        :param s3:
        :return:
        """
        bucket = s3.Bucket(self.s3_bucket)
        for key in bucket.objects.all():
            if key.key.startswith(self.job_name) is True:
                key.delete()
                logger.info("Removed '{}' from bucket for temporary files".format(key.key))

    def run(self,execute_type='create'):
        if execute_type == 'create':
            logger.info(
                "*******************************************+**********************************************************")
            logger.info("Load config and set up client.")

            logger.info(
                "*******************************************+**********************************************************")
            logger.info("Check if bucket exists otherwise create it and upload files to S3.")
            self.create_bucket_on_s3(bucket_name=self.s3_bucket)
            self.upload_to_s3(os.path.dirname( __file__ )+"/scripts/bootstrap_actions.sh", bucket_name=self.s3_bucket,
                                    path_on_s3="temp/bootstrap_actions.sh")
            self.upload_to_s3(os.path.dirname( __file__ )+"/scripts/pyspark_quick_setup.sh", bucket_name=self.s3_bucket,
                                    path_on_s3="temp/pyspark_quick_setup.sh")
            self.upload_to_s3(os.path.dirname( __file__ )+"/scripts/terminate_idle_cluster.sh", bucket_name=self.s3_bucket,
                                    path_on_s3="temp/terminate_idle_cluster.sh")

            logger.info(
                "*******************************************+**********************************************************")
            logger.info("Create cluster and run boostrap.")
            emr_response = self.load_cluster()
            emr_client = self.boto_client("emr")
            self.job_flow_id = emr_response.get("JobFlowId")
            while True:
                job_response = emr_client.describe_cluster(
                    ClusterId=emr_response.get("JobFlowId")
                )
                time.sleep(10)
                if job_response.get("Cluster").get("MasterPublicDnsName") is not None:
                    master_dns = job_response.get("Cluster").get("MasterPublicDnsName")

                step = True

                job_state = job_response.get("Cluster").get("Status").get("State")
                job_state_reason = job_response.get("Cluster").get("Status").get("StateChangeReason").get("Message")

                if job_state in ["TERMINATING","TERMINATED","TERMINATED_WITH_ERRORS"]:
                    step = False
                    logger.info(
                        "Script stops with state: {job_state} "
                        "and reason: {job_state_reason}".format(job_state=job_state, job_state_reason=job_state_reason))
                    break
                elif job_state in ["WAITING","RUNNING"]:
                    step = True
                    break
                else: # BOOTSTRAPPING,STARTING
                    logger.info(job_response)

            if step:
                logger.info(
                    "*******************************************+**********************************************************")
                logger.info("Run steps.")
                add_step_response = self.add_create_step(emr_response.get("JobFlowId"), master_dns)

                while True:
                    list_steps_response = emr_client.list_steps(ClusterId=emr_response.get("JobFlowId"),
                                                                StepStates=["COMPLETED"])
                    time.sleep(10)
                    if len(list_steps_response.get("Steps")) == len(
                            add_step_response.get("StepIds")):  # make sure that all steps are completed
                        break
                    else:
                        logger.info(emr_client.list_steps(ClusterId=emr_response.get("JobFlowId")))
                return True
            else:
                logger.info("Cannot run steps.")
                return False
        elif execute_type == 'run_job':
            self.tar_python_script()
            self.upload_to_s3(os.path.dirname( __file__ )+'/files/script.tar.gz', bucket_name=self.s3_bucket,
                                    path_on_s3="temp/script.tar.gz")
            self.add_run_step(self.job_flow_id)
            return True
    def step_copy_data_between_s3_and_hdfs(self, c, src, dest):
        """
        Copy data between S3 and HDFS (not used for now)
        :param c:
        :return:
        """
        response = c.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[{
                    'Name': 'Copy data from S3 to HDFS',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "s3-dist-cp",
                            "--s3Endpoint=s3-eu-west-1.amazonaws.com",
                            "--src={}".format(src),
                            "--dest={}".format(dest)
                        ]
                    }
                }]
        )
        logger.info("Added step 'Copy data from {} to {}'".format(src, dest))
