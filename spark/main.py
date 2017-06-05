import pyspark as ps
from pyspark import SparkContext, SparkConf

import os
import sys

from spark_controler.emr_controller import EMRController



deployer = EMRController()
deployer.path_script = os.path.dirname( __file__ )
deployer.file_to_run = 'test.py'
deployer.master_instance_type = 'm4.xlarge'
deployer.slave_instance_type = 'r4.4xlarge'
deployer.instance_count = 4
# deployer.run('create')
deployer.job_flow_id = 'j-275TC924AO8C1'
deployer.run('run_job')
