import pyspark as ps
from pyspark import SparkContext, SparkConf

import os
import sys

from spark_controler.emr_controller import EMRController



deployer = EMRController()
deployer.path_script = os.path.dirname( __file__ )
deployer.file_to_run = 'test.py'
deployer.instance_count = 2
# deployer.run('create')
deployer.job_flow_id = 'j-IAUJ77TAQ96S'
deployer.run('run_job')
