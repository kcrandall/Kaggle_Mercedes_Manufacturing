import os
import sys

from spark_controler.emr_controller import EMRController

deployer = EMRController()
deployer.profile_name = 'default'
deployer.subnet_id = 'subnet-50c2a327'
deployer.key_name = 'EMR_Key'
deployer.s3_bucket = 'emr-related-files'
deployer.master_instance_type = 'm4.large'
deployer.slave_instance_type = 'r4.4xlarge'
deployer.instance_count = 3
# deployer.run('create')

deployer.job_flow_id = 'j-3U0QGAMBK25V4'
deployer.path_script = os.path.dirname( __file__ )
deployer.file_to_run = 'test.py'
# spark-submit --packages ai.h2o:sparkling-water-core_2.11:2.1.7 --conf spark.dynamicAllocation.enabled=false
deployer.additional_job_args = ['--packages', 'ai.h2o:sparkling-water-core_2.11:2.1.7', '--conf', 'spark.dynamicAllocation.enabled=false']
deployer.run('run_job')
