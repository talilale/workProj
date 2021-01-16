import phaseOne
import phaseTwo
import phaseThree
import phaseFour
import phaseFive
import logging
import os
import datetime as dt
from utils import init_spark

now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
dir_path = os.path.dirname(os.path.realpath(__file__))


if __name__ == "__main__":
    log_name = dir_path + '/data_folder/logs/log' + now + '.txt'
    logging.basicConfig(filename=log_name,level=logging.INFO)
    logging.info(now + ' Initiating spark')
    spark = init_spark()
    logging.info(now + ' Start of the pipeline')
    # Running all phases, imitating a pipeline where when one phases fails the rest are not running
    success = phaseOne.run(spark, log_name)
    if success:
        success = phaseTwo.run(spark, log_name)
    if success:
        success = phaseThree.run(spark, log_name)
    if success:
        success = phaseFour.run(spark, log_name)
    if success:
        success = phaseFive.run(spark, log_name)
    logging.getLogger(log_name).setLevel(logging.INFO)
    logging.info(now + ' The pipeline has finished')
