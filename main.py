import phaseOne
import phaseTwo
import phaseThree
import phaseFour
import phaseFive
import logging
import os
import datetime as dt
from utils import init_spark


if __name__ == "__main__":
    spark = init_spark()
    now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
    dir_path = os.path.dirname(os.path.realpath(__file__))
    log_name = dir_path + '/data_folder/logs/log' + now + '.txt'
    logging.basicConfig(filename=log_name,level=logging.DEBUG)
    logging.debug(now + ' Start of the pipeline')
    # Running all phases
    success = phaseOne.run(spark)
    if success:
        success = phaseTwo.run(spark)
    if success:
        success = phaseThree.run(spark)
    if success:
        success = phaseFour.run(spark)
    if success:
        success = phaseFive.run(spark)
