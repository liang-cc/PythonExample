import os
import tarfile
from datetime import datetime, timedelta
import logging
def main():
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S',
                        level=logging.INFO)
    cwd = os.getcwd()+"/logs/"
    today = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    logging.info("archive yesterday's log files as {} udner folder ./archive".format("qa-platform2-kafka1_{}.tar.gz".format(today)))
    tar = tarfile.open("archive/qa-platform2-kafka1_{}.tar.gz".format(today), "w:gz")
    list = (fileName for fileName in os.listdir(cwd) if fileName.endswith(".txt") and fileName.find(today) != -1)
    for file in list :
        logging.info("add {} into gz file".format(file))
        tar.add(cwd+file,arcname=file)
    logging.info("archive file completed")
    tar.close()

if __name__ == '__main__':
    main()