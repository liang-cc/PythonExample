import os
import tarfile
from datetime import datetime, timedelta
def main():
    cwd = os.getcwd()+"/logs/"
    today = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    tar = tarfile.open("archive/qa-platform2-kafka1_{}.tar.gz".format(today), "w:gz")
    list = (fileName for fileName in os.listdir(cwd) if fileName.endswith(".txt") and fileName.find(today) != -1)
    for file in list :
        tar.add(cwd+file,arcname=file)
    tar.close()

if __name__ == '__main__':
    main()