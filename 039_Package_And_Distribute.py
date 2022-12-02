from zipfile import ZipFile
from datetime import date, datetime
import os
from os.path import basename

# Zip the files from given directory that matches the filter
def zipFilesInDir(dirName, zipFileName, filter):
   # create a ZipFile object
   with ZipFile(zipFileName, 'w') as zipObj:
       # Iterate over all the files in directory
       for folderName, subfolders, filenames in os.walk(dirName):
           for filename in filenames:
               if filter(filename):
                   # create complete filepath of file in directory
                   filePath = os.path.join(folderName, filename)
                   # Add file to zip
                   zipObj.write(filePath, basename(filePath))
def main():
    # Name of the Directory to be zipped
    dirName = 'Trading_Excel_Files'

    #get date range
    todays_date = date.today()

    HOUR = datetime.now().hour   # the current hour
    MINUTE = datetime.now().minute # the current minute
    SECONDS = datetime.now().second

    file_name = "Trading_Excel_Files-%s-%s-%s-%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day, HOUR, MINUTE, SECONDS)

    # create a ZipFile object
    with ZipFile(file_name + '.zip', 'w') as zipObj:
       # Iterate over all the files in directory
       for folderName, subfolders, filenames in os.walk(dirName):
           for filename in filenames:
               #create complete filepath of file in directory
               filePath = os.path.join(folderName, filename)
               # Add file to zip
               zipObj.write(filePath)

if __name__ == '__main__':
   main()