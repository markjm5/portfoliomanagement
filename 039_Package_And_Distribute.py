from zipfile import ZipFile
from datetime import date, datetime
import os
import sys
from os.path import basename
from google_services import Create_Service
from googleapiclient.http import MediaFileUpload

CLIENT_SECRET_FILE = "google_client_secret.json"
API_NAME = "drive"
API_VERSION = "v3"
SCOPES = ["https://www.googleapis.com/auth/drive"]

service = Create_Service(CLIENT_SECRET_FILE,API_NAME,API_VERSION,SCOPES)

google_drive_folder_id = "1-hfhZ9phouewa36YVU67x2h8Q8lkXzB6"
mime_type = "application/zip"

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

    file_name = "Trading_Excel_Files-%s-%s-%s-%s-%s-%s.zip" % (todays_date.year, todays_date.month, todays_date.day, HOUR, MINUTE, SECONDS)
    file_dir = "%s/../../../Desktop/" % (sys.path[0])
    file_path = "%s/%s" % (file_dir,file_name)

    # create a ZipFile object
    with ZipFile(file_path, 'w') as zipObj:
       # Iterate over all the files in directory
       for folderName, subfolders, filenames in os.walk(dirName):
           for filename in filenames:
               #create complete filepath of file in directory
               filePath = os.path.join(folderName, filename)
               # Add file to zip
               zipObj.write(filePath)

    #os.chmod(file_path, 0o777)

    file_metadata = {
        "name": file_name,
        "parents": [google_drive_folder_id]
    }
    #file_name = "Form.pdf"

    media = MediaFileUpload(file_dir + format(file_name),chunksize=5 * 1024 * 1024,mimetype=mime_type,resumable=True)
    #import pdb; pdb.set_trace()
    request = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    )

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print("Uploaded %d%%." % int(status.progress() * 100))

    print("Upload of {} is complete.".format(file_name))


if __name__ == '__main__':
   main()