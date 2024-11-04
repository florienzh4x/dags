import os
import stat
import json
from datetime import datetime

def get_file_details(directory, output_file=None):
    
    file_details = []
    
    for root, dirs, files in os.walk(directory):
        
        for filename in files:
            
            file_path = os.path.join(root, filename)
            file_stat = os.stat(file_path)
            
            last_access = datetime.fromtimestamp(file_stat.st_atime).isoformat()
            last_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            metadata_changed = datetime.fromtimestamp(file_stat.st_ctime).isoformat()
            
            detail = {
                "filename": filename,
                "path": file_path,
                "root_path": root,
                "last_accessed": last_access,
                "last_modified": last_modified,
                "metadata_changed": metadata_changed
            }
            
            file_details.append(detail)
    
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(file_details, f, indent=4)
        print(f"File details have been saved to {output_file}")
    else:
        print(json.dumps(file_details, indent=4))
        
def cek_edited(file_path):
    try:
        file_stat = os.stat(file_path)

        last_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        metadata_changed = datetime.fromtimestamp(file_stat.st_ctime).isoformat()

        if last_modified != metadata_changed:
            return True
        else:
            return False
    except Exception as e:
        raise e
    
def cek_access(file_path):
    try:
        file_stat = os.stat(file_path)

        last_access = datetime.fromtimestamp(file_stat.st_atime).isoformat()
        metadata_changed = datetime.fromtimestamp(file_stat.st_ctime).isoformat()

        if last_access != metadata_changed:
            return True
        else:
            return False
    except Exception as e:
        raise e

directory = "/home/wisnu/airflow/plugins/wisnu"
output_file = "metadata_cek_isiah.json"

# get_file_details(directory, output_file)

file_path = "/home/wisnu/airflow/plugins/wisnu"