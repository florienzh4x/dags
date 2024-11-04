import os
import json
from airflow.models import Variable
from datetime import datetime

class Metadata:
    
    def __init__(self):

        metadata_path_env = Variable.get('DIRECTORY_PATH')
        metadata_path_json = json.loads(metadata_path_env)
        
        self.metadata_abs_path = metadata_path_json.get('metadata_folder')
        
        self.check_metadata_folder()
        
    def check_metadata_folder(self):
        if not os.path.exists(self.metadata_abs_path):
            os.mkdir(self.metadata_abs_path)
            
    def generate(self, csv_file_name, bak_file_name, csv_file_path, bak_file_path):

        metadata = {
            "csv_file_name": csv_file_name,
            "bak_file_name": bak_file_name,
            "csv_file_path": csv_file_path,
            "bak_file_path": bak_file_path,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # "updated_at": None,
            # "deleted_at": None,
            "is_pulled": False
        }
        
        # supaya nama file nya tanpa .csv
        base_name = os.path.splitext(csv_file_name)[0]
        metadata_file_path = os.path.join(self.metadata_abs_path, f"{base_name}_metadata.json")
        
        # metadata_file_path = os.path.join(self.metadata_abs_path, f"{csv_file_name}_metadata.json")
        
        with open(metadata_file_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file, indent=4)
        
        print(f"Metadata generated for {csv_file_name} and saved at {metadata_file_path}")
        return metadata_file_path

    def modify(self, metadata_file_path, delete=False, edit=None, moved=None):

        with open(metadata_file_path, 'r') as metadata_file:
            metadata = json.load(metadata_file)
        
        if delete:
            metadata['deleted_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if edit:
            metadata.update(edit)
            metadata['updated_at'] =datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if moved:
            metadata['csv_file_path'] = moved.get('new_csv_file_path', metadata['csv_file_path'])
            metadata['bak_file_path'] = moved.get('new_bak_file_path', metadata['bak_file_path'])
            metadata['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(metadata_file_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file, indent=4)
        
        print(f"Metadata modified for {metadata['csv_file_name']} and saved at {metadata_file_path}")