import os 
import json
from airflow.models import Variable
from faker import Faker
import random
from datetime import datetime
from generator_utils.generate_backup import GenerateBackup
from generator_utils.generate_file import GenerateFile
from generator_utils.metadata import Metadata

class GenerateContent:
    
    def __init__(self):
        
        metadata_path_env = Variable.get('DIRECTORY_PATH')
        metadata_path_json = json.loads(metadata_path_env)
        
        self.metadata_abs_path = metadata_path_json.get('metadata_folder')
        
        self.check_metadata_folder()
        
    def check_metadata_folder(self):
        if not os.path.exists(self.metadata_abs_path):
            os.mkdir(self.metadata_abs_path)
            
    def generate_content(self):
        
        faker = Faker()
        name = faker.unique.first_name().lower()
        age = random.randint(20,30)
                
        context = """name,age\n{},{}""".format(name,age)
        
        return context
    
    def make(self, **kwargs):
        
        ti = kwargs['ti']
        
        folder_list = ti.xcom_pull(key="folder_list", task_ids="generate_folder_task")
        
        count_folder = len(folder_list)
        
        faker = Faker()
        
        folder_range = random.randrange(1, count_folder)
        
        for i in range(folder_range):
            
            folder_abs_path = random.choice(folder_list)
            
            # metadata_dict = {}
            
            name = faker.unique.first_name().lower()
            
            content = self.generate_content()
            
            original_filename = "{}/{}.csv".format(folder_abs_path, name)
            backup_filename = "{}/{}.bak".format(folder_abs_path, name)
            # metadata_filename = "{}/{}.json".format(self.metadata_abs_path, name)
                        
            file_maker = GenerateFile()
            backup_maker = GenerateBackup(original_filename, backup_filename)
            
            try:
                file_maker.make_file(original_filename, content)
                backup_maker.make_backup()
                
                ##########################################################################
                if os.path.exists(original_filename) and os.path.exists(backup_filename):
                    metadata = Metadata()
                    metadata.generate(
                        csv_file_name=os.path.basename(original_filename),
                        bak_file_name=os.path.basename(backup_filename),
                        csv_file_path=original_filename,
                        bak_file_path=backup_filename
                    )
                    print(f"Metadata generated for {original_filename}")
                ##########################################################################
                
            except Exception as err:
                raise err
            