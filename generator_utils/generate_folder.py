import os, json
from airflow.models import Variable
from faker import Faker
import random


class GenerateFolder:
    
    def __init__(self):
        
        root_path_env = Variable.get('DIRECTORY_PATH')
        root_path_json = json.loads(root_path_env)
        
        self.root_abs_path = root_path_json.get('root_folder')
        
        self.check_root()
        
    def check_root(self):
        if not os.path.exists(self.root_abs_path):
            os.mkdir(self.root_abs_path)
            
    def generate(self, **kwargs):
        
        ti = kwargs['ti']
        
        pattern_name = 'user_{}'
        
        fake_name = Faker()
        
        folder_count = random.randrange(1, 6)
        
        folder_list = []
        
        try:
            for i in range(folder_count):
                
                folder_name = pattern_name.format(fake_name.unique.first_name())
                
                folder = '{}/{}'.format(self.root_abs_path, folder_name.lower())
                
                if not os.path.exists(folder):
                    os.mkdir(folder)
                    folder_list.append(folder)
                else:
                    pass
        except Exception as err:
            print(str(err))
            
        ti.xcom_push(key="folder_list", value=folder_list)
        # return folder_list
                
        
            
        