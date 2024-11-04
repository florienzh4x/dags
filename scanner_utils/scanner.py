import json, os
from datetime import datetime
from airflow.models import Variable

class Scanner():
    """docstring for ClassName."""
    def __init__(self):
        
        root_path_env = Variable.get('DIRECTORY_PATH')
        root_path_json = json.loads(root_path_env)
        
        self.root_abs_path = root_path_json.get('root_folder')
        
    def scan_folder(self):
        
        directory_structure = {}

        for folder_name in os.listdir(self.root_abs_path):
            folder_path = os.path.join(self.root_abs_path, folder_name)
        
        # root_path = "/mnt/d/KELAS DE CENAH/KODE/dags/files"
            
        # for folder_name in os.listdir(root_path):
        #     folder_path = os.path.join(root_path, folder_name)
            
            # user_folder = '/'.join([root_path,folder_name])
            user_folder = '/'.join([self.root_abs_path,folder_name])

            if os.path.isdir(folder_path):
                files = os.listdir(folder_path)
                directory_structure[user_folder] = files

        return directory_structure
    
    def pick_csv_file(self):
        
        list_of_folder = self.scan_folder()
        
        list_dir = []
        
        for folder, files in list_of_folder.items():

            # check if list empty
            # pass for now
            if not files:
                pass
            else:
                for file in files:
                    if file.endswith('.csv'):
                        list_dir.append('/'.join([folder,file]))
        
        return list_dir
    
    def detailed_scan(self):
        
        list_of_folder = self.scan_folder()
        
        csv_only = []
        bak_only = []
        have_both = []
        
        for folder, files in list_of_folder.items():
            if len(files) == 0:
                pass
            else:
                for file in files:
                    if file.endswith('.csv'):
                        ### CHECK BAK FILE
                        if os.path.exists('/'.join([folder, file.replace('.csv', '.bak')])):
                            #### FILE BAK FOUND
                            have_both.append('/'.join([folder, file]))
                            # have_both.append('/'.join([folder, file.replace('.csv', '.bak')]))
                        else:
                            #### FILE BAK NOT FOUND
                            csv_only.append('/'.join([folder, file]))
                    elif file.endswith('.bak'):
                        if os.path.exists('/'.join([folder, file.replace('.bak', '.csv')])):
                            #### FILE CSV FOUND
                            # have_both.append('/'.join([folder, file]))
                            have_both.append('/'.join([folder, file.replace('.bak', '.csv')]))
                        else:
                            #### FILE CSV NOT FOUND
                            bak_only.append('/'.join([folder, file]))
        
        return {
            'csv_only' : csv_only,
            'bak_only' : bak_only,
            'have_both': list(dict.fromkeys(have_both))
        }
        
    def scan_empty_folder(self):
        
        list_of_folder = self.scan_folder()
        
        return [key for key, value in list_of_folder.items() if len(value) == 0]
    
    def cek_edited(self,file_path):
        try:
            file_stat = os.stat(file_path)

            last_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            create_changed = datetime.fromtimestamp(file_stat.st_ctime).isoformat()

            if last_modified != create_changed:
                return True
            else:
                return False
        except Exception as e:
            raise e
        
    def cek_access(self,file_path):
        try:
            file_stat = os.stat(file_path)

            last_access = datetime.fromtimestamp(file_stat.st_atime).isoformat()
            create_changed = datetime.fromtimestamp(file_stat.st_ctime).isoformat()

            if last_access != create_changed:
                return file_stat.st_atime
            else:
                return False
        except Exception as e:
            raise e