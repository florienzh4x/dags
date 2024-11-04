import shutil

class GenerateBackup:
    
    def __init__(self, original_file, backup_file):
        
        self.original_file = original_file
        self.backup_file = backup_file
        
    def make_backup(self):
        try:
            shutil.copyfile(self.original_file, self.backup_file)
        except Exception as err:
            raise 'error cause {}'.format(err)