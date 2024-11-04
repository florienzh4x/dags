import os, random, csv
from scanner_utils.scanner import Scanner

class SimulationAction:
    def __init__(self):
        
        self.to_consume = Scanner.pick_csv_file()
        # self.to_consume = ['/opt/airflow/plugins/wisnu/user_patricia/test.csv']
    
    def delete_file(self, file_path):
        # Delete .csv file only 
        os.remove(file_path)
        print("PAKE OS REMOVE")
    
    def pull_file(self):
        # Simulating consume file
        for csv_path in self.to_consume:
            print("CONSUMING FILE FROM " + csv_path)
            with open(csv_path, "r") as file:
                reader = csv.reader(file, delimiter=',')
                for row in reader:
                    print(', '.join(row))
            # Randomize action
            action = random.randrange(1, 11)
            # Delete original .csv & .bak
            if action % 2 == 0:
                print("DELETING FILE " + csv_path)
                self.delete_file(csv_path)
            # Keep original file, edit metadata todo
            else:
                print("UPDATING METADATA ")
                continue
    
    def consume(self, file_path):
        
        with open(file_path, "rb") as openfile:
            openfile.read()
            openfile.close()
    
    def action(self):
        self.pull_file()
    
    def move_file(to_move):
        pass
    
    def modify_metadata():
        pass