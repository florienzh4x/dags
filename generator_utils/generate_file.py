import os, json
import random
from faker import Faker


class GenerateFile:
    
    def make_file(self, filename, context):
        try:
            
            with open(filename, "w") as files:
                files.write(context)
                files.close()
            
            return 'success'
        except Exception as err:
            raise 'error cause {}'.format(err)
        