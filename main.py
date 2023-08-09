from libs import config


# this is a main.py file for a project
import pandas as pd
import json
import ast #connvert string to dictionary
import numpy as np
import os

import pandas as pd
import json
import ast #connvert string to dictionary
import numpy as np
import os
from calendar import monthrange
# export_folder_path='data/wensum/'
# hydra_csv_folder_path='./data/armenia/'

# hydra_csv_folder_path='/root/.hydra/data/botswana/'


# pathDB = r'C:\Leonardo\ManchesterUniversity\Projects\Wensum\data_base\pywr_network_database_xls_V1.19.7_Wensum.xlsm'

# default_location = [25.92491276, -24.70064353]









def main():
    print("HELLO")
    
    pathDB, script, export_folder_path, hydra_csv_folder_path = config.update_paths()
    # print(pathDB)
    sheets_dict = config.define_sources(pathDB)
    # sheets_dict = config.update_paths()   
    print(sheets_dict)
    UOMs = config.UOM(pathDB)
    print(UOMs)
    
    

if __name__ == "__main__":
    main()