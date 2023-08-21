from libs import config, json_info, read_data, recorders


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

import warnings
# export_folder_path='data/wensum/'
# hydra_csv_folder_path='./data/armenia/'

# hydra_csv_folder_path='/root/.hydra/data/botswana/'


# pathDB = r'C:\Leonardo\ManchesterUniversity\Projects\Wensum\data_base\pywr_network_database_xls_V1.19.7_Wensum.xlsm'

# default_location = [25.92491276, -24.70064353]



def main():
    
    # warnings.filterwarnings("ignore", category=DeprecationWarning) 
    warnings.filterwarnings("ignore")
    print("STARTING CONVERSION FROM XLS TO JSON")
    
    
    # config.global_tracking_variables()
    
    '''get path of xls file'''
    pathDB, script, export_folder_path, hydra_csv_folder_path = config.update_paths()
    
    '''dictionary with all sheets to be read from xls file'''
    sheets_dict = config.define_sources(pathDB)
    
    '''dictionary of Units of Measurements (conversions)'''
    UOMs = config.UOM(pathDB)
    
    '''read data from xls file creating all dataframes'''
    data, dict_df_units = read_data.read_data(pathDB, sheets_dict)
    
    print('Reading edges from excel file')
    data['df_Network'], data['df_Network_Components'] = json_info.get_edges(pathDB, data)
    
    print('Getting all nodes from excel file')
    data['df_Network_Components'] = json_info.get_nodes(data['df_Network_Components'], data['df_Network'])
    
    print('Creating and indentifying attributes for all nodes')
    data['df_attribute_selection'] = json_info.get_attribute_selection(data['df_attribute_selection'])
    
    net_comp = json_info.identify_components(data['df_Network'], data['df_Network_Components'])
    net_comp = net_comp.sort_values(by=['CompType'], ascending=True)
    
    print('Writting to json file')
    network_pywr = json_info.initialize_json(pathDB)

    print('Creating Nodes')
    network_pywr = json_info.node_creation(data, net_comp, network_pywr)
    
    df_recorders_nodes, df_recorders_parameters, df_recorders_extras = recorders.split_df_recorders(data['df_recorders'])
    
    print('Creating recorders')
    network_pywr = json_info.recorders_creation(df_recorders_nodes, df_recorders_parameters, df_recorders_extras, network_pywr, net_comp)
    
    network_pywr = json_info.set_locations(data['df_Network_Components'], network_pywr)
    
    print('Creating Edges')
    network_pywr['edges'] = json_info.create_edges(data['df_Network'])
    
    print("pruning json file, removing extra constructors")
    network_pywr = json_info.pruning(network_pywr)
    
    json_info.export_json(network_pywr, pathDB)

    print("JSON CREATED")

if __name__ == "__main__":
    main()