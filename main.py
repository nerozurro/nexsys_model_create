# this is a main.py file for a project
import pandas as pd
import ast #convert string to dictionary
import numpy as np
import json
import numpy as np
from calendar import monthrange

import warnings

from libs import config, json_info, read_data, recorders, globals



def main():
    
    # warnings.filterwarnings("ignore", category=DeprecationWarning) 
    warnings.filterwarnings("ignore")
    print("STARTING CONVERSION FROM XLS TO JSON")
    
    
    globals.global_tracking_variables()
    
    '''get path of xls file'''
    pathDB, export_folder_path, hydra_csv_folder_path = config.update_paths()
    
    '''dictionary of Units of Measurements (conversions)'''
    UOMs = config.UOM(pathDB)
    
    
    '''dictionary with all sheets to be read from xls file'''
    sheets_dict = config.define_main_sources(pathDB)
    sheets_dict = config.define_data_sources(pathDB,sheets_dict)
    
    # print(f"Information of sheets to read: {sheets_dict}")
     
    
    
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
    
    '''Create scenarios if any'''
    globals.is_scenario = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 3,usecols = 'O', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    globals.is_scenario = True if globals.is_scenario.lower()=='yes' else False
    
    if globals.is_scenario == True:
        print('Creating Scenarios')
        network_pywr['scenarios'] = json_info.create_scenarios(pathDB)
    else: print('No Scenarios included in the network')

    print('Creating Nodes')
    network_pywr = json_info.node_creation(data, net_comp, network_pywr)
    # network_pywr = json_info.node_creation(data, net_comp, network_pywr)
    
    df_recorders_nodes, df_recorders_parameters, df_recorders_extras = recorders.split_df_recorders(data['df_recorders'])
    
    print('Creating recorders')
    network_pywr = json_info.recorders_creation(df_recorders_nodes, df_recorders_parameters, df_recorders_extras, network_pywr, net_comp)
    
    network_pywr = json_info.set_locations(data['df_Network_Components'], network_pywr)
    
    print('Creating Edges')
    print('')
    network_pywr['edges'] = json_info.create_edges(data['df_Network'])
    
    

    
    print("pruning json file, removing extra constructors")
    network_pywr = json_info.pruning(network_pywr)
    
    json_info.export_json(network_pywr, pathDB)

    print("JSON CREATED")
    
    print("Exporting dataframes to CSV files")
    json_info.export_dataframes(data)
    print('Process finished')
    

if __name__ == "__main__":
    main()