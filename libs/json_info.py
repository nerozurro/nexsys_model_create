import pandas as pd
import numpy as np
import json
import os

from libs import parameters, globals, recorders, apply_conversions

def json_name(pathDB):
    
    json_file_name=pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'F', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    json_file_name=json_file_name+str('.json')
    
    return json_file_name


def json_metadata_tittle(pathDB):
    return pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'F', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]


def json_metadata_description(pathDB):
    return pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 3,usecols = 'F', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]



def create_scenarios(pathDB):
        
    scn_df = pd.read_excel(pathDB,sheet_name='scenarios',skiprows = 4,usecols = 'C:F')  
    scn_df = scn_df[scn_df['Scenario Name'].notnull()]
    scn_df.reset_index(inplace=True, drop=True)
    
    scenarios = []
    
    for index, row in scn_df.iterrows():
        
        row = row.dropna()
        row = row.to_dict()
               
        try:
            row['size'] = int(row['size'])
        except:
            pass
        
        try:
            row['slice'] = [x.strip() for x in row['slice'].split(', ')]
            row['slice'] = [int(i) for i in row['slice']]
        except:
            pass
        
        try:
            row['ensemble_names'] = row['ensemble_names'].replace("\n", "")
            row['ensemble_names'] = [x.strip() for x in row['ensemble_names'].split(', ')]
        except:
            pass
    
        scenarios.append(row)
    
    return scenarios


# def json_scenarios_old(pathDB):  # Scenarios still to be implemented
#     globals.global_tracking_variables()
    
#     # scenarios_name = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 0,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    
#     try:
#         globals.scenarios_name = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 0,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    
#         if pd.isna(globals.scenarios_name):
#             globals.scenarios_name=None
            
#     except:
#         globals.scenarios_name=None
#         # pass
        
#     try:
#         scenarios_size = int(pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"])
    
#     except:
#         print(f"Scenario size is not a valid value")
#         scenarios_size = None
    
    
#     try:
#         scenarios_description = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
#     except: scenarios_description = None
        
        
#     if globals.scenarios_name!=None and scenarios_size!= None:
#         print(f"Model will under scenario configuration")
#         print(f"scenario name: {globals.scenarios_name} and scenario size: {scenarios_size}")
#         globals.is_scenario = True
    
#     else:
#         print(f"Model will be created without using scenarios")
#         print(f"To create Model with scenarios, please fill the fields scenarios_name and scenarios_size in the excel file")
#         globals.is_scenario = False
        
#     return globals.scenarios_name, scenarios_size, scenarios_description, globals.is_scenario


def json_timestepper_start(pathDB):
    return pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'K', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]


def json_timestepper_end(pathDB):
    return pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 3,usecols = 'K', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]


def json_timestepper_timestep(pathDB):
    timestepper_timestep = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'K', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    
    try:
        timestepper_timestep = int(timestepper_timestep)
    except:
        pass
    
    return timestepper_timestep

    # comp2type_df = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 4,usecols = 'N:O')
    # comp2type_df.dropna(inplace=True, how='any')
    # comp2type = {}
    # for index, row in comp2type_df.iterrows():
    #     comp2type[row['Node Component']]= row['Node Type']
    
def fill_empty_locations(df_Network_Components,df_Network):
    
    default_location = [df_Network_Components["location_lat"].mean(), df_Network_Components["location_long"].mean()]
    
    df_empty_loc_Network_Components = df_Network_Components.copy()
    df_empty_loc_Network_Components = df_empty_loc_Network_Components[df_empty_loc_Network_Components['location_lat'].isna()]
    df_empty_loc_Network_Components.reset_index(inplace=True, drop=True)
    
    max_lat = df_Network_Components['location_lat'].max()
    min_lat = df_Network_Components['location_lat'].min()
    max_lon = df_Network_Components['location_long'].max()
    min_lon = df_Network_Components['location_long'].min()    
    
    for index, row in df_empty_loc_Network_Components.iterrows():
            nameNode = row['name']

            df_filtered = df_Network.query("StartNodeName == @nameNode or EndNodeName == @nameNode")
            neighbors_list = pd.melt(df_filtered, value_vars=['StartNodeName', 'EndNodeName'], ignore_index=False).value.unique()
            df_filtered = df_Network_Components[df_Network_Components['name'].isin(neighbors_list)].copy()
            df_filtered = df_filtered[df_filtered['location_lat'].notna()]
        
            
            if len(df_filtered)==0:
                # print(f"{nameNode} will have default locations")
                new_lat = default_location[0] + ((max_lat-min_lat)/10) * np.random.randint(-10,10,size=1) *0.1
                new_lon = default_location[1] + ((max_lon-min_lon)/10) * np.random.randint(-10,10,size=1) *0.1
                
            elif len(df_filtered)==1:
                # print(f"{nameNode} has 1 neighbor with coordinates")
                new_lat = df_filtered['location_lat'].mean()  + ((max_lat-min_lat)/10) * np.random.randint(-10,10,size=1) *0.1
                new_lon = df_filtered['location_long'].mean() + ((max_lon-min_lon)/10) * np.random.randint(-10,10,size=1) *0.1
                
            else:
                # print(f"{nameNode} has more than 1 neighbor with coordinates")
                new_lat = df_filtered['location_lat'].mean()
                new_lon = df_filtered['location_long'].mean()
            
            try: # Try to convert each item to float 3 decimals
                new_lat = float(new_lat)
                new_lon = float(new_lon)
                new_lat = float(format(new_lat, '.4e'))
                new_lon = float(format(new_lon, '.4e'))
            except:
                pass
            
            df_Network_Components.loc[df_Network_Components['name'] == nameNode,'location_lat'] = new_lat
            df_Network_Components.loc[df_Network_Components['name'] == nameNode,'location_long'] = new_lon
            
    df_Network_Components = df_Network_Components[['Type','name','Node Source','location_lat','location_long']]
    
    return df_Network_Components

# def fill_empty_locations_old(df_Network_Components,df_Network):
    
#     default_location = [df_Network_Components["location_lat"].mean(), df_Network_Components["location_long"].mean()]
    
#     df_empty_loc_Network_Components = df_Network_Components.copy()
#     df_empty_loc_Network_Components = df_empty_loc_Network_Components[df_empty_loc_Network_Components['location_lat'].isna()]
#     df_empty_loc_Network_Components.reset_index(inplace=True, drop=True)
    
#     for index, row in df_empty_loc_Network_Components.iterrows():
#             nameNode = row['name']

#             df_filtered = df_Network.query("StartNodeName == @nameNode or EndNodeName == @nameNode")
#             neighbors_list = pd.melt(df_filtered, value_vars=['StartNodeName', 'EndNodeName'], ignore_index=False).value.unique()
#             df_filtered = df_Network_Components[df_Network_Components['name'].isin(neighbors_list)].copy()
#             df_filtered = df_filtered[df_filtered['location_lat'].notna()]
            
#             if len(df_filtered)==0:
#                 # print(f"{nameNode} will have default locations")
#                 new_lat = default_location[0] * (1 + 0.003* np.random.randint(-10,10,size=1))
#                 new_lon = default_location[1] * (1 + 0.003* np.random.randint(-10,10,size=1))
                
#             elif len(df_filtered)==1:
#                 # print(f"{nameNode} has 1 neighbor with coordinates")
#                 new_lat = df_filtered['location_lat'].mean() * (1 + 0.0003* np.random.randint(-10,10,size=1))
#                 new_lon = df_filtered['location_long'].mean() * (1 + 0.0003* np.random.randint(-10,10,size=1))
                
#             else:
#                 # print(f"{nameNode} has more than 1 neighbor with coordinates")
#                 new_lat = df_filtered['location_lat'].mean()
#                 new_lon = df_filtered['location_long'].mean()
            
#             try: # Try to convert each item to float 3 decimals
#                 new_lat = float(new_lat)
#                 new_lon = float(new_lon)
#                 new_lat = float(format(new_lat, '.4e'))
#                 new_lon = float(format(new_lon, '.4e'))
#             except:
#                 pass
            
#             df_Network_Components.loc[df_Network_Components['name'] == nameNode,'location_lat'] = new_lat
#             df_Network_Components.loc[df_Network_Components['name'] == nameNode,'location_long'] = new_lon
            
#     df_Network_Components = df_Network_Components[['Type','Status','name','Node Source','location_lat','location_long']]
    
#     return df_Network_Components
    
    
    
def get_nodes(df_Network_Components, df_Network):
    
    df_Network_Components['Node Source'].fillna(df_Network_Components['name'], inplace=True)
    df_Network_Components = df_Network_Components.dropna(subset=['name'])
    df_Network_Components.reset_index(inplace=True, drop=True)    
    df_Network_Components = fill_empty_locations(df_Network_Components, df_Network)
    
    return df_Network_Components
    
    


def get_edges(pathDB, data):
    
    df_Network_Components = data['df_Network_Components']
    df_Network=pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 3,usecols = 'C', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    
    print(f"Network to be created: {df_Network}")
    
    df_Network_Components = df_Network_Components[df_Network_Components['name'].notna()]
    df_Network_Components = df_Network_Components[df_Network_Components['Type'].notna()]
    df_Network_Components.reset_index(inplace=True, drop=True)
    df_Network = data[str(str("df_")+df_Network)]
    df_Network = df_Network[['StartNodeName', 'EndNodeName']]
    df_Network.reset_index(inplace=True, drop=True)
    
    return df_Network, df_Network_Components
    
    
def get_attribute_selection(df_attribute_selection):
    
    df_attribute_selection = df_attribute_selection[df_attribute_selection['attributes']!=0]
    df_attribute_selection = df_attribute_selection[df_attribute_selection['attributes']!='0']
    df_attribute_selection = df_attribute_selection[df_attribute_selection['attributes'].notna()]
    df_attribute_selection = df_attribute_selection[df_attribute_selection['Parameter Type'].notna()]
    df_attribute_selection.reset_index(inplace=True, drop=True)
    
    return df_attribute_selection


def create_edges(this_dataframe):
    """
    Import Dataframe which has the network created by the user
    Returns list form, for the 'edges'
    """
    this_dataframe = this_dataframe[this_dataframe.StartNodeName != this_dataframe.EndNodeName]
    this_dataframe.reset_index(inplace=True)
    edge_list=[]
    
    for index, row in this_dataframe.iterrows():
        this_list = row[['StartNodeName','EndNodeName']].tolist()
        edge_list.append(this_list)
        
    return edge_list


def identify_components(this_dataframe, all_nodes):
    """
    receive as input the network created by the user and identify from columns 
    StartNodeType, StartNodeName, EndNodeType, EndNodeName which are all the nodes used (unique)
    If a name is duplicated (same name for 2 different nodes), print a warning and identify duplicated names
    It returns Dataframe with unique components (name and type) to be used in JSON key 'Nodes'
    """
    unique_components = pd.DataFrame()
    unique_components = pd.concat([this_dataframe[['StartNodeName']].rename(columns={'StartNodeName':'CompName'}),
                                   this_dataframe[['EndNodeName']].rename(columns={'EndNodeName':'CompName'})],
                                   ignore_index=True)
    
    unique_components=unique_components.drop_duplicates().reset_index(drop=True)
    
#     duplicatedNames = unique_components[unique_components['CompName'].duplicated()]
    
    unique_components=pd.merge(unique_components,all_nodes, left_on='CompName', right_on='name', how='left')  
    unique_components = unique_components.rename(columns={'Type':'CompType'})
    unique_components = unique_components[['CompName', 'CompType']]
    df_no_assignedNodes = unique_components[unique_components['CompType'].isnull()]
    
    if len(df_no_assignedNodes)>0:
        print(f"ERROR: The following nodes doesnt have a Node Type Assigned:  {df_no_assignedNodes}")
    return unique_components


def input_node_location(_node, _df):
    """
    For the location (latitude and longitude) inputed in the xls file, read and update nodes for existing locations
    """
    
    _node['position']={'geographic':[_df[['location_long']],_df[['location_lat']]]}
    
    return _node

    
def delete_none(_dict): 
    """Delete None values from all of the dictionaries"""
    """From https://stackoverflow.com/questions/33797126/proper-way-to-remove-keys-in-dictionary-with-none-values-in-python"""
    
    for key, value in list(_dict.items()):
        
        if isinstance(value, dict):
            delete_none(value)
            
        elif value is None:
            del _dict[key]
            
        elif isinstance(value, list):
            for v_i in value:
                if isinstance(v_i, dict):
                    delete_none(v_i)
                    
    return _dict


def delete_nones(_list):
    """
    Iterate throw list of dictionaries to delete none values.
    This function send node by node wo delete_none function in order to check and delete nones
    """
    
    output_delete_nones = []
    
    for this_dict in _list:
         output_delete_nones.append(delete_none(this_dict))
         
    return output_delete_nones        
    
    
def initialize_json(pathDB):
    # Initialize nodes with name and type
    
    network_pywr={}
    network_pywr['metadata'] = {
        "title": json_metadata_tittle(pathDB),
        "description": json_metadata_description(pathDB)
        }

    network_pywr['timestepper'] = {
        "start": json_timestepper_start(pathDB),
        "end": json_timestepper_end(pathDB),
        "timestep": json_timestepper_timestep(pathDB)
        }
    
    # globals.scenarios_name, scenarios_size, scenarios_description, globals.is_scenario = json_scenarios(pathDB)
    
    # if globals.is_scenario == True:
        
    #     network_pywr['scenarios'] = [
    #         {                
    #             "name": globals.scenarios_name,
    #             "size": scenarios_size,
    #             "comment": scenarios_description
    #         }
    #     ]

    network_pywr['nodes']=[]
    network_pywr['parameters']={}
    network_pywr['recorders']={}
    
    return network_pywr


def node_creation(data, net_comp, network_pywr):
    
    df_Network_Components = data['df_Network_Components']
    df_attribute_selection = data['df_attribute_selection']
    
    index_nodes=0
    for index, row in net_comp.iterrows():
    #     print(f"comp2type {comp2type[row['CompType']], row['CompType']}")
        
        node_created, df_attributes_paremeters, df_manual_parameters = createNode(row['CompName'], row['CompType'], data)
        
        node_created_copy = node_created.copy()
        
        # print(f"     ######################################################################")
        # print(f"     node created: {node_created}")
        # print(f"     ######################################################################")
        
        network_pywr['nodes'].append(node_created_copy)
        
        if (len(df_attributes_paremeters)>0):
    #         print(f"df_attributes_paremeters {df_attributes_paremeters}")
    
            for index, param_row in df_attributes_paremeters.iterrows():
                parameter_created = parameters.createParameter(node_created, param_row, data)
                network_pywr['parameters'].update(parameter_created)
                
        if (len(df_manual_parameters)>0):
    #         print(f"df_manual_parameters {df_manual_parameters}")
    
            for index, param_row in df_manual_parameters.iterrows():
                parameter_created = parameters.createParameter(node_created, param_row, data)
                network_pywr['parameters'].update(parameter_created)      
                
        index_nodes+=1
    
    
    network_pywr, param_not_included = parameters.create_extra_parameters(network_pywr, data['df_extra_parameters'])    
    
    print("Please check the following parameters, they are not included in any node:\n", param_not_included)
    
    
    return network_pywr


def createNode(this_name, this_type, data):
    """
    After identify_components function is used. All nodes are created one by one.
    This function has as input name, type and source if it is a dependent node (eg inflow, evaporation),
    meaning it is associated to another node
    It creates node with name and type (all nodes have this 2 attributes) and call complete_node function
    which complete the node attributes from the pre defined attributes in attr_per_nodes dictionary.
    It returns node with almost all attributes (not geographic location)
    """
    
    df_Network_Components = data['df_Network_Components']
    df_attribute_selection = data['df_attribute_selection']
    
    # print(f"Starting node creation: {this_name} as {this_type}")

    node={}
    node['name']=this_name
    node['type']=this_type


    this_source = df_Network_Components[df_Network_Components['name']==this_name]['Node Source'].item()
    
    if (this_source!=None):
        node['node_source']=this_source
    else: node['node_source'] = this_name

    # print(f"DF df_attribute_selection !!! _____ {df_attribute_selection}")
    df_attributes = df_attribute_selection[df_attribute_selection['Node name']==this_name]
    # print(f"DF ATTRIBUTES 1 _____ {df_attributes}")
    df_parameters_manual = df_attributes[df_attributes['Node Type']=='parameter']
    df_attributes = df_attributes[df_attributes['Node Type']==this_type]
    # print(f"DF ATTRIBUTES 2 _____ {df_attributes}")
    # print(f"DF ATTRIBUTES 3 _____ {df_parameters_manual}")
    df_attributes_constantV = df_attributes[df_attributes['Parameter Type']=='ConstantValue']
    df_attributes_constantL = df_attributes[df_attributes['Parameter Type']=='ConstantList']
    df_attributes_paremeters= df_attributes[df_attributes['Parameter Type'].isin(['ConstantValue','ConstantList'])==False]
        
    additional_attr={}
    
    if len(df_attributes_constantV)>0:
        
        df_attributes_constantV.reset_index(inplace=True, drop=True)
        additional_attr = parameters.create_constant_attributes(df_attributes_constantV, this_name, this_type, this_source, data)
        node.update(additional_attr) 
        
    if len(df_attributes_constantL)>0:
        
        df_attributes_constantL.reset_index(inplace=True, drop=True)
        additional_attr = parameters.create_constant_List(df_attributes_constantL, this_name, this_type, this_source, data)
#         print(f"CONFIRM CONSTANT LIST {additional_attr}")
        node.update(additional_attr) 
    
    if len(df_attributes_paremeters)>0:
        
        df_attributes_paremeters.reset_index(inplace=True, drop=True)
        additional_attr, df_attributes_paremeters = parameters.create_node_parameter_names(df_attributes_paremeters, this_name, this_type, this_source)
        node.update(additional_attr) 
        
    if len (df_parameters_manual)>0:
        df_parameters_manual.reset_index(inplace=True, drop=True)
    
    return node,df_attributes_paremeters, df_parameters_manual


def recorders_creation(df_recorders_nodes, df_recorders_parameters, df_recorders_extras, network_pywr, net_comp):
    '''
    Iterates throug all recorders (previously splitted by node, parameter and extra and create the recorder if meet requirements)
    For Aggregated recorders, it compares the 'recorders' attribute list with the created recorders and update the list
    '''
    
    network_pywr['recorders']={}

    if len(df_recorders_nodes)>0:
        # print("STARTING CREATION OF NODE RECORDERS")
        new_recorders = recorders.create_recorders(df_recorders_nodes, net_comp, network_pywr)
        network_pywr['recorders'].update(new_recorders)
        
    if len(df_recorders_parameters)>0:
        # print("STARTING CREATION OF PARAMETER RECORDERS")
        new_recorders = recorders.create_recorders(df_recorders_parameters, net_comp, network_pywr)
        network_pywr['recorders'].update(new_recorders)
        
    if len(df_recorders_extras)>0:
        # print("STARTING CREATION OF EXTRA RECORDERS")
        new_recorders = recorders.create_recorders(df_recorders_extras, net_comp, network_pywr)
        network_pywr['recorders'].update(new_recorders)    
        
        
    recorders_listing = list(network_pywr['recorders'].keys())
    agg_recorders_listing = recorders.filter_aggregated_recorders(recorders_listing, network_pywr)

    for this_recorder in agg_recorders_listing:
        network_pywr['recorders'][this_recorder]['recorders'] = list(set(recorders_listing).intersection(network_pywr['recorders'][this_recorder]['recorders']))
        
    return network_pywr


def set_locations(df_Network_Components, network_pywr):
    '''
    Assign lat long as per excel file
    If no lat long is provided, it will assign a default location calculated by the mean of all locations
    '''
    locs_df = df_Network_Components.copy()
    locs_df = locs_df[locs_df['location_lat'].notna()]
    locs_df = locs_df[locs_df['location_long'].notna()]
    default_location = [locs_df["location_lat"].mean(), locs_df["location_long"].mean()]
    
    try:
        locs_df['location_lat'] = locs_df['location_lat'].apply(lambda x: float(format(float(x), '.5e')))
        locs_df['location_long'] = locs_df['location_long'].apply(lambda x: float(format(float(x), '.5e')))
    except:
        pass

    for _nodes in network_pywr['nodes']:
        geolocation_updated=0
        
        for index, row in locs_df.iterrows():
            
            if _nodes['name']==row['name']:
                
                _nodes["position"]={}
                _nodes["position"]['geographic']=[row['location_lat'],row['location_long']]
                network_pywr.update(_nodes)
                geolocation_updated=1

        if (geolocation_updated==0):
            _nodes["position"]={}   
            _nodes["position"]['geographic']=default_location
            
    return network_pywr

def pruning(network_pywr):
    '''
    Deletes extra information needed for the creation of the network
    '''
    
    debug_JSON = {}
    debug_JSON['metadata'] = network_pywr['metadata']
    debug_JSON['timestepper'] = network_pywr['timestepper']
    
    if globals.is_scenario == True:
        debug_JSON['scenarios'] = network_pywr['scenarios']
        
    debug_JSON['nodes'] = network_pywr['nodes']
    debug_JSON['edges'] = network_pywr['edges']
    debug_JSON['parameters'] = network_pywr['parameters']
    debug_JSON['recorders'] = network_pywr['recorders']
    
    debug_JSON['nodes'] = delete_nones(debug_JSON['nodes'])
    debug_JSON=remove_constructors_info(debug_JSON)
    
    return debug_JSON

def remove_constructors_info(d):
    
    if not isinstance(d, (dict, list)):
        return d
    
    if isinstance(d, list):
        return [remove_constructors_info(v) for v in d]
    
    return {k: remove_constructors_info(v) for k, v in d.items()
            if k not in {'node_comp','node_source', 'param_type_node'}}
    
    
def export_dataframes():
    # STILL NEED TO MAKE EXPORT DATAFRAMES FOR COLUMNS ONLY REQUESTED

    for df_type_actual in globals.dataframe_types_list:

        for sheet_actual in globals.dict_dataframeparameter[df_type_actual]:
            
            print(f"EXPORTING csv dataframe {sheet_actual} with columns: {globals.dict_dataframeparameter[df_type_actual][sheet_actual]} ")
            
            if df_type_actual == 'Time Series pivoted':
                dataframe2export = globals()[str("df_")+sheet_actual]
                
            elif df_type_actual == 'Monthly':
                dataframe2export = globals()[str("df_")+sheet_actual]
                
            else:
                dataframe2export = globals()[sheet_actual]
                dataframe2export[dataframe2export['Node'].isin(globals.dict_dataframeparameter[df_type_actual][sheet_actual])]


            if df_type_actual == 'Time Series':
                column_index = globals.dict_dataframeparameter_column['Time Series'][sheet_actual]
                dataframe2export = apply_conversions.ts2unstack(dataframe2export, column_index)

            elif df_type_actual == 'Monthly':
                pass
                # print(" ")

            elif df_type_actual == 'Time Series pivoted':
                pass
                print(" ")
                
            try:
                file_name=sheet_actual+str('.csv')
                dataframe2export.to_csv(globals.export_folder_path+'/'+file_name)
            except:
                print(f"ERROR: While exporting dataframe to CSV file")

    
    
def export_json(exportJSON, pathDB):
    '''
    When the network is created, it is exported to a JSON file
    '''
    with open(os.path.join(globals.output_folder_path, json_name(pathDB)), 'w', encoding='utf-8') as f:
        json.dump(exportJSON, f, ensure_ascii=False, indent=4)