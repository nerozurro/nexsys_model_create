def json_name():
    
    json_file_name=pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 0,usecols = 'E', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    json_file_name=json_file_name+str('.json')
    
    return json_file_name


def json_metadata():
    metadata_title = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'E', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    metadata_description = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'E', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]

def json_scenarios():
    scenarios_name = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 0,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    try:
        if pd.isna(scenarios_name):
            scenarios_name=None
    except:
        pass
        
    try:
        scenarios_size = int(pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"])
    except:
        print(f"scenario size must be an integer")
        scenarios_size = None
    scenarios_description = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'S', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]

    if scenarios_name!=None and scenarios_size!= None:
        print(f"Model will be created using scenario name {scenarios_name} and scenario size: {scenarios_size}")
        is_scenario = True
    else:
        print(f"Model is NOT using scenarios")
        is_scenario = False

def json_timestepper():
    timestepper_start = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'L', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    timestepper_end = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'L', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    timestepper_timestep = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 0,usecols = 'L', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    try:
        timestepper_timestep = int(timestepper_timestep)
    except:
        pass

    # comp2type_df = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 4,usecols = 'N:O')
    # comp2type_df.dropna(inplace=True, how='any')
    # comp2type = {}
    # for index, row in comp2type_df.iterrows():
    #     comp2type[row['Node Component']]= row['Node Type']
    
    
    
def get_edges():
    df_Network=pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 2,usecols = 'B', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    print(f"Selected {df_Network}")
    df_Network_Components=df_Network_Components[df_Network_Components['name'].notna()]
    df_Network_Components=df_Network_Components[df_Network_Components['Type'].notna()]
    df_Network=vars()[str("df_")+df_Network]
    df_Network = df_Network[['StartNodeName', 'EndNodeName']]
    
    
    df_Network_Components['Node Source'].fillna(df_Network_Components['name'], inplace=True)
    df_Network_Components = df_Network_Components[df_Network_Components['name'].notna()]
    df_Network_Components.reset_index(inplace=True, drop=True)
    df_Network_Components.head(2)
    default_location = [df_Network_Components["location_lat"].mean(), df_Network_Components["location_long"].mean()]
    
    
    
    
    df_empty_loc_Network_Components = df_Network_Components.copy()
    df_empty_loc_Network_Components = df_empty_loc_Network_Components[df_empty_loc_Network_Components['location_lat'].isna()]
    df_empty_loc_Network_Components.reset_index(inplace=True, drop=True)

    for index, row in df_empty_loc_Network_Components.iterrows():
            nameNode = row['name']
    #         print(nameNode)
            df_filtered = df_Network.query("StartNodeName == @nameNode or EndNodeName == @nameNode")
            neighbors_list = pd.melt(df_filtered, value_vars=['StartNodeName', 'EndNodeName'], ignore_index=False).value.unique()
            df_filtered = df_Network_Components[df_Network_Components['name'].isin(neighbors_list)].copy()
            df_filtered = df_filtered[df_filtered['location_lat'].notna()]
            
            if len(df_filtered)==0:
                print(f"{nameNode} will have default locations")
                new_lat = default_location[0] * (1 + 0.003* np.random.randint(-10,10,size=1))
                new_lon = default_location[1] * (1 + 0.003* np.random.randint(-10,10,size=1))
            elif len(df_filtered)==1:
                print(f"{nameNode} has 1 neighbor with coordinates")
                new_lat = df_filtered['location_lat'].mean() * (1 + 0.0003* np.random.randint(-10,10,size=1))
                new_lon = df_filtered['location_long'].mean() * (1 + 0.0003* np.random.randint(-10,10,size=1))
            else:
                print(f"{nameNode} has more than 1 neighbor with coordinates")
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
            
    df_Network_Components = df_Network_Components[['Type','Status','name','Node Source','location_lat','location_long']]
    
    
def get_attribute_selection():
    df_attribute_selection = df_attribute_selection[df_attribute_selection['attributes']!=0]
    df_attribute_selection=df_attribute_selection[df_attribute_selection['attributes'].notna()]
    df_attribute_selection=df_attribute_selection[df_attribute_selection['Parameter Type'].notna()]
    # df_attribute_selection=df_attribute_selection[df_attribute_selection['SourceSheet1'].notna()]
    df_attribute_selection.reset_index(inplace=True, drop=True)
    df_attribute_selection.tail(10)


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
    print(output_delete_nones)