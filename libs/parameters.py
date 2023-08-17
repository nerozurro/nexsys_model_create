import pandas as pd

from libs import apply_conversions, config, g

        
        
def createParameter(node_info, param_row, data):
    g.global_tracking_variables()
    print(f"--- STARTING PARAMETER CREATION FOR {node_info} USING {param_row} -----")
    parameter={}
    get_parameter_attributes={}
#     parametrized_attribute = param_row['ParameterName'].item()
    if param_row['Node Type']=='parameter':
        param_row['ParameterName']=param_row['attributes']
        
    parameter[param_row['ParameterName']]={}
#     print(f"Parameter to be created using: {node_info}")
    
    if (pd.isnull(param_row['SourceSheet1']) and pd.isnull(param_row['Column1'])):
        print(f"Need to process on additional attributes for: {param_row['ParameterName']}")
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
    elif (param_row['Parameter Type'] == 'InterpolatedVolumeParameter'): # TYPE InterpolatedVolumeParameter
        print("    ........Chosen interpolated Volume parameter........")
        parameter[param_row['ParameterName']]={}
#         print(f"Parameter to be created using: {node_info}")
        param_func=completeInterpolatedVolumeParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)

        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        

    elif (param_row['Parameter Type'] == 'InterpolatedFlowParameter'): # TYPE InterpolatedVolumeParameter
        print("    ........Chosen interpolated Volume parameter........")
        parameter[param_row['ParameterName']]={}
#         print(f"Parameter to be created using: {node_info}")
        param_func=completeInterpolatedFlowParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)

        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
    elif (param_row['Parameter Type'] == 'MonthlyProfileParameter'):           # TYPE MonthlyProfile
        print("    ........Chosen MonthlyProfileParameter........")
#         parameter[node_info[parametrized_attribute]]={}
#         print(f"Parameter to be created using: {node_info}")
        param_func=completeMonthlyProfileParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)

        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']

    
    elif (param_row['Parameter Type'] == 'DataFrameParameter'):       # TYPE DataFrameParameter
        print("    ........Chosen DataFrameParameter........")
#         print("..............ENTRA CREAR DATAFRAME................")
        parameter[param_row['ParameterName']]={}
#         print(f"Parameter to be created using: {node_info}")
        param_func=completeDataFrameParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)

        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
    elif (param_row['Parameter Type'] == 'DataFrameParameter External'):       # TYPE DataFrameParameter from external csv
        print("    ........Chosen DataFrameParameter........")
#         print("..............ENTRA CREAR DATAFRAME................")
        parameter[param_row['ParameterName']]={}
#         print(f"Parameter to be created using: {node_info}")
        param_func=completeDataFrameParameterExternal
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row)
        parameter.update(get_parameter_attributes)
        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']='DataFrameParameter'
        
        
        # THIS ControlCurveInterpolatedParameter works if filled attributes sheet
        
#     elif (param_row['Parameter Type'] == 'ControlCurveInterpolatedParameter'):
# #         print("..............Start creating ControlCurveInterpolatedParameter................")
#         print("    ........Chosen DataFrameParameter........")
#         parameter[param_row['ParameterName']]={}
# #         parameter[node_info[parametrized_attribute]]={}
# #         print(f"Parameter to be created using: {node_info}")
#         param_func=completeControlCurveInterpolatedParameter
#         main_param, child_param = param_func(node_info, param_row)
# #         print(f"--------MAIN PARAMETER--------- {main_param}")
#         get_parameter_attributes[param_row['ParameterName']]=main_param
# #         get_parameter_attributes = child_param
#         get_parameter_attributes[param_row['ParameterName']]['node_source']=node_info['node_source']
#         get_parameter_attributes[param_row['ParameterName']]['type']=param_row['Parameter Type']
#         parameter.update(get_parameter_attributes)
#         if child_param != 'Empty Parameter':
#             parameter.update(child_param)
        
             

    elif (param_row['Parameter Type'] == 'Aggregated'):
        
        parameter[param_row['ParameterName']]['type']='aggregated'
        parameter[param_row['ParameterName']]['type']='AggregatedParameter'
        parameter[param_row['ParameterName']]['agg_func']=param_row['Aggregated Function']
        
        try:
            parameter[param_row['ParameterName']]['parameters'] = apply_conversions.transform_value_type(param_row['Column1'], 'to_list')
#             data_attr = transform_value_type(data_attr, row['function_special'])
#                     if row['function_special']=='to_dict':
#                         data_attr = ast.literal_eval(data_attr)
#                     if row['function_special']=='to_list':
#                         data_attr = [x.strip() for x in data_attr.split(', ')]
#                         try:
#                             data_attr = list(map(float, data_attr))
#                         except:
#                             pass
        except:
            print(f"ERROR: creating aggregated list {param_row['attributes']}")

#         try:
#             parameter_list_aggregated = get_parameter_list(param_row)
#         except:
#             parameter_list_aggregated='ERROR'
        
#         parameter[param_row['ParameterName']]['parameters']=parameter_list_aggregated
        
    else:
        print(f"Creating empty slot for Parameter: {param_row['ParameterName']}")
        parameter[param_row['ParameterName']]={}
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
        
    
#     else: print("!!!!!!!!!!!!! Parameter Type not recognized !!!!!!!!!!!!!")
        
    return parameter



def create_constant_List(df_attributes_constantL, this_name, this_type, this_source, data):
    g.global_tracking_variables()
    print(f"---- Creating constant attribute List {df_attributes_constantL['attributes']} for {this_name} --------")
    
    node_constant_attribute={}
#     node_constant_attribute['comment']="units:["
    
    for index, row in df_attributes_constantL.iterrows():
#         print(f"ROW TO USE IN CONSTANT LIST{row}")
        attribute = row['attributes']
#         column_value = row['Column1']
        column_value = apply_conversions.transform_value_type(row['Column1'], 'to_list')
#         df_source_Value = str("df_")+row['SourceSheet1']
#         df_source_Value = globals()[df_source_Value]
#         df_source_Value = df_source_Value[df_source_Value['Node']==this_source]
        print(f"column_value{column_value}")
        print(f"FOR attribute:{attribute}")
#         print(f"df_source_Value{df_source_Value}")
#         constant_value = df_source_Value[column_value].item()
#         try:
#             constant_value = verify_value_type(constant_value)
#         except:
#             print(f"No value transformation done for {row['attributes']} / {this_name}")
        
        node_constant_attribute[attribute] = column_value

    return node_constant_attribute
    
    
    
def create_constant_attributes(df_attributes_constantV, this_name, this_type, this_source, data):
    g.global_tracking_variables()
    print(f"---- Creating constant attribute {df_attributes_constantV['attributes']} for {this_name} --------")
    node_constant_attribute={}
    node_constant_attribute['comment']="units:["
    
    for index, row in df_attributes_constantV.iterrows():
        attribute = row['attributes']
        column_value = row['Column1']
        df_source_Value = str("df_")+row['SourceSheet1']
        df_source_Value = data[str(df_source_Value)]
        df_source_Value = df_source_Value[df_source_Value['Node']==this_source]
        print(f"column_value{column_value}")
        print(f"df_source_Value{df_source_Value}")
        constant_value = df_source_Value[column_value].item()
        
        
        try:
            constant_value = apply_conversions.verify_value_type(constant_value)
        except:
            print(f"No value transformation done for {row['attributes']} / {this_name}")
        
        node_constant_attribute[attribute] = constant_value
        
        try: # Try to convert each item to float 3 decimals
            node_constant_attribute[attribute] = float(node_constant_attribute[attribute])
            node_constant_attribute[attribute] = float(format(node_constant_attribute[attribute], '.6e'))
            if node_constant_attribute[attribute].is_integer(): 
                node_constant_attribute[attribute]=int(node_constant_attribute[attribute])
        except:
            pass
        
        try:
            itemUnits4comments=f"UOM_{row['SourceSheet1']}_{column_value}"
            prev_comment = node_constant_attribute["comment"]
            print(itemUnits4comments)
            node_constant_attribute["comment"]= f"{prev_comment}{attribute}: {g.dict_df_units[itemUnits4comments]}   " 
        except:
            itemUnits4comments=f"UOM_{row['SourceSheet1']}_{column_value}"
            prev_comment = node_constant_attribute["comment"]
            node_constant_attribute["comment"]= f"{prev_comment}{attribute}: ---   " 
#             print(f"{attribute} has no units related")
     
    node_constant_attribute['comment']=node_constant_attribute['comment']+"]"
    return node_constant_attribute


        
def create_node_parameter_names(df_attributes_paremeters, this_name, this_type, this_source):
    
    node_parameter_attribute={}
    
    for index, row in df_attributes_paremeters.iterrows():
        
        attribute = row['attributes']
        parameter_name = this_name+str(' ')+str(attribute)
        node_parameter_attribute[attribute] = parameter_name
        print(f"index: {index}")
        df_attributes_paremeters.loc[index, 'ParameterName'] = parameter_name
        
    return node_parameter_attribute, df_attributes_paremeters 



def completeInterpolatedVolumeParameter(this_node_info, this_param_row, data):
    g.global_tracking_variables()
    print(f"Starting Interpolated Volume Parameter for {this_param_row['ParameterName']}")   
    try:
#         print(this_param_row)
        print(f"CHECK 01")
        df_source = str("df_")+this_param_row['SourceSheet1']
        print(f"CHECK 02")
        df_source = data[str(df_source)]
        print(f"CHECK 03")
        column_source1 = this_param_row['Column1']
        print(f"CHECK 04")
        column_source2 = this_param_row['Column2']
        print(f"CHECK 05")
        ref_name = this_param_row['Node name']
        print(f"CHECK 06")
        print(ref_name)
        print(column_source1)
        print(column_source2)
        print("")
        print(f"df_source {df_source}")
        df_source=df_source[['Node',column_source1,column_source2]]
        print(f"CHECK 07")
        df_source = df_source.dropna()
        print(f"CHECK 08")
        print(f"____CREATING VolumeParam for {ref_name} _____ ")
    except:
        print(f"Error: InterpolatedVolumeParameter Can not retrieve dataframe source or columns information to create {this_param_row['ParameterName']}")
    
    
    additional_param_attr={}
    additional_param_attr['volumes']=None
    additional_param_attr['values']=None
    this_list=None
    additional_param_attr["comment"]=""
        
    try:
        this_list=list(df_source.loc[df_source.iloc[:,0]==ref_name][column_source1])
        additional_param_attr['volumes']=this_list
        this_list=list(df_source.loc[df_source.iloc[:,0]==ref_name][column_source2])
        additional_param_attr['values']=this_list
    except:
        print(f"ERROR: Can not retrieve list information for creating parameter {this_param_row['ParameterName']}, on columns {column_source1} and {column_source2}")
    
    # INSERT COMMENTS UNITS
    try:
        itemUnits4comments1=f"UOM_{this_param_row['SourceSheet1']}_{column_source1}"
        itemUnits4comments2=f"UOM_{this_param_row['SourceSheet1']}_{column_source2}"
        additional_param_attr["comment"]= f"volumes: {g.dict_df_units[itemUnits4comments1]}, values:  {g.dict_df_units[itemUnits4comments2]}" 
    except:
        print(f"No units related for {ref_name}")
        
    additional_param_attr['node'] = this_node_info['node_source']
    additional_param_attr['interp_kwargs']={}
    additional_param_attr['interp_kwargs']['kind'] = 'linear'

    return additional_param_attr



def completeInterpolatedFlowParameter(this_node_info, this_param_row, data):
    g.global_tracking_variables()
    print(f"Starting Interpolated Flow Parameter for {this_param_row['ParameterName']}")   
    try:
#         print(this_param_row)
        print(f"CHECK 01")
        df_source = str("df_")+this_param_row['SourceSheet1']
        print(f"CHECK 02")
        df_source = data[str(df_source)]
        print(f"CHECK 03")
        column_source1 = this_param_row['Column1']
        print(f"CHECK 04")
        column_source2 = this_param_row['Column2']
        print(f"CHECK 05")
        ref_name = this_param_row['Node name']
        print(f"CHECK 06")
        df_source=df_source[['Node',column_source1,column_source2]]
        print(f"CHECK 07")
        df_source = df_source.dropna()
        print(f"CHECK 08")
        print(f"____CREATING FlowParam for {ref_name} _____ ")
    except:
        print(f"Error: InterpolatedFlowParameter Can not retrieve dataframe source or columns information to create {this_param_row['ParameterName']}")
    
    
    additional_param_attr={}
    additional_param_attr['flows']=None
    additional_param_attr['values']=None
    this_list=None
    additional_param_attr["comment"]=""
        
    try:
        this_list=list(df_source.loc[df_source.iloc[:,0]==ref_name][column_source1])
        additional_param_attr['flows']=this_list
        this_list=list(df_source.loc[df_source.iloc[:,0]==ref_name][column_source2])
        additional_param_attr['values']=this_list
    except:
        print(f"ERROR: Can not retrieve list information for creating parameter {this_param_row['ParameterName']}, on columns {column_source1} and {column_source2}")
    
    # INSERT COMMENTS UNITS
    try:
        itemUnits4comments1=f"UOM_{this_param_row['SourceSheet1']}_{column_source1}"
        itemUnits4comments2=f"UOM_{this_param_row['SourceSheet1']}_{column_source2}"
        additional_param_attr["comment"]= f"flows: {g.dict_df_units[itemUnits4comments1]}, values:  {g.dict_df_units[itemUnits4comments2]}" 
    except:
        print(f"No units related for {ref_name}")
        
    additional_param_attr['node'] = this_node_info['node_source']
    additional_param_attr['interp_kwargs']={}
    additional_param_attr['interp_kwargs']['kind'] = 'linear'

    return additional_param_attr



def completeMonthlyProfileParameter(this_node_info, this_param_row, data):
    g.global_tracking_variables()
    print(f"Starting MonthlyProfile Parameter for {this_param_row['ParameterName']}")   
    try:
#         print(this_param_row)
        df_source = str("df_")+this_param_row['SourceSheet1']
        df_source = data[str(df_source)]
#         column_source1 = this_param_row['Column1']
#         column_source2 = this_param_row['Column2']
        print(f"this_param_row{this_param_row}")
        
        ref_name = this_node_info['node_source']
        print(f"ref_name {ref_name}")
        print(f"df_source {df_source.head(2)}")
#         df_source=df_source[['Node',column_source1]]
#         df_source = df_source.dropna()
        print(f"____CREATING MonthlyProfileParameter for {ref_name} _____ ")
    except:
        print(f"ERROR: MonthlyProfileParameter Can not retrieve dataframe source or column information to create {this_param_row['ParameterName']}")
    
# 
    additional_param_attr={}
    additional_param_attr['values']=None
    this_list=None
    additional_param_attr["comment"]=""
    
    try:
        print(f"full dataframe df_source {df_source}")
        print(f"")
        print(f"ref_name to look for {ref_name}")
        print(f"")
        df_source=df_source[df_source['Node']==ref_name]
        print(f"df_source[df_source['Node']==ref_name] {df_source}")
        print(f"")
        df_source.reset_index(inplace=True)
        print(f"reset index {df_source}")
        print(f"")
        df_source=df_source[g.months].T
        print(f"df_source[months].T {df_source}")
        print(f"")
        print(f"df_source trasposed {df_source.head(2)}")
        print(f"")
        this_list=list(df_source[0])
#         this_list = [item.upper() for item in mylis]
#         this_list = [item.transform_value_type() for item in this_list]
#         this_list = list(map(transform_value_type(), this_list))
        this_list = [apply_conversions.transform_value_type(item) for item in this_list]

#         print(f"Created Monthly profile for {this_node_info['node_source']}")
    except:
        print(f"ERROR when trying to read 1 column from xls sheet: {columna[0]} for creating parameter {this_node_info['node_source']}")
    
    # INSERT COMMENTS UNITS
    try:
        keyUnits4comments=f"UOM_{this_param_row['SourceSheet1']}"
        additional_param_attr["comment"]= f"units: {g.dict_df_units[keyUnits4comments]}" 
    except:
        print("can not include Monthly profile units in comments")
        
#     print(f"Parameter was created using: {this_node_info}")
    additional_param_attr['values']=this_list
    return additional_param_attr



def completeDataFrameParameter(this_node_info, this_param_row, data):
    g.global_tracking_variables()
    print(f"Starting DataFrameParameter for {this_param_row['ParameterName']}") 
    additional_param_attr={}
#     try:
#     print(this_param_row)

    sheet_name = this_param_row['SourceSheet1']                      
    ref_name = this_param_row['Node name']
    
    
    if pd.isnull(this_param_row['Column1']):
        this_param_row['Column1'] = this_node_info['node_source']
    print(f"this_param_row['Column1'] {this_param_row['Column1']}")
    
    if this_param_row['Dataframe Type'] == 'Time Series pivoted':
        file_name = sheet_name
    
    elif this_param_row['Dataframe Type'] == 'Monthly':
        file_name = sheet_name
    else: file_name = sheet_name+str('_')+this_param_row['Column1']
        
#     print(this_param_row['UOM Units Column 1'])
    #units = str("UOM_"+this_param_row['UOM Units Column 1'].lower())

    # please variable import hydra_csv_folder_path that is already declared as global in g.py and called in main.py using the fucntion g.global_tracking_variables()
    
    
    additional_param_attr["url"]= f"{g.hydra_csv_folder_path}{file_name}"+str('.csv')
    if this_param_row.notnull()['Column1']:
        additional_param_attr["column"]= this_param_row['Column1']
    else:
        additional_param_attr["column"]= this_node_info['node_source']
        
    additional_param_attr["index_col"]= "timestep"
    additional_param_attr["parse_dates"]= True
#     additional_param_attr["dayfirst"]= True
    
    try:
        keyUnits4comments=f"UOM_{this_param_row['SourceSheet1']}"
        additional_param_attr["comment"]= f"units: {g.dict_df_units[keyUnits4comments]}" 
    except:
        print("can not include dataframe units")
      
    # Construct dictionary for later dataframes creation:
    df_origin_type = this_param_row['Dataframe Type']
    
    print(f"sheet_name {sheet_name}")
    print(f"dict_dataframeparameter {g.dict_dataframeparameter}")
    print(f"df_origin_type {df_origin_type}")
    
    if file_name in g.dict_dataframeparameter[df_origin_type]:
        print(f"{file_name} already exist in DF Dictionary")
    else:
        print(f"{file_name} will be created in DF Dictionary")
        g.dict_dataframeparameter[df_origin_type][file_name]=[]
        df_source = str("df_")+sheet_name
        df_source = data[str(df_source)]
#         print(f"df_source {df_source}")
        if this_param_row['Dataframe Type'] == 'Time Series pivoted':
            print(this_param_row)
            globals()[file_name] = df_source[['timestep', this_param_row['Column1']]]
        else:
            try:
                globals()[file_name] = df_source[['Node', 'timestep', this_param_row['Column1']]]
            except:
                print("There is an error about writting the units in dictionary")
    
    g.dict_dataframeparameter[df_origin_type][file_name].append(ref_name)
    
    if df_origin_type == 'Time Series':
        g.dict_dataframeparameter_column['Time Series'][file_name]=this_param_row['Column1']
        
        
    print("finished dataframeexternal")
        
#     except:
#         print(f"ERROR: Can not retrieve information for creating parameter {this_param_row['ParameterName']}")
    
    return additional_param_attr



def completeDataFrameParameterExternal(this_node_info, this_param_row):
    g.global_tracking_variables()
    
    print(f"Starting DataFrameParameter for {this_param_row['ParameterName']}") 
    additional_param_attr={}
    
    file_name = this_param_row['SourceSheet1']               
    ref_name = this_param_row['Node name']
    
    additional_param_attr["url"]= f"{g.hydra_csv_folder_path}{file_name}"
    
    
    
    if g.is_scenario == True:
        additional_param_attr["scenario"]= g.scenarios_name
    else:
        if this_param_row.notnull()['Column1']:
            additional_param_attr["column"]= this_param_row['Column1']
        else:
            additional_param_attr["column"]= this_node_info['node_source']
        
        
#     additional_param_attr["index_col"]= "Date"
    additional_param_attr["index_col"]= 0
    additional_param_attr["parse_dates"]= True
    
    if this_param_row.notnull()['DataFrame scenario']:  
        '''
        If build in scenarios and both columns on excel file are filled
        '''
        additional_param_attr["key"] = this_param_row['DataFrame key']
        additional_param_attr["key"] = this_node_info['node_source'] # Comment this line if printing NaN
        additional_param_attr["scenario"]= this_param_row['DataFrame scenario']
        
        try:
            # Temporal development, in case column attribute has been created
            del additional_param_attr["column"]
        except:
            pass

      
    # Construct dictionary for later dataframes creation:
    df_origin_type = "External DataFrame"
    
#     print(f"sheet_name {sheet_name}")
    print(f"dict_dataframeparameter {g.dict_dataframeparameter}")
    print(f"df_origin_type {df_origin_type}")
    
    if file_name in g.dict_dataframeparameter[df_origin_type]:
        print(f"{file_name} already exist in DF Dictionary")
    else:
        print(f"{file_name} will be created in DF Dictionary")
        g.dict_dataframeparameter[df_origin_type][file_name]=[]
#         print(f"df_source {df_source}")
    
    g.dict_dataframeparameter[df_origin_type][file_name].append(ref_name)   
    print("finished dataframeexternal")
    return additional_param_attr



def customAggregatedParameter(this_param_name, this_func, param_list):
    this_parameter_node = {}
    this_parameter_node[this_param_name]={}
    this_parameter_node[this_param_name]['type']='AggregatedParameter'
    this_parameter_node[this_param_name]['agg_func']=this_func
    this_parameter_node[this_param_name]['parameters']=param_list
    return this_parameter_node



def completeControlCurveInterpolatedParameter(this_node_info, this_param_row, data):
    g.global_tracking_variables()
    print(f"Starting Control Curve Interpolated Volume Parameter for {this_param_row['ParameterName']}") 
    try:
        print(this_param_row)
#         print(f"CHECKPOINT 1")
        df_source = str("df_")+this_param_row['SourceSheet1']
#         print(f"CHECKPOINT 2")
        df_source = data[str(df_source)]
#         print(f"CHECKPOINT 3")
        column_source1 = this_param_row['Column1']
#         print(f"CHECKPOINT 4")
        column_source2 = this_param_row['Column2']
#         print(f"CHECKPOINT 5")
        ref_name = this_node_info['node_source']
#         print(f"CHECKPOINT 6")
        df_source=df_source[['Node',column_source1,column_source2]]
#         print(f"CHECKPOINT 7")
        df_source = df_source.dropna()
        df_source = df_source[df_source['Node']==ref_name] 
        df_source.reset_index(inplace=True, drop=True)
        print(f"____CREATING ControlCurveInterpolatedParameter for {ref_name} _____ ")
    except:
        print(f"Error: ControlCurveInterpolatedParameter Can not retrieve dataframe source or columns information to create {this_param_row['ParameterName']}")

    additional_param_attr={}
    child_param={}
    
    additional_param_attr['control_curves']=None
    additional_param_attr['storage_node']=this_node_info['node_source']
    this_list=None

#     try:
#     print(f"IMPRIMIENDO df_source {df_source}")

#         temporal_dataframe=temporal_dataframe[temporal_dataframe.iloc[:,0]==this_node_info['node_source']]
#         temporal_dataframe[columna[0]]=temporal_dataframe[columna[0]].round(decimals = 2)
#         temporal_dataframe[columna[1]]=temporal_dataframe[columna[1]].round(decimals = 2)

    if this_param_row['ControlCurveType']=='values':
        df_source = df_source.sort_values(column_source1, ascending=False)
        this_list=list(df_source.loc[df_source.iloc[:,0]==this_node_info['node_source']][column_source1])
        additional_param_attr['control_curves']=this_list
        this_list=list(df_source.loc[df_source.iloc[:,0]==this_node_info['node_source']][column_source2])
        additional_param_attr['values']=this_list
    
    elif this_param_row['ControlCurveType']=='parameters': # This IF removes 0 and 1 from Starting Storage
        additional_param_attr['parameters']=[]
        df_source = df_source[df_source[column_source1] != 0]
        df_source = df_source[df_source[column_source1] != 1]
        new_row0 = {'Node':this_node_info['node_source'],column_source1:0, column_source2:0}
        new_row1 = {'Node':this_node_info['node_source'],column_source1:1, column_source2:1}
        df_source = df_source.append(new_row0, ignore_index=True)
        df_source = df_source.append(new_row1, ignore_index=True)
        

        df_source = df_source.sort_values(column_source1, ascending=False)   
        this_list=list(df_source.loc[df_source.iloc[:,0]==this_node_info['node_source']][column_source1])
        additional_param_attr['control_curves']=this_list  #Created List with control curves Starting Storage
        this_list=list(df_source.loc[df_source.iloc[:,0]==this_node_info['node_source']][column_source2])


        for this_control_curve in additional_param_attr['control_curves']:
            name_control_curve = this_control_curve*100
            name_control_curve = str(f"{this_node_info['name']} {int(name_control_curve)}")
            additional_param_attr['parameters'].append(name_control_curve) 

        control_curve_list_pc = list(additional_param_attr['parameters'])
        controls_df = pd.DataFrame(list(zip(control_curve_list_pc, this_list)), columns=['control_curve_list_pc','values'])
        df_consumo_actual = df_consumo[df_consumo['Node']==this_node_info['node_source']]
        if (len(df_consumo_actual)>1): # If 1 dam provides to multiple demand centers
            name_parameter = str(this_node_info['node_source'] + ' demand sum')
            DemandC_parameters_list = list(df_consumo[df_consumo['Node']==this_node_info['node_source']]['Partition'])
            child_param.update(customAggregatedParameter(name_parameter, 'sum', DemandC_parameters_list)) #Add sum parameter for multiple demands on 1 dam             
        if (len(df_consumo_actual)==1): # If 1 dam provides to Only 1 demand centre
            name_parameter = str(f"{this_node_info['node_source']} {this_param_row['attributes']}")

        # for cycle to create all control release parameters, e.g. Booka Dam control Release 100
        for index, row in controls_df.iterrows():
            child_param.update(customAggregatedParameter(row['control_curve_list_pc'], 'product', [row['values'],name_parameter])) #Add sum parameter for multiple demands on 1 dam             

    # Drop first and last item from control realease 0% and 100%    
    additional_param_attr['control_curves'] = additional_param_attr['control_curves'][1:-1]
    
#     print(f"additional_param_attr['control_curves'] {additional_param_attr['control_curves']}")

    

#     print(f"===========CONTROL.CURVES FINISHED. RETURNING ==========")
#     print(f"additional_param_attr{additional_param_attr} ")
#     print(f"this_node_info{this_node_info}")
#         print(f"parametrized_attribute {parametrized_attribute}")   

#     except:

#         print(f"ERROR when trying to read 2 columns from xls sheet: {columna[0]} and {columna[1]}  for creating parameter {this_node_info[parametrized_attribute]}")

    if child_param==None:
        child_param = 'Empty Parameter'
    print("&&&&&&&&&&&&&&&&&&&&& RETRUNS additional_param_attr, child_param &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    print(f"additional_param_attr {additional_param_attr}")
    print(f"child_param {child_param}")
    return additional_param_attr, child_param
