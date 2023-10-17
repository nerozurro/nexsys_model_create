import pandas as pd
import sys

from libs import apply_conversions, config, globals, read_data

        
        
def createParameter(node_info, param_row, data):
    
    globals.global_tracking_variables()
    # print(f"--- STARTING PARAMETER CREATION FOR {node_info} USING {param_row} -----")
    
    parameter={}
    get_parameter_attributes={}

    if param_row['Node Type']=='parameter':
        param_row['ParameterName']=param_row['attributes']
        
    parameter[param_row['ParameterName']]={}
#     print(f"Parameter to be created using: {node_info}")
    
    if (pd.isnull(param_row['SourceSheet1']) and pd.isnull(param_row['Column1'])):
        # print(f"Need to process on additional attributes for: {param_row['ParameterName']}")
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
    elif (param_row['Parameter Type'] == 'InterpolatedVolumeParameter'): # TYPE InterpolatedVolumeParameter
        
        # print("    ........Chosen interpolated Volume parameter........")
        parameter[param_row['ParameterName']]={}
        param_func=completeInterpolatedVolumeParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)

        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        

    elif (param_row['Parameter Type'] == 'InterpolatedFlowParameter'): # TYPE InterpolatedVolumeParameter
        # print("    ........Chosen interpolated Volume parameter........")
        
        parameter[param_row['ParameterName']]={}
        param_func=completeInterpolatedFlowParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)
        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
    elif (param_row['Parameter Type'] == 'ControlCurveInterpolatedParameter'):       # TYPE DataFrameParameter from external csv
       
        
        param_func=completeControlCurveInterpolatedParameter
        get_parameter_attributes[param_row['ParameterName']], _ = param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)
        print(f"get_parameter_attributes, {get_parameter_attributes}")
        print(f"param_row['Node name'], {param_row['Node name']}")
        print(f"parameter, {parameter}")
        
        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
    elif (param_row['Parameter Type'] == 'MonthlyProfileParameter'):           # TYPE MonthlyProfile
        # print("    ........Chosen MonthlyProfileParameter........")
        
        param_func=completeMonthlyProfileParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)
        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']

    
    elif (param_row['Parameter Type'] == 'DataFrameParameter'):       # TYPE DataFrameParameter
        # print("    ........Chosen DataFrameParameter........")
        
        parameter[param_row['ParameterName']]={}
        param_func=completeDataFrameParameter
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row, data)
        parameter.update(get_parameter_attributes)
        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
        
        
    elif (param_row['Parameter Type'] == 'DataFrameParameter_External'):       # TYPE DataFrameParameter from external csv
        # print("    ........Chosen DataFrameParameter........")
        
        parameter[param_row['ParameterName']]={}
        param_func=completeDataFrameParameterExternal
        get_parameter_attributes[param_row['ParameterName']]=param_func(node_info, param_row)
        parameter.update(get_parameter_attributes)
        parameter[param_row['ParameterName']]['node_source']=param_row['Node name']
        parameter[param_row['ParameterName']]['type']='DataFrameParameter'   
             

    elif (param_row['Parameter Type'] == 'aggregated'):
        
        parameter[param_row['ParameterName']]['type']='aggregated'
        parameter[param_row['ParameterName']]['type']='AggregatedParameter'
        parameter[param_row['ParameterName']]['agg_func']=param_row['Aggregated Function']
        
        try:
            parameter[param_row['ParameterName']]['parameters'] = apply_conversions.transform_value_type(param_row['Column1'], 'to_list')
        except:
            print(f"ERROR: creating aggregated list {param_row['attributes']}")
            
            
    
        
    else:
        print(f"Creating empty slot for Parameter: {param_row['ParameterName']}")
        parameter[param_row['ParameterName']]={}
        parameter[param_row['ParameterName']]['type']=param_row['Parameter Type']
                
    return parameter


def create_extra_parameters(network_pywr, data):
    apply_conversions.monthly_days()
    # print(f"data.keys(), {data.keys()}")
    df_extra_parameters = data['df_extra_parameters']
    df_extra_parameters_ = df_extra_parameters.copy()
    
    # Filter out rows (attributes) with no data in Value column or SourceSheet
    
    df_extra_parameters_ = df_extra_parameters_[df_extra_parameters_['Parameter Type'].notna()]
    df_extra_parameters_ = df_extra_parameters_[df_extra_parameters_['Value'].notna() | df_extra_parameters_['SourceSheet'].notna()]
    df_extra_parameters_.reset_index(inplace=True, drop=True)
    
    # print(df_extra_parameters_)
    param_not_included = []

    
    extra_parameters_list = list(df_extra_parameters_['Parameter Name'].unique())
    extra_parameters_list

    for actual_parameter in extra_parameters_list: # iterates over each parameter for the current Node
        actual_parameter_df = df_extra_parameters_[df_extra_parameters_['Parameter Name']==actual_parameter]
        actual_parameter_df.reset_index(inplace=True, drop=True)

        param_dict = {}
        param_dict[actual_parameter]={}

        for index, row in actual_parameter_df.iterrows(): # iterates over attributes of each parameter

            name_attr = row['Parameter Attributes']
            
            if row.notnull()['Value']: # First Priority If 'Value' Column is filled
                data_attr = row['Value']
                data_attr = read_data.read_value(data_attr, row)     



            elif row.notnull()['SourceSheet']:  # Second Priority If 'SourceSheet' Column is filled
                
                
                try:
                    source_df = data[str('df_'+row['SourceSheet'])]  # ASSIGN DATAFRAME ACCORDING TO THE SEEHT NAME
                                
                    if (row.notnull()['SourceSheet'] & row.notnull()['Column']): # If selected source annd column (Value or vertical list)

                        try: # Filter data from source sheet and column
                            source_df = source_df[['Node', row['Column']]]
                            source_df = source_df[source_df['Node']==row['Node Source']]
                            
                            if len(source_df)==0: # Empty DF
                                print(f"ERROR: Not data found in xls Sheet {row['SourceSheet']} for column {row['Column']} aasigned to Node {row['Node Source']}")
                            
                            elif len(source_df)==1: # Value  
                                data_attr = source_df.iloc[0][row['Column']]
                                data_attr = read_data.read_value(data_attr, row) 

                            else: #vertical list
                                data_attr = source_df[row['Column']].values.tolist()
                                data_attr = [read_data.read_value(item, row) for item in data_attr]
                            
                        except:
                            print(f"ERROR:  Match between SourceSheet {row['SourceSheet']} and column {row['Column']} Wrong or not defined in extra_parameters sheet")
                        
                    else: # Only 'SourceSheet' has data, not 'Column'. It means Monthly profile

                        source_df = source_df[source_df['Node']==row['Node Source']]
                        source_df.reset_index(inplace=True)
                        source_df=source_df[apply_conversions.months].T
                        data_attr=list(source_df[0])
                        data_attr = [apply_conversions.transform_value_type(item) for item in data_attr]
                              
                except:
                    print(f"ERROR: Sheet {row['SourceSheet']} assigned in extra_parameters sheet doesnt exist in xls file")
                
            try:
                param_dict[actual_parameter][name_attr]=data_attr                
            except Exception as e: 
                print(f"ERROR: Trying to access value {data_attr} for {str(actual_parameter)}")
                print(e)
                break   
            
            

        if actual_parameter in network_pywr['parameters']:
            # print(f"Existe el parametro {actual_parameter}")
            network_pywr['parameters'][actual_parameter].update(param_dict[actual_parameter])
        else:
            # print(f"NO Existe el parametro {actual_parameter}")
            param_not_included.append(actual_parameter)
    print("")
    # print(f"parameters defined but not included in network: {param_not_included}")
    
    return network_pywr, param_not_included


   
    
    

def create_constant_List(df_attributes_constantL, this_name, this_type, this_source, data):
    globals.global_tracking_variables()
    # print(f"---- Creating constant attribute List {df_attributes_constantL['attributes']} for {this_name} --------")
    
    node_constant_attribute={}
    
    for index, row in df_attributes_constantL.iterrows():

        attribute = row['attributes']
        column_value = apply_conversions.transform_value_type(row['Column1'], 'to_list')
        # print(f"column_value{column_value}")
        # print(f"FOR attribute:{attribute}")
        
        node_constant_attribute[attribute] = column_value

    return node_constant_attribute
    
    
    
def create_constant_attributes(df_attributes_constantV, this_name, this_type, this_source, data):
    
    globals.global_tracking_variables()
    # print(f"---- Creating constant attribute {df_attributes_constantV['attributes']} for {this_name} --------")
    node_constant_attribute={}
    node_constant_attribute['comment']="units:["
    
    for index, row in df_attributes_constantV.iterrows():
        
        attribute = row['attributes']
        column_value = row['Column1']
        df_source_Value = str("df_")+row['SourceSheet1']
        df_source_Value = data[str(df_source_Value)]
        df_source_Value = df_source_Value[df_source_Value['Node']==this_source]
        # print(f"column_value{column_value}")
        # print(f"df_source_Value{df_source_Value}")
        try:
            constant_value = df_source_Value[column_value].item()
        except Exception as e: 
            print(f"ERROR: Trying to access value {column_value} for {str(this_source)}")
            print(e)
            sys.exit(1)
            
        
        try:
            constant_value = apply_conversions.verify_value_type(constant_value)
        except:
            pass
            # print(f"No value transformation done for {row['attributes']} / {this_name}")
        
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
            node_constant_attribute["comment"]= f"{prev_comment}{attribute}: {read_data.dict_df_units[itemUnits4comments]}   " 
        
        except:
            itemUnits4comments=f"UOM_{row['SourceSheet1']}_{column_value}"
            prev_comment = node_constant_attribute["comment"]
            node_constant_attribute["comment"]= f"{prev_comment}{attribute}: ---   " 
     
    node_constant_attribute['comment']=node_constant_attribute['comment']+"]"
    return node_constant_attribute


        
def create_node_parameter_names(df_attributes_paremeters, this_name, this_type, this_source):
    
    node_parameter_attribute={}
    
    for index, row in df_attributes_paremeters.iterrows():
        
        attribute = row['attributes']
        parameter_name = this_name+str(' ')+str(attribute)
        node_parameter_attribute[attribute] = parameter_name
        df_attributes_paremeters.loc[index, 'ParameterName'] = parameter_name
        
    return node_parameter_attribute, df_attributes_paremeters 



def completeInterpolatedVolumeParameter(this_node_info, this_param_row, data):
    globals.global_tracking_variables()
    # print(f"   --> Starting Interpolated Volume Parameter for {this_param_row['ParameterName']}")   
    
    try:

        df_source = str("df_")+this_param_row['SourceSheet1']
        df_source = data[str(df_source)]
        column_source1 = this_param_row['Column1']
        column_source2 = this_param_row['Column2']
        ref_name = this_param_row['Node name']
        df_source=df_source[['Node',column_source1,column_source2]]
        df_source = df_source.dropna()
        # print(f"____CREATING VolumeParam for {ref_name} _____ ")
        
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
        additional_param_attr["comment"]= f"volumes: {read_data.dict_df_units[itemUnits4comments1]}, values:  {read_data.dict_df_units[itemUnits4comments2]}" 
    except:
        pass
        # print(f"No units related for {ref_name}")
        
    additional_param_attr['node'] = this_node_info['node_source']
    additional_param_attr['interp_kwargs']={}
    additional_param_attr['interp_kwargs']['kind'] = 'linear'

    return additional_param_attr



def completeInterpolatedFlowParameter(this_node_info, this_param_row, data):
    
    globals.global_tracking_variables()
    # print(f"   --> Starting Interpolated Flow Parameter for {this_param_row['ParameterName']}")   
    
    try:
        df_source = str("df_")+this_param_row['SourceSheet1']
        df_source = data[str(df_source)]
        column_source1 = this_param_row['Column1']
        column_source2 = this_param_row['Column2']
        ref_name = this_param_row['Node name']
        df_source=df_source[['Node',column_source1,column_source2]]
        df_source = df_source.dropna()
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
        additional_param_attr["comment"]= f"flows: {read_data.dict_df_units[itemUnits4comments1]}, values:  {read_data.dict_df_units[itemUnits4comments2]}" 
    except:
        pass
        # print(f"No units related for {ref_name}")
        
    additional_param_attr['node'] = this_node_info['node_source']
    additional_param_attr['interp_kwargs']={}
    additional_param_attr['interp_kwargs']['kind'] = 'linear'

    return additional_param_attr



def completeMonthlyProfileParameter(this_node_info, this_param_row, data):
    
    globals.global_tracking_variables()
    # print(f"   --> Starting MonthlyProfile Parameter for {this_param_row['ParameterName']}")   
    
    try:
        df_source = str("df_")+this_param_row['SourceSheet1']
        df_source = data[str(df_source)]
        ref_name = this_node_info['node_source']

        # print(f"____CREATING MonthlyProfileParameter for {ref_name} _____ ")
    except:
        print(f"ERROR: MonthlyProfileParameter Can not retrieve dataframe source or column information to create {this_param_row['ParameterName']}")
    
# 
    additional_param_attr={}
    additional_param_attr['values']=None
    this_list=None
    additional_param_attr["comment"]=""
    apply_conversions.monthly_days()
    
    try:

        df_source=df_source[df_source['Node']==ref_name]
        df_source.reset_index(inplace=True)
        df_source=df_source[apply_conversions.months].T
        this_list=list(df_source[0])

        this_list = [apply_conversions.transform_value_type(item) for item in this_list]

#         print(f"Created Monthly profile for {this_node_info['node_source']}")
    except:
        print(f"ERROR when trying to read some column from xls sheet for creating parameter {this_node_info['node_source']}")
    
    # INSERT COMMENTS UNITS
    try:
        # print(this_param_row)
        keyUnits4comments=f"UOM_{this_param_row['SourceSheet1']}"
        # print(read_data.dict_df_units)
        additional_param_attr["comment"]= f"units: {read_data.dict_df_units[keyUnits4comments]}" 
    except:
        print("can not include Monthly profile units in comments")
        
#     print(f"Parameter was created using: {this_node_info}")
    additional_param_attr['values']=this_list
    return additional_param_attr



def completeDataFrameParameter(this_node_info, this_param_row, data):
    
    globals.global_tracking_variables()
    # print(f"   --> Starting DataFrameParameter for {this_param_row['ParameterName']}") 
    additional_param_attr={}
#     try:
#     print(this_param_row)

    sheet_name = this_param_row['SourceSheet1']                      
    ref_name = this_param_row['Node name']
    
    if pd.isnull(this_param_row['Column1']):
        this_param_row['Column1'] = this_node_info['node_source']
    # print(f"this_param_row['Column1'] {this_param_row['Column1']}")
    
    if this_param_row['Dataframe Type'] == 'Time Series pivoted':
        file_name = sheet_name
        
    elif this_param_row['Dataframe Type'] == 'Time Series':
        file_name = sheet_name
    
    elif this_param_row['Dataframe Type'] == 'Monthly':
        file_name = sheet_name
        
    else: file_name = sheet_name+str('_')+this_param_row['Column1']
        
    
    additional_param_attr["url"]= f"{globals.hydra_csv_folder_path}{file_name}"+str('.csv')
    
    if this_param_row.notnull()['Column1']:
        additional_param_attr["column"]= this_param_row['Column1']
    else:
        additional_param_attr["column"]= this_node_info['node_source']
        
    additional_param_attr["index_col"]= "timestep"
    additional_param_attr["parse_dates"]= True
    
    try:
        keyUnits4comments=f"UOM_{this_param_row['SourceSheet1']}"
        additional_param_attr["comment"]= f"units: {read_data.dict_df_units[keyUnits4comments]}" 
    except:
        print("can not include dataframe units")
      
    # Construct dictionary for later dataframes creation:
    df_origin_type = this_param_row['Dataframe Type']
    
    if file_name in globals.dict_dataframeparameter[df_origin_type]:
        print("")
        # print(f"          {file_name} already exist in DF Dictionary")
    
    else:
        print(f"{file_name} will be created in DF Dictionary")
        globals.dict_dataframeparameter[df_origin_type][file_name]=[]
        df_source = str("df_")+sheet_name
        print(data.keys())
        df_source = data[df_source]

        if this_param_row['Dataframe Type'] == 'Time Series pivoted':
            # print(this_param_row)
            globals()[file_name] = df_source[['timestep', this_param_row['Column1']]]
        
        elif this_param_row['Dataframe Type'] == 'Time Series':
            pass
            # print(this_param_row)
            # globals()[file_name] = df_source
            
        else:
            
            try:
                globals()[file_name] = df_source[['Node', 'timestep', this_param_row['Column1']]]
            except:
                print("There is an error about writting the units in dictionary")
    
    globals.dict_dataframeparameter[df_origin_type][file_name].append(ref_name)
    
    if df_origin_type == 'Time Series':
        globals.dict_dataframeparameter_column['Time Series'][file_name]=this_param_row['Column1']
        
    # print("finished dataframeexternal")

    return additional_param_attr



def completeDataFrameParameterExternal(this_node_info, this_param_row):
    globals.global_tracking_variables()
    
    # print(f"   --> Starting External DataFrameParameter for {this_param_row['ParameterName']}") 
    additional_param_attr={}
    
    file_name = this_param_row['SourceSheet1']               
    ref_name = this_param_row['Node name']
    
    additional_param_attr["url"]= f"{globals.hydra_csv_folder_path}{file_name}"
    
    
    
    if globals.is_scenario == True:     # ''' If build in scenarios and both columns on excel file are filled '''
                
        additional_param_attr["key"] = this_node_info['node_source'] # Comment this line if printing NaN
        
        if this_param_row.notnull()['DataFrame key']:
            additional_param_attr["key"] = this_param_row['DataFrame key']
            
        additional_param_attr["scenario"]= this_param_row['DataFrame scenario']
        
        try:
            # Temporal development, in case column attribute has been created
            del additional_param_attr["column"]
        except:
            pass

    else: # if no scenario case
        if this_param_row.notnull()['Column1']:
            additional_param_attr["column"]= this_param_row['Column1']
        else:
            additional_param_attr["column"]= this_node_info['node_source']
        
        
#     additional_param_attr["index_col"]= "Date"
    additional_param_attr["index_col"]= 0
    additional_param_attr["parse_dates"]= True
    
    

      
    # Construct dictionary for later dataframes creation:
    df_origin_type = "External DataFrame"

    # print(f"dict_dataframeparameter {globals.dict_dataframeparameter}")
    # print(f"df_origin_type {df_origin_type}")
    
    if file_name in globals.dict_dataframeparameter[df_origin_type]:
        # print("")
        # print(f"          {file_name} already exist in DF Dictionary")
        pass
        
    else:
        # print(f"{file_name} will be created in DF Dictionary")
        globals.dict_dataframeparameter[df_origin_type][file_name]=[]
#         print(f"df_source {df_source}")
    
    globals.dict_dataframeparameter[df_origin_type][file_name].append(ref_name)   
    # print("finished dataframeexternal")
    return additional_param_attr





def customAggregatedParameter(this_param_name, this_func, param_list):
    
    this_parameter_node = {}
    this_parameter_node[this_param_name]={}
    this_parameter_node[this_param_name]['type']='AggregatedParameter'
    this_parameter_node[this_param_name]['agg_func']=this_func
    this_parameter_node[this_param_name]['parameters']=param_list
    
    return this_parameter_node



def completeControlCurveInterpolatedParameter(this_node_info, this_param_row, data):
    
    globals.global_tracking_variables()
    print(f"   --> Starting Control Curve Interpolated Volume Parameter for this_param_row {this_param_row}") 
    print(f"   --> Starting Control Curve Interpolated Volume Parameter for this_node_info {this_node_info}")
    try:
        
        # print(this_param_row)
        df_source = str("df_")+this_param_row['SourceSheet1']
        df_source = data[str(df_source)]
        column_source1 = this_param_row['Column1']
        column_source2 = this_param_row['Column2']
        ref_name = this_node_info['node_source']
        df_source=df_source[['Node',column_source1,column_source2]]
        df_source = df_source.dropna()
        df_source = df_source[df_source['Node']==ref_name] 
        df_source.reset_index(inplace=True, drop=True)
        # print(f"____CREATING ControlCurveInterpolatedParameter for {ref_name} _____ ")
    
    except:
        print(f"Error: ControlCurveInterpolatedParameter Can not retrieve dataframe source or columns information to create {this_param_row['ParameterName']}")

    additional_param_attr={}
    child_param={}
    
    additional_param_attr['control_curves']=None
    additional_param_attr['storage_node']=this_node_info['node_source']
    this_list=None

    if this_param_row['ControlCurveType']=='values':
        
        df_source = df_source.sort_values(column_source1, ascending=False)
        this_list=list(df_source.loc[df_source.iloc[:,0]==this_node_info['node_source']][column_source1])
        additional_param_attr['control_curves']=this_list
        print(f"additional_param_attr['control_curves'], {additional_param_attr['control_curves']}")
        this_list=list(df_source.loc[df_source.iloc[:,0]==this_node_info['node_source']][column_source2])
        additional_param_attr['values']=this_list
        print(f"additional_param_attr['values'], {additional_param_attr['values']}")
    
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
    
    if child_param==None:
        child_param = 'Empty Parameter'
    # print("&&&&&&&&&&&&&&&&&&&&& RETRUNS additional_param_attr, child_param &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    # print(f"additional_param_attr {additional_param_attr}")
    # print(f"child_param {child_param}")
    return additional_param_attr, child_param
