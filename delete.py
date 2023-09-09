from libs import parameters, globals, recorders, apply_conversions, read_data
import pandas as pd


def create_extra_parameters(network_pywr, data):
    apply_conversions.monthly_days()
    
    df_extra_parameters = data['df_extra_parameters']
    df_extra_parameters_ = df_extra_parameters.copy()
    
    # Filter out rows (attributes) with no data in Value column or SourceSheet
    
    df_extra_parameters_ = df_extra_parameters_[df_extra_parameters_['Parameter Type'].notna()]
    df_extra_parameters_ = df_extra_parameters_[df_extra_parameters_['Value'].notna() | df_extra_parameters_['SourceSheet'].notna()]
    df_extra_parameters_.reset_index(inplace=True, drop=True)
    
    param_not_included = []

    # print(df_extra_parameters_)
    
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
                    source_df = data[row['SourceSheet']]  # ASSIGN DATAFRAME ACCORDING TO THE SEEHT NAME
                                
                    if row.notnull()['SourceSheet'] & row.notnull()['Column']: # If selected source annd column (Value or vertical list)
                    
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
                

                param_dict[actual_parameter][name_attr]=data_attr
            

        if actual_parameter in network_pywr['parameters']:
            # print(f"Existe el parametro {actual_parameter}")
            network_pywr['parameters'][actual_parameter].update(param_dict[actual_parameter])
        else:
            # print(f"NO Existe el parametro {actual_parameter}")
            param_not_included.append(actual_parameter)

    # print(f"parameters defined but not included in network: {param_not_included}")
    
    return network_pywr, param_not_included




pathDB = r'C:\Leonardo\ManchesterUniversity\general_scripts\excel formatting\pywr_network_database_xls_V1.33.xlsm'

data = {}
data['df_extra_parameters'] = pd.read_excel(pathDB, sheet_name='extra_parameters', 
                     skiprows=4, usecols='C:J')
data['Init_Dams'] = pd.read_excel(pathDB, sheet_name='Init_Dams', 
                     skiprows=4, usecols='B:T')

data['D_Bathymetry'] = pd.read_excel(pathDB, sheet_name='D_Bathymetry', 
                     skiprows=4, usecols='B:D')

data['Monthly_Profiles'] = pd.read_excel(pathDB, sheet_name='Monthly_Profiles', 
                     skiprows=4, usecols='B:O')

# print(df_extra_parameters)

create_extra_parameters({}, data)