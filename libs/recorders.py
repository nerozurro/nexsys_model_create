from libs import apply_conversions

def split_df_recorders(df_recorders):
    '''
    input dataframe with all recorders included in excel file
    split the dataframe in 3 dataframes: nodes, parameters and extra
    '''
    df_recorders = df_recorders[['Recorder Name', 'Recorder Source Type', 'Recorder Source','Recorder Type','Recorder Attributes','function_special','Value']]
    df_recorders = df_recorders[df_recorders['Value'].notna()]
    df_recorders.reset_index(inplace=True, drop=True)

    df_recorders_nodes = df_recorders[df_recorders['Recorder Source Type']=='node']
    df_recorders_nodes.reset_index(inplace=True, drop=True)

    df_recorders_parameters = df_recorders[df_recorders['Recorder Source Type']=='parameter']
    df_recorders_parameters.reset_index(inplace=True, drop=True)

    df_recorders_extras = df_recorders[df_recorders['Recorder Source Type']!='node']
    df_recorders_extras = df_recorders_extras[df_recorders_extras['Recorder Source Type']!='parameter']
#     df_recorders_extras = df_recorders[df_recorders['Recorder Source Type'].isnull()] # Because now it is '_Empty'
    df_recorders_extras.reset_index(inplace=True, drop=True)
    
    
    return df_recorders_nodes, df_recorders_parameters, df_recorders_extras


def create_recorders(slice_recorders, net_comp, network_pywr):
    '''
    '''
    
    # print(f"Creating recorders")
    
    list_recorders = slice_recorders["Recorder Name"].unique()
    actual_recorder={}
    
    for recorder_name in list_recorders:
        
        allow_recorder = True
        individual_recorder = slice_recorders[slice_recorders['Recorder Name']==recorder_name]
        actual_recorder[recorder_name]={}
        actual_recorder[recorder_name]['type'] = individual_recorder["Recorder Type"].iloc[0]
        
        
        
        if individual_recorder["Recorder Source Type"].iloc[0] == 'node':
            actual_recorder[recorder_name]['node'] = individual_recorder['Recorder Source'].iloc[0]
            
            if (individual_recorder["Recorder Source"].iloc[0]) not in list(net_comp['CompName']):
                allow_recorder=False
                # print(f"{recorder_name} not created. Node {individual_recorder['Recorder Source'].iloc[0]} doesnt exist in network")
        
        elif individual_recorder["Recorder Source Type"].iloc[0] == 'parameter':
            actual_recorder[recorder_name]['parameter'] = individual_recorder['Recorder Source'].iloc[0]
            
            if (individual_recorder["Recorder Source"].iloc[0]) not in network_pywr['parameters']:
                allow_recorder=False
                # print(f"Parameter {individual_recorder['Recorder Source'].iloc[0]} doesnt exist in network")
            
        else: 
            if (individual_recorder["Recorder Source"].iloc[0]) not in list(net_comp['CompName']):
                allow_recorder=False
                # print(f"{recorder_name} not created. Node {individual_recorder['Recorder Source'].iloc[0]} doesnt exist in network")
        
        individual_recorder[individual_recorder['Value'].notna()]
        individual_recorder.reset_index(inplace=True, drop=True)

        if len(individual_recorder)>0:
            
            # print(f"CALCULATING RECORDER USING: {individual_recorder}")
            
            for index, row in individual_recorder.iterrows():

                name_attr = row['Recorder Attributes']
                data_attr = row['Value']

                try:
                    data_attr = apply_conversions.verify_value_type(data_attr)
                except:
                    pass
                    # print(f"No value transformation done for {name_attr} / {recorder_name}")

                if row.notnull()['function_special']:
                    # print(f"user wants to use function {row['function_special']} on {row['Recorder Attributes']} for recorder {row['Recorder Name']}")
                    
                    try:
                        data_attr = apply_conversions.transform_value_type(data_attr, row['function_special'])
                    except:
                        print(f"ERROR: function {row['function_special']} not recognized")


                actual_recorder[recorder_name][name_attr] = data_attr


            if 'is_objective' not in actual_recorder[recorder_name]:
                actual_recorder[recorder_name]['is_objective']=None

            if allow_recorder==False:
                actual_recorder.pop(recorder_name, None)
                # print(f"    {recorder_name}  <---- recorder removed")
            else:
                pass
                # print(f"CREATED RECORDER: {recorder_name}")
                # print(f"{recorder_name} - recorder created: {actual_recorder[recorder_name]}")
                # print("")
                
        else:
            print(f"Please fill values for {recorder_name}")
            
        
        
        
    return actual_recorder



def filter_aggregated_recorders(lst, network_pywr):
    agg_recorders_listing = []

    for this_recorder in lst:
        
        if network_pywr['recorders'][this_recorder]['type'] == 'AggregatedRecorder':
            agg_recorders_listing.append(this_recorder)
            
    return agg_recorders_listing