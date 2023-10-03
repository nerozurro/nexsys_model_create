import os
import pandas as pd
import numpy as np

from libs import globals
    
        

def dataframes_setup():
    '''
    This function create dictionaries to track what kind of dataframes are being created.
    It is important for time series exportation or to know if time series is read externally
    '''
    
    globals.dataframe_types_list = ['Time Series', 'Time Series pivoted', 'Monthly']
    globals.dict_dataframeparameter={}
    globals.dict_dataframeparameter['Time Series']={}
    globals.dict_dataframeparameter['Time Series pivoted']={}
    globals.dict_dataframeparameter['Monthly']={}
    globals.dict_dataframeparameter['External DataFrame']={}

    globals.dict_dataframeparameter_column={}
    globals.dict_dataframeparameter_column['Time Series']={}
    globals.dict_dataframeparameter_column['Time Series pivoted']={}
    globals.dict_dataframeparameter_column['External DataFrame']={}



def update_paths():
    '''
    This function to be changed for a selectable path by the user
    '''
    
    globals.global_tracking_variables()
    dataframes_setup()
    
    '''Still to define how to read file Path'''
    script = os.getcwd()
    # pathDB = r'C:\Leonardo\ManchesterUniversity\general_scripts\excel formatting\pywr_network_database_xls_V1.33.xlsm'
    # pathDB = r'C:\Leonardo\ManchesterUniversity\Projects\Ethiopia\Development\model\database\Ethiopia_pywr_create_network_xls_V0.1.xlsm'
    pathDB = r'C:\Leonardo\ManchesterUniversity\Tutorial\Test\Botswana_pywr_create_network_xls_V0.1.xlsm'
    
    # hydra_csv_folder_path will contain the url of the parameters. local path if run locally, Hydra path if run in Hydra
    '''url path to be used in dataframes: O-2'''
    try:
        globals.hydra_csv_folder_path = pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 1,usecols = 'O', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
        print('url path to be used in dataframes:')
        
    except:
        globals.hydra_csv_folder_path='../data/'  
        print("No url path found in excel file. Using local path.")
        print("If you want to use a u8ser defined path, please populate the cell 'O3' in sheet 'Network_Components'")
    
    print(f"    '{globals.hydra_csv_folder_path}'")
    print("")
    
    # globals.hydra_csv_folder_path='../data/'  
    # globals.output_folder_path= os.path.mpath(os.path.join('..',script, "../output/"))      # this path will contain the json file created
    # globals.export_folder_path= os.path.normpnorath(os.path.join('..',script, "../output/data/")) # This path will contain data created from csv, e.g dataframes from xlsm
    
    globals.output_folder_path= os.path.normpath(os.path.join('..',pathDB, "../output/"))      # this path will contain the json file created
    globals.export_folder_path= os.path.normpath(os.path.join('..',pathDB, "../output/data/")) # This path will contain data created from csv, e.g dataframes from xlsm
    
    print('Json file will be saved in:')
    print(f"    {globals.output_folder_path}")
    print("Dataframes (if any) will be saved in:")
    print(f"    {globals.export_folder_path}")
    print("")
    
    os.makedirs(globals.output_folder_path, exist_ok=True)
    os.makedirs(globals.export_folder_path, exist_ok=True) 
    
    return pathDB, globals.export_folder_path, globals.hydra_csv_folder_path



def define_main_sources(pathDB):
    '''
    input str path and reads 'read_sheet' toknow what sheets will be used in json creation
    returns a dict with the sheet name, rows 2 skip [integer] in the xls and columns to use [A-Z]
    '''
    '''Cell C4 contains the name of the Network to use (Network_components Sheet)'''
    
    main_sheets_dict = {}
        
    main_sheets_dict['Network_Components'] = {}
    main_sheets_dict['Network_Components']['rows2skip'] = 4
    main_sheets_dict['Network_Components']['cols2use'] = "B:J"
    
    main_sheets_dict['attribute_selection'] = {}
    main_sheets_dict['attribute_selection']['rows2skip'] = 4
    main_sheets_dict['attribute_selection']['cols2use'] = "B:P"
    
    main_sheets_dict['extra_parameters'] = {}
    main_sheets_dict['extra_parameters']['rows2skip'] = 4
    main_sheets_dict['extra_parameters']['cols2use'] = "B:J"
    
    main_sheets_dict['recorders'] = {}
    main_sheets_dict['recorders']['rows2skip'] = 4
    main_sheets_dict['recorders']['cols2use'] = "B:I"
    
    
    Network=pd.read_excel(pathDB,sheet_name='Network_Components',skiprows = 3,usecols = 'C', header=None,nrows=1,names=["Value"]).iloc[0]["Value"]
    main_sheets_dict[Network] = {}
    main_sheets_dict[Network]['rows2skip'] = 4
    main_sheets_dict[Network]['cols2use'] = "E:G"
    
    return main_sheets_dict
    

def define_data_sources(pathDB,sheets_dict):
    '''
    input str path and reads 'read_sheet' to know what sheets will be used in json creation
    returns a dict with the sheet name, rows 2 skip [integer] in the xls and columns to use [A-Z]
    '''
    
    
    df_sheets_dict1 = pd.read_excel(pathDB, 
                                   sheet_name='attribute_selection',
                                   skiprows = 4,
                                   usecols = 'I:J')
    
    df_sheets_dict2 = pd.read_excel(pathDB, 
                                   sheet_name='extra_parameters',
                                   skiprows = 4,
                                   usecols = 'I')
    
    df_sheets_dict = pd.DataFrame(np.concatenate([df_sheets_dict1.SourceSheet1.values,df_sheets_dict1.SourceSheet2.values, df_sheets_dict2.SourceSheet.values]))
    df_sheets_dict.columns = ['SHEET NAME']
    
    del df_sheets_dict1, df_sheets_dict2
    
    df_sheets_dict.reset_index(inplace=True, drop=True)
    df_sheets_dict = df_sheets_dict[df_sheets_dict['SHEET NAME'].notna()]
    df_sheets_dict.drop_duplicates(subset=['SHEET NAME'], keep='first', inplace=True)
    df_sheets_dict.reset_index(inplace=True, drop=True)
     

    for index, row in df_sheets_dict.iterrows():
        
        sheets_dict[row['SHEET NAME']]={}
        sheets_dict[row['SHEET NAME']]['rows2skip'] = int(5)-1
        sheets_dict[row['SHEET NAME']]['cols2use'] = "A:Z"
  
    return sheets_dict



def UOM(pathDB):
    '''
    This is used for conversion of units of measure between the entered units and the units used in Hydra
    
    UOM_table: Z5 to AN17: contains all the conversion table from origin units to target units
    UOM_selected: to delete feature
    UOM_target: units to be used in Hydra.
    '''

    
    UOM_table = pd.read_excel(pathDB,sheet_name='Configuration',skiprows = 4,usecols = 'Z:AN', nrows=18)
    UOM_table.dropna(how='all', inplace=True)
    UOM_table.reset_index(inplace=True, drop=True)

    # UOM_selected = pd.read_excel(pathDB,sheet_name='Configuration',skiprows = 7,nrows=10 ,usecols = 'C:D', header=None, names=['measurement', 'unit'])
    UOM_target   = pd.read_excel(pathDB,sheet_name='Configuration',skiprows = 7,nrows=10 ,usecols = 'C,D', header=None, names=['measurement', 'unit'])
    UOM_target.dropna(how='any', inplace=True)
    UOM_target.reset_index(inplace=True, drop=True)
    
    UOMs = {}
    UOMs['UOM_table'] = UOM_table
    # UOMs['UOM_selected'] = UOM_selected
    UOMs['UOM_target'] = UOM_target
    
    return UOMs

