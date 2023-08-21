import os
import pandas as pd

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
    
    script = os.getcwd()
    pathDB = r'C:\Leonardo\ManchesterUniversity\general_scripts\Model_Creation\model_creaction_xls\pywr_network_database_xls_V1.21.22_Botswana.xlsm'
    globals.hydra_csv_folder_path='../data/'
    
    globals.output_folder_path= os.path.normpath(os.path.join('..',script, "../output/"))
    globals.export_folder_path= os.path.normpath(os.path.join('..',script, "../output/data/"))
    
    os.makedirs(globals.output_folder_path, exist_ok=True)
    os.makedirs(globals.export_folder_path, exist_ok=True)
    
    return pathDB, script, globals.export_folder_path, globals.hydra_csv_folder_path



def define_sources(pathDB):
    '''
    input str path and reads 'read_sheet' toknow what sheets will be used in json creation
    returns a dict with the sheet name, rows 2 skip [integer] in the xls and columns to use [A-Z]
    '''
    df_sheets_dict = pd.read_excel(pathDB, sheet_name='read_sheets',
                               skiprows = 4,
                               usecols = 'C:G')
    
    df_sheets_dict = df_sheets_dict[df_sheets_dict['SHEET NAME'].notna()]
    df_sheets_dict = df_sheets_dict[df_sheets_dict['INCLUDE']=="YES"]
    df_sheets_dict.reset_index(inplace=True, drop=True)
    df_sheets_dict['ROW HEADER'].fillna(5, inplace=True)
    df_sheets_dict['FIRST COLUMN'].fillna('A', inplace=True)
    df_sheets_dict['LAST COLUMN'].fillna('Z', inplace=True)
    
    sheets_dict = {}

    for index, row in df_sheets_dict.iterrows():
        
        sheets_dict[row['SHEET NAME']]={}
        sheets_dict[row['SHEET NAME']]['rows2skip'] = int(row['ROW HEADER'])-1
        sheets_dict[row['SHEET NAME']]['cols2use'] = row['FIRST COLUMN']+":"+row['LAST COLUMN']
  
    return sheets_dict

def UOM(pathDB):
    '''
    in the configuration sheet, user can determine what would be the target units
    Also UOM table contains all the conversion table from origin units to target units
    returns a dict with 3 keys: table, sleected and target
    '''
    
    UOM_table = pd.read_excel(pathDB,sheet_name='Configuration',skiprows = 17,usecols = 'Z:AN', nrows=18)
    UOM_table.dropna(how='all', inplace=True)
    UOM_table.reset_index(inplace=True, drop=True)

    UOM_selected = pd.read_excel(pathDB,sheet_name='Configuration',skiprows = 20,nrows=10 ,usecols = 'C:D', header=None, names=['measurement', 'unit'])
    UOM_target   = pd.read_excel(pathDB,sheet_name='Configuration',skiprows = 20,nrows=10 ,usecols = 'C,I', header=None, names=['measurement', 'unit'])
    UOM_target.dropna(how='any', inplace=True)
    UOM_target.reset_index(inplace=True, drop=True)
    
    UOMs = {}
    UOMs['UOM_table'] = UOM_table
    UOMs['UOM_selected'] = UOM_selected
    UOMs['UOM_target'] = UOM_target
    
    return UOMs

