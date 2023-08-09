import os
import pandas as pd


def update_paths():
    
    script = os.getcwd()
    pathDB = r'C:\Leonardo\ManchesterUniversity\Projects\Botswana\Models\data_base\pywr_network_database_xls_V1.21.22_Botswana.xlsm'
    hydra_csv_folder_path='../data/'
    
    output_folder_path= os.path.normpath(os.path.join('..',script, "../output/"))
    export_folder_path= os.path.normpath(os.path.join('..',script, "../output/data/"))
    
    os.makedirs(output_folder_path, exist_ok=True)
    os.makedirs(export_folder_path, exist_ok=True)
    
    return pathDB, script, export_folder_path, hydra_csv_folder_path



def define_sources(pathDB):
    
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

def dataframes_setup():
    dataframe_types_list = ['Time Series', 'Time Series pivoted', 'Monthly']
    dict_dataframeparameter={}
    dict_dataframeparameter['Time Series']={}
    dict_dataframeparameter['Time Series pivoted']={}
    dict_dataframeparameter['Monthly']={}
    dict_dataframeparameter['External DataFrame']={}

    dict_dataframeparameter_column={}
    dict_dataframeparameter_column['Time Series']={}
    dict_dataframeparameter_column['Time Series pivoted']={}
    dict_dataframeparameter_column['External DataFrame']={}