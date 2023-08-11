import pandas as pd
from libs import apply_conversions, config

def read_data(pathDB, sheets_dict):    
    
    '''
    inputs xls pth and sheets to read
    returns a dict [data] that contains all the dataframes or excel sheets with unit conversion done
    '''
    
    apply_conversions.monthly_days()
    list_dataframes = [] # List to track all dataframes created
    list_units=[]
    dict_df_units = {}
    exportdataframes_list=[]
    data = {}
    
    for this_sheet in sheets_dict:
        print(f"")
        print(f"Reading sheet from excel file: '{this_sheet}'")
        vars()[str("df_")+this_sheet] = pd.read_excel(pathDB, 
                                                sheet_name=this_sheet,
                                                skiprows = sheets_dict[this_sheet]['rows2skip'],
                                                usecols = sheets_dict[this_sheet]['cols2use'])
        # Delete rows which contain ALL NaN values    

        
        vars()[str("df_")+this_sheet].dropna(how='all', inplace=True) #Remove rows with all nan values

        vars()[str("df_")+this_sheet].reset_index(inplace=True, drop=True)

        list_dataframes.append(str("df_")+this_sheet) # append name new dataframe created

        # print(f"READ {this_sheet}")

        # PASS UNITS TO TARGET UNIT FOR EACH DATAFRAME
        '''
        The next step is reading a specific part of the xls.
        THe xls file after CA column has an indicator according to the type of sheet:
                1. Each column has its own units
                2. monthly: 12 months arranged in column. 1 type of unit for all sheets
                3. timeseries: each column is node
                4. timeseries by column
        '''

        # print(f"Converting units if needed in {this_sheet}")
        try:
            UOM_column_by_column = pd.read_excel(pathDB, 
                                             sheet_name=this_sheet,
                                             skiprows = 4,
                                             usecols = 'CA:CC',
                                             names=['column_name', 'uom', 'units'])
            
            UOM_column_by_column.dropna(how='any', inplace=True)
            UOM_column_by_column.reset_index(inplace=True, drop=True)
            
        except:
            UOM_column_by_column = pd.DataFrame()
        
        
        try:
            
            UOM_monthly = pd.read_excel(pathDB, sheet_name=this_sheet,
                                    skiprows = 4,
                                    usecols = 'CE,CF',
                                    names=['uom', 'units'])
            UOM_monthly.dropna(how='any', inplace=True)
            UOM_monthly.reset_index(inplace=True, drop=True)
            
        except:
            UOM_monthly = pd.DataFrame()


        try:
            
            UOM_timeseries = pd.read_excel(pathDB, sheet_name=this_sheet,
                                        skiprows = 4,usecols = 'CH:CJ', 
                                        names=['uom', 'units','column_name'])
            UOM_timeseries_columns=UOM_timeseries[['column_name']]
            UOM_timeseries_columns.dropna(how='any', inplace=True)
            UOM_timeseries.drop(columns=['column_name'], inplace=True)
            UOM_timeseries_columns=UOM_timeseries_columns['column_name'].values.tolist()
            UOM_timeseries.dropna(how='any', inplace=True)
            UOM_timeseries.reset_index(inplace=True, drop=True)
            
        except:
            UOM_timeseries = pd.DataFrame()

        
        try:
        
            UOM_timeseries_byColumn = pd.read_excel(pathDB, sheet_name=this_sheet,
                                        skiprows = 4,usecols = 'CL:CN', 
                                        names=['uom', 'units','column_name'])
            UOM_timeseries_byColumn_columns=UOM_timeseries_byColumn[['column_name']]
            UOM_timeseries_byColumn_columns.dropna(how='any', inplace=True)
            UOM_timeseries_byColumn.drop(columns=['column_name'], inplace=True)
            UOM_timeseries_byColumn_columns=UOM_timeseries_byColumn_columns['column_name'].values.tolist()
            UOM_timeseries_byColumn.dropna(how='any', inplace=True)
            UOM_timeseries_byColumn.reset_index(inplace=True, drop=True)
            
        except:
            UOM_timeseries_byColumn = pd.DataFrame()
        
               
        
        
        
        UOMs = config.UOM(pathDB)
              
        if not UOM_column_by_column.empty:
            vars()[str("df_")+this_sheet], list_units, dict_df_units  = apply_conversions.convert_units_column_by_colum(this_sheet, UOM_column_by_column, vars()[str("df_")+this_sheet], UOMs, dict_df_units)  
        
        elif not UOM_monthly.empty:
            vars()[str("df_")+this_sheet], list_units, dict_df_units = apply_conversions.convert_units_monthly_table(this_sheet, UOM_monthly, vars()[str("df_")+this_sheet], UOMs, dict_df_units)
        
        elif not UOM_timeseries.empty:
            vars()[str("df_")+this_sheet], list_units, dict_df_units = apply_conversions.convert_units_timeseries(UOM_timeseries_columns, this_sheet, UOM_timeseries, vars()[str("df_")+this_sheet], UOMs, dict_df_units)
        
        elif not UOM_timeseries_byColumn.empty:
            vars()[str("df_")+this_sheet], list_units, dict_df_units = apply_conversions.convert_units_timeseries(UOM_timeseries_byColumn_columns, this_sheet, UOM_timeseries_byColumn, vars()[str("df_")+this_sheet], UOMs, dict_df_units)
        
        else:
            print(f"No units conversion has been requested for {this_sheet}")
            
        data[str("df_")+this_sheet] = vars()[str("df_")+this_sheet]
        
    return data, dict_df_units