import pandas as pd
import ast #connvert string to dictionary
from calendar import monthrange

def monthly_days():
    # Create dataframe with days per month. Globar variables
    
    global months, days_month, df_days_month
    
    months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    days_month = [31, 28, 31,30,31,30,31,31,30,31,30,31]
    list_of_tuples = list(zip(months, days_month))
    
    df_days_month = pd.DataFrame(list_of_tuples,columns=['months', 'days_month'])
    df_days_month=df_days_month.set_index('months')
    df_days_month=df_days_month.T
    


def convert_time_flowvalue(dataframetoconvert_flowvalue, units_from, units_to, column_name):
    # Convert time units from vol/Year or vol/Month to vol/Day
    # vol = volume. It can be ML or Mm3
    # This function is called from apply_conversions.convert_units_column_by_colum()
    # This function only applies Time conversion. It does not apply volume conversion

    if not units_from in['ML/Day','Mm3/Day']:
        
        if units_from in ['ML/Month', 'Mm3/Month']:
            dataframetoconvert_flowvalue=dataframetoconvert_flowvalue/30
            print(f"  ----> Time Transformation applied for column {column_name} from Month to Day (/30)")
        
        elif units_from in ['ML/Year', 'Mm3/Year']:
            dataframetoconvert_flowvalue=dataframetoconvert_flowvalue/365
            print(f"  ----> Time Transformation applied for column {column_name} from Year to Day (/365)")
        
        else: 
            print(f"  ----> WARNING: Time transformation for column {column_name} from '{units_from}' to '{units_to}' not applied")
            units_to=units_from
        
    return dataframetoconvert_flowvalue, units_to
            
    
    
def convert_units_column_by_colum(this_sheet, unitstoconvert, dataframetoconvert, UOMs, dict_df_units):
    # Convert units column by column. Usually for constant Parameter or Constant values
    # This function is called from apply_conversions.apply_conversions()
    # This function only applies units conversion. It does not apply time conversion
    # All conversions are applied as per UOMs table. Which is registered in the UOMs dictionary from excel->config->UOMs
    
    print(f" ----> Requested column by column conversion for '{this_sheet}'")
    
    UOM_target = UOMs['UOM_target']
    UOM_table = UOMs['UOM_table']
    
    list_units=[]
    # unitstoconvert = unitstoconvert.squeeze() TO CONFIRM IF THIS IS NEEDED

    for index, row in unitstoconvert.iterrows():
        column_name = row['column_name']
        units_from = row['units']
        units_to   = UOM_target[UOM_target['measurement'] == row['uom']]['unit'].item()
        factor = UOM_table[UOM_table['unit']== units_from][units_to].item()
        
        if factor!= 1:
            
            dataframetoconvert[column_name] = dataframetoconvert[column_name] * factor
            print(f" ----> Converted sheet '{this_sheet}' column '{column_name}' from '{units_from}' to '{units_to}' -> Factor applied: '{factor}'")
            units_from = units_to
            
        else: print(f" ----> No units conversion was applied in '{this_sheet}' column '{column_name}' from '{units_from}' to '{units_to}'")
        
        dataframetoconvert[column_name], units_to = convert_time_flowvalue(dataframetoconvert[column_name], units_from, units_to, column_name)     
        dict_df_units[f"UOM_{this_sheet}_{column_name}"]=units_to
    
    return dataframetoconvert, list_units, dict_df_units



def convert_units_monthly_table(this_sheet, unitstoconvert, dataframetoconvert, UOMs, dict_df_units):
    '''
    Returns: monthly profile (dataframe), list_units(list), dict_df_units (dictionary)
    Convert units for monthly tables. Usually applied for monthly profile parameters
    This function is called from apply_conversions.apply_conversions()
    All conversions are applied as per UOMs table. Which is registered in the UOMs dictionary from excel->config->UOMs
    Monthly tables are 12 columns (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
    This function can make time conversions if flow from Month or Year to Day
    '''
    print(f" ----> Requested monthly table conversion for '{this_sheet}'")
    
    UOM_target = UOMs['UOM_target']
    UOM_table = UOMs['UOM_table']
    
    unitstoconvert=unitstoconvert.squeeze()
    list_units=[]
    
    units_from = unitstoconvert['units']
    units_to   = UOM_target[UOM_target['measurement'] == unitstoconvert['uom']]['unit'].item()
    factor = UOM_table[UOM_table['unit']== units_from][units_to].item()

    if factor!= 1: # Unit conversion according to table in excel file UOM
        dataframetoconvert[months] = dataframetoconvert[months] * factor
        print(f" ----> Converted volume '{this_sheet}' from '{units_from}' to '{units_to}' -> Factor applied: '{factor}'")
    
    else: print(f" ----> No Volumne conversion was applied in {this_sheet} from {units_from} to {units_to}")
    
    # If needed time conversion for flow from Month or Year to Day
    if units_from in['ML/Month','Mm3/Month']:
        
        if units_to in['ML/Day','Mm3/Day']:
            dataframetoconvert = table_cnv_month2daily(dataframetoconvert)
            print(f" ----> Converted Time '{this_sheet}' from monthly total value to average per day (Total month / Days of month)")
        
        else:
            print(f" ----> No time recognized for '{this_sheet}' from '{units_from}' to '{units_to}'")
    
    else:
        print(f" ----> Not applied conversion time for '{this_sheet}' from '{units_from}' to '{units_to}'")   
        
    dict_df_units[f"UOM_{this_sheet}"]=units_to
    
    return dataframetoconvert, units_to, dict_df_units



def convert_units_timeseries(UOM_timeseries_columns, this_sheet, unitstoconvert, dataframetoconvert, UOMs, dict_df_units):
    '''
    Returns: timeseries (dataframe), units_to (str), dict_df_units (dictionary)
    Convert units for timeseries. 
    This function is called from apply_conversions.apply_conversions()
    All conversions are applied as per UOMs table. Which is registered in the UOMs dictionary from excel->config->UOMs
    By the moment, this function can resample flow and volume from month to day, and convert units
    '''
    
    print(f" ----> Requested timeseries conversion for '{this_sheet}'")
    
    UOM_target = UOMs['UOM_target']
    UOM_table = UOMs['UOM_table']
    
    unitstoconvert=unitstoconvert.squeeze()
    units_from = unitstoconvert['units']
    units_to   = UOM_target[UOM_target['measurement'] == unitstoconvert['uom']]['unit'].item()
    factor = UOM_table[UOM_table['unit']== units_from][units_to].item()

    if factor!= 1:
        dataframetoconvert[UOM_timeseries_columns] = dataframetoconvert[UOM_timeseries_columns] * factor
        print(f" ----> Converted '{this_sheet}' from '{units_from}' to '{units_to}' -> Factor applied: '{factor}'")
    
    else:
        print(f" ----> No Volume conversion was applied in '{this_sheet}' from '{units_from}' to '{units_to}'")
    
    
    # Time resampling manual. Need to update using resample function
    # This is done manually as It can be expanded to more time conversions. Still on development if needed
    if not units_from in['ML/Day','Mm3/Day']:
        
        if units_from in ['ML/Month', 'Mm3/Month', 'ML', 'Mm3']: #FLOW and volume
            
            dataframetoconvert['newdate'] = pd.to_datetime(dataframetoconvert['timestep']).dt.to_period('m')
            dataframetoconvert["days"] = [monthrange(x.year, x.month)[1] for x in dataframetoconvert['newdate']]
            dataframetoconvert[UOM_timeseries_columns] = dataframetoconvert.loc[:, UOM_timeseries_columns].div(dataframetoconvert["days"], axis=0)
            dataframetoconvert.drop(columns=['days','newdate'], inplace=True)
            print(f" ----> Converted Time '{this_sheet}' from monthly total value to average per day ('{units_from}' to '{units_to}')") 
    
    dict_df_units[f"UOM_{this_sheet}"]=units_to
    
    return dataframetoconvert, units_to, dict_df_units

   

def table_cnv_month2daily(this_df_monthly):   
    '''
    This function is called from monthlyprofile.
    get average per day from total in month
    '''  
    for thismonthdays in df_days_month:
        
        actual_month=thismonthdays
        number_days=int(df_days_month[thismonthdays][0])
        this_df_monthly[actual_month] = this_df_monthly[[actual_month]]/number_days
        
    return this_df_monthly




# source: https://stackoverflow.com/questions/55050351/combining-year-and-month-columns-to-form-index-for-time-series-data
def cnv(row):
#     print(f"row.date {row.date}")
    datStr = '01 ' + row.date + ' ' + str(int(row.Year))
    # print(row)
    # print(datStr)
#     return pd.to_datetime(datStr, format='%d %b %Y').date()
    # Change date format to YYYY-MM-DD
    return pd.to_datetime(datStr, format='%d %b %Y').date().strftime('%Y-%m-%d')



def ts2unstack(timeseries_df, column_value):
    """                                  Converts
            Node     index    values                    index         Node1    Node2    ........
            Node1  01-01-01   10           ====>      01-01-01         10        20
            Node2  01-01-01   20                        .
             .         .       .                        .
    """        
#     print(f"ts2unstack DF, {timeseries_df.head(2)}")
    timeseries_df["timestep"] = pd.to_datetime(timeseries_df["timestep"])
    timeseries_df = timeseries_df.set_index('timestep')
#     df2 = pd.pivot_table(df, index=df.index.date, columns=df.index.hour, values="Value")
    timeseries_df=pd.pivot_table(timeseries_df, values=column_value, index=timeseries_df.index,columns=['Node'])
#     timeseries_df=timeseries_df.sort_index()
#     print(f"AFTER PIVOT, {timeseries_df.head(2)}")
    return timeseries_df



def df2ts2unstack(timeseries_df): 
    """ 
    Read Dataframe Node     Year     Jan Feb Mar May ... Dec
    Return         Date     Node1  Node2  Node3....
    """
    timeseries_df=timeseries_df[timeseries_df['Node'].notna()]
    timeseries_df=timeseries_df[timeseries_df['Year'].notna()]

    timeseries_df = timeseries_df.melt(id_vars=['Node', 'Year'], var_name='date', value_name='level')
    timeseries_df['timestep'] = timeseries_df.apply(cnv, axis=1) # combine year and month columns, Add day
    timeseries_df.drop(['Year'], axis=1,inplace=True) # Drop extra column year
    timeseries_df["timestep"] = pd.to_datetime(timeseries_df["timestep"])
    timeseries_df = timeseries_df.set_index('timestep')
    timeseries_df = pd.pivot_table(timeseries_df, values='level', index=timeseries_df.index,columns=['Node'])

    return timeseries_df




def verify_value_type(constant_value):
    # Standireze lower for TRUES and FALSES
    if constant_value.lower() == 'true':
        constant_value = True
    elif constant_value.lower() == 'false':
        constant_value = False
    return constant_value



def transform_value_type(constant_value, function_special='none', lform='.6e'):
    '''
    input: constant_value (can be number, float, text, dictionary form,..), function_special, 
    lform: 6 decimals, scientific notation
    
    to_dict: if user need to input special notation. Or if parameter is too deep to be included  in excel file
    to_list: if the type of the value is a list. First transform to str, then tries float and then integer
             eg: ['param_mm', 'param_area', 0.001]
             proper inputation in xls--> param_mm; param_area; 0.001  
    '''
    if function_special == 'to_dict': # if dict
        constant_value = ast.literal_eval(constant_value)
        
    elif function_special == 'to_list': # if list
        
        try: # sometimes needed empty list. User needs to input none
            if constant_value=='none':
                constant_value=[]
            else: # if values, then split by ; and space '; ' as separator
                constant_value = [x.strip() for x in constant_value.split('; ')]
        except: 
            constant_value=[str(constant_value)]

        constant_value2=constant_value
        
        for i in range(len(constant_value)):
            
            try: # Try to convert each item to float 
                constant_value2[i] = float(str(constant_value2[i]))
                constant_value2[i] = float(format(constant_value2[i], lform))
                if constant_value2[i].is_integer(): constant_value2[i]=int(constant_value2[i])
            except:
                pass
    
        constant_value = list(constant_value2)
        
    else:
        constant_value2=constant_value
        
        try: 
            constant_value2 = float(str(constant_value2))
            constant_value2 = float(format(constant_value2, lform))
            if constant_value2.is_integer(): constant_value2=int(constant_value2)
            
        except: # ADD WARNING
            print(f"Can Not Transform value {constant_value}")
            pass
        
        constant_value = constant_value2
        
    return constant_value