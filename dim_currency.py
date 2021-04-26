import pandas

# Create the table dim_currency with all available currencies in the file
def dim_cur(table):
    cur_history = table.drop([0,1,2,3,4])
    # Calculate cur_history with the history of the currencies and their values in euro
    cur_history = cur_history.melt(id_vars=['Titre :'], var_name= 'cur_code', value_name= 'one_euro_value')
    cur_history = cur_history.rename(columns = {'Titre :': 'cur_date'})
    cur_history = cur_history[(cur_history.one_euro_value != '-') & (cur_history.one_euro_value.notnull())]
    cur_history['cur_date']= pandas.to_datetime(cur_history['cur_date'], format='%d/%m/%Y')
    # Calculate the last update date and value of each currency
    last_update = cur_history[['cur_code', 'cur_date']]
    last_update = last_update.groupby('cur_code').max()
    last_update_currency = pandas.merge(cur_history, last_update, 'inner', ['cur_code', 'cur_date'])
    last_update_currency = last_update_currency.rename(columns = {'cur_date': 'last_updated_date'})
    # Calculate ser_code with the currencies and their serial codes
    ser_code = pandas.DataFrame(table.iloc[0:1,1:])
    ser_code = ser_code.melt(var_name= 'cur_code', value_name= 'Serial_code')
    # merge ser_code and last_update_currency
    dim_currency = pandas.merge(ser_code, last_update_currency, 'inner', ['cur_code'])
    dim_currency['cur_code'] = dim_currency.Serial_code.str[6:9]
    return dim_currency
  
df = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', delimiter= ';')
 # apply the function dim_cur to the table df
dim_currency = dim_cur(df)

