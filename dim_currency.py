import pandas

def create_dim_cur(table):
    dates = table.drop([0,1,2,3,4,5])
    dates = dates.melt(id_vars=['Titre :'], var_name= 'cur_code', value_name= 'one_euro_value')
    dates = dates.rename(columns = {'Titre :': 'cur_date'})
    dates = dates[(dates.one_euro_value != '-') & (dates.one_euro_value.notnull())]
    dates['cur_date']= pandas.to_datetime(dates['cur_date'], format='%d/%m/%Y')
    dates_max = dates[['cur_code', 'cur_date']]
    dates_max = dates_max.groupby('cur_code').max()
    dates = pandas.merge(dates, dates_max, 'inner', ['cur_code', 'cur_date'])
    dates = dates.rename(columns = {'cur_date': 'last_updated_date'})
    ser_code = pandas.DataFrame(table.iloc[0:1,1:])
    ser_code = ser_code.melt(var_name= 'cur_code', value_name= 'Serial_code')
    dim_currency = pandas.merge(ser_code, dates, 'inner', ['cur_code'])
    dim_currency['cur_code'] = dim_currency['cur_code'].str.rsplit('(', 1).str[1].str.rsplit(')', 1).str[0]
    return dim_currency
  
 df = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', delimiter= ';')
currency_date = create_dim_cur(df)
