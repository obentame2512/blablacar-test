import pandas

def exchange_rate_history(table):
    dates = df.drop([0,1,2,3,4,5])
    dates = dates.melt(id_vars=['Titre :'], var_name= 'cur_code', value_name= 'one_euro_value')
    dates = dates.rename(columns = {'Titre :': 'history_date'})
    dates = dates[(dates.one_euro_value != '-') & (dates.one_euro_value.notnull())]
    dates['cur_code'] = dates['cur_code'].str.rsplit('(', 1).str[1].str.rsplit(')', 1).str[0]
    dates['one_euro_value'] = dates['one_euro_value'].str.replace(',', '.')
    dates['one_euro_value'] = dates['one_euro_value'].astype(float)
    from_cur = dates.rename(columns = {'cur_code': 'from_cur_code', 'one_euro_value': 'from_euro_value'})
    to_cur = dates.rename(columns = {'cur_code': 'to_cur_code', 'one_euro_value': 'to_euro_value'})
    rate_history = pandas.merge(from_cur, to_cur, 'inner', ['history_date'])
    rate_history = rate_history[rate_history.from_cur_code != rate_history.to_cur_code]
    rate_history['exchange_rate'] = rate_history['to_euro_value']/rate_history['from_euro_value']
    return rate_history[['history_date', 'from_cur_code', 'to_cur_code', 'exchange_rate']]
    
df = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', delimiter= ';')
rate_history = exchange_rate_history(df)
