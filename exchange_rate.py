import pandas

# Create a table fact_exchange_rate_history with exchange rate history for all currencies
def exchange_rate_history(table):
    cur_history = df.drop([0,1,2,3,4])
    # Create the table cur_history with the history of the currencies and their values in euro
    cur_history = cur_history.melt(id_vars=['Titre :'], var_name= 'cur_code', value_name= 'one_euro_value')
    cur_history = cur_history.rename(columns = {'Titre :': 'history_date'})
    cur_history = cur_history[(cur_history.one_euro_value != '-') & (cur_history.one_euro_value.notnull())]
    # Calculate the currency code
    cur_history['cur_code'] = cur_history['cur_code'].str.rsplit('(', 1).str[1].str.rsplit(')', 1).str[0]
    # Cast the column one_euro_value to float
    cur_history['one_euro_value'] = cur_history['one_euro_value'].str.replace(',', '.').astype(float)
    # Create the table from_cur with the column from_cur_code
    from_cur = cur_history.rename(columns = {'cur_code': 'from_cur_code', 'one_euro_value': 'from_euro_value'})
    # Create the table to_cur with the column to_cur_code
    to_cur = cur_history.rename(columns = {'cur_code': 'to_cur_code', 'one_euro_value': 'to_euro_value'})
    # Merge the tables from_cur and to_cur
    rate_history = pandas.merge(from_cur, to_cur, 'inner', ['history_date'])
    rate_history = rate_history[rate_history.from_cur_code != rate_history.to_cur_code]
    #Calculate the exchange rate
    rate_history['exchange_rate'] = rate_history['to_euro_value']/rate_history['from_euro_value']
    return rate_history[['history_date', 'from_cur_code', 'to_cur_code', 'exchange_rate']]

# Read the csv file
df = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', delimiter= ';')
# Apply the function exchange_rate_history to df
fact_exchange_rate_history = exchange_rate_history(df)
