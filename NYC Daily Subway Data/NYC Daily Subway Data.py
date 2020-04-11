
'''
Data collected from New York State Open Data (https://data.ny.gov/browse?q=Turnstile+Usage+Data&tags=subway)

The 'Entries' and 'Exits' are measured in each turnstile within each station and are cumulative. The data is collected
about each 4 hours.

In order to get the actual entries and exits for each turnstile, we need to calculate the change from the previous period.

This code opens each file and calculates the daily number of entries for each station, removing outliers (abs(zscore) > 3).
Then it append all the resulting dataframes into one and exports it to a Excel file.

Since each file contains a year of information about turnstile entries and exits they have about ~700MB/each.
Dealing with such large files is not a task for a Jupyter Notebook, so I ran this program in an IDLE (PyCharm).

'''

# ================================================================================================================================
# # Libraries
# ================================================================================================================================
import os
import pandas as pd
from tqdm import tqdm
import re
from scipy import stats
import numpy as np

# ================================================================================================================================
# # Params
# ================================================================================================================================

path = r'C:\Users\gusta\Downloads\Macro\mta'
archives = [path + '\\' + x for x in os.listdir(path)]
dateparse = lambda x, y: pd.datetime.strptime(str(x) + ' ' + str(y), '%m/%d/%Y %H:%M:%S')

# ================================================================================================================================
# # Filtering data
# ================================================================================================================================

all = []
for arq in tqdm(archives):

    year = re.match('.*Turnstile_Usage_Data__(.*).csv', arq)[1]
    dates = pd.date_range(start = f'01/01/{year}', end = f'12/31/{year}', freq='D')

    # temp_tb = pd.read_table(arq, sep=',', parse_dates={'datetime': ['Date', 'Time']}, date_parser=dateparse)\
    #     .groupby(['C/A', 'Unit', 'SCP', 'Station','Line Name', 'Division', pd.Grouper(key='datetime', freq='D')]).tail(1) \
    #     .merge(pd.DataFrame({'datetime': dates}), left_on='datetime', right_on='datetime', how='outer')\
    #     .drop_duplicates(['C/A', 'Unit', 'SCP', 'Station','Line Name', 'Division', 'datetime'], keep = 'last')

    full_tb = pd.read_table(arq, sep=',', parse_dates={'datetime': ['Date', 'Time']}, date_parser=dateparse)

    unique_stations = full_tb.Station.unique().tolist()

    temps_tables = []
    for station in tqdm(unique_stations):

        temp_tb = full_tb.query('Station == @station')\
                    .pivot_table(columns=['C/A', 'Unit', 'SCP', 'Station', 'Line Name', 'Division'], index='datetime', values='Entries')\
                    .ffill().diff()

        temp_tb = temp_tb[temp_tb > 0]

        temp_tb_zscore = (temp_tb.apply(lambda a: np.abs((a - a.mean()) / a.std(ddof=0))) < 3) \
                            .reset_index().melt(id_vars='datetime').value

        temp_tb_cleaned = temp_tb.reset_index().melt(id_vars='datetime')[temp_tb_zscore] # Filter out Z-Score > 3

        temp_tb, temp_tb_zscore = None, None

        temp_tb_cleaned_sumed = temp_tb_cleaned.groupby(['Station', pd.Grouper(key='datetime', freq='D')])\
                                    .sum().reset_index()

        temp_tb_cleaned = None

        temps_tables.append(temp_tb_cleaned_sumed)

        temp_tb_cleaned_sumed = None

    send_tb = pd.concat(temps_tables, axis = 0)

    temps_tables = None

    all.append(send_tb)

    send_tb = None

# ================================================================================================================================
# # Daily entries by day
# ================================================================================================================================

merged = pd.concat(all, axis = 0)\
            .query('abs(value) < 5000000')\
            .drop_duplicates(['Station', 'datetime'], keep = 'last')\
            .pivot_table(index = 'Station', columns = 'datetime', values = 'value')

merged_ = merged.rolling(7, axis = 1).mean().pct_change(365, axis = 1)

print('Saving merged')
merged.to_excel(r'C:\Users\gusta\Downloads\Macro\mta_info_daily_stations.xlsx')
merged_.to_excel(r'C:\Users\gusta\Downloads\Macro\mta_info_daily_stations_pctchange.xlsx')

# ================================================================================================================================
# # Total daily entries
# ================================================================================================================================

total = pd.concat(all, axis = 0).query('abs(value) < 5000000')\
            .drop_duplicates(['Station', 'datetime'], keep = 'last')\
            .pivot_table(index = 'Station', columns = 'datetime', values = 'value')\
            .sum()

total_ = pd.DataFrame({'value':total.values, 'variable':total.index})
total_['ma7d'] = total_.value.rolling(7).mean()
# total_['chg_yoy'] = total_.groupby([total_.variable.dt.month, total_.variable.dt.month])['ma7d'].pct_change()
total_['chg_yoy'] = total_['ma7d'].pct_change(365)
# total_ = total_.query('-1 < chg_yoy < 1')
# total_.query('variable >= @pd.to_datetime("01/01/2016")').dropna().plot(x = 'variable', y = 'chg_yoy')
# plt.show()

print('Saving total')
total.to_excel(r'C:\Users\gusta\Downloads\Macro\mta_info_daily_total.xlsx')
total_.to_excel(r'C:\Users\gusta\Downloads\Macro\mta_info_daily_total_pctchange.xlsx')
