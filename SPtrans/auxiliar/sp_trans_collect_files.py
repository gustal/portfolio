'''
This script collects São Paulo's bus statistics from SPTrans and send it to a local SQLite3 database.

Multithreading was used to speed up the process.
'''


# ===================================================================================================
# # Libraries
# ===================================================================================================

# Data handling
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs

# RegEX
import re

# Visualization
import matplotlib.pyplot as plt

# SQL
from sqlalchemy import Column, create_engine, MetaData, insert, Table, bindparam, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.types import TEXT, BLOB, DATETIME, INTEGER, FLOAT

# Others
from datetime import datetime as dt
import io
from tqdm import tqdm

import locale
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

# ======================================================================================================
# # Database
# ======================================================================================================

engine = create_engine(f'sqlite:///D:/Datasets/SPtrans/sptrans_all.db')

meta = MetaData()
meta.bind = engine

sptrans = Table(
    'sptrans', meta,
    Column('date', DATETIME),
    Column('type', TEXT),
    Column('area', TEXT),
    Column('company', TEXT),
    Column('line', TEXT),
    Column('cash_passengers', INTEGER),
    Column('normal_passengers', INTEGER),
    Column('monthly_normal_passengers', INTEGER),
    Column('students_passengers', INTEGER),
    Column('monthly_students_passengers', INTEGER),
    Column('vt_passengers', INTEGER),
    Column('monthly_vt_passengers', INTEGER),
    Column('int_cptm_passengers', INTEGER),
    Column('monthly_int_cptm_passengers', INTEGER),
    Column('paying_passengers', INTEGER),
    Column('int_bus_passengers', INTEGER),
    Column('free_pass_passengers', INTEGER),
    Column('free_pass_student_passengers', INTEGER),
    Column('total_passengers', INTEGER),
    PrimaryKeyConstraint('date', 'type', 'area', 'company', 'line', sqlite_on_conflict = 'REPLACE')
)

smto = insert(sptrans).values(
    {f'{x.name}':bindparam(f'{x.name}') for x in sptrans.columns}
)

if not engine.dialect.has_table(engine, 'sptrans'):
    print('Creating database')
    sptrans.create()


# ===================================================================================================
# # Links
# ===================================================================================================

links = [
    'https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/agenda/index.php?p=292723',
    'https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=269652',
    'https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=247850',
    'https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=228269',
    'https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=209427'
]

# ===================================================================================================
# # Built-in functions
# ===================================================================================================

def getLinks(url):
    '''This function parses each SP Trans page and returns the links for each day'''

    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

    r = requests.get(url)
    soup = bs(r.text, 'html.parser')

    match_xlsx = '(https|http)://www.prefeitura.sp.gov.br/cidade/secretarias/upload/.*.xls' # Match pattern

    year = ''.join([x for x in soup.h2.text if x.isnumeric()])
    tables = soup.findAll('table')
    urls = []
    for table in tables:
        a_href = table.findAll('a')
        month = table.caption.text

        month = re.sub(r'\t', '', month).strip() # Fixing format

        urls_temp = [(x['href'], dt.strptime(month + x.text + year, '%B%d%Y')) for x in a_href if
                     re.match(match_xlsx, x['href']) and not re.match('.*Total|total.*', x.text)]
        urls += urls_temp

    url_iter = iter(urls)

    def ExcelFile(reversed = False):

        iurl = next(iter(url_iter))
        col_names = {'Data': 'date',
         'Tipo': 'type',
         'Area': 'area',
         'Empresa': 'company',
         'Linha': 'line',
         'Passageiros Pagtes Em Dinheiro': 'cash_passengers',
         'Passageiros Pagtes Comum':'normal_passengers',
         'Passageiros Pgts Bu Comum M':'monthly_normal_passengers',
         'Passageiros Pagtes Estudante':'students_passengers',
         'Passageiros Pgts Bu Est Mensal':'monthly_students_passengers',
         'Passageiros Pagtes Bu Vt':'vt_passengers',
         'Passageiros Pgts Bu Vt Mensal':'monthly_vt_passengers',
         'Passageiros Pagtes Int M/Cptm':'int_cptm_passengers',
         'Passageiros Pgts Int M/Cptm M':'monthly_int_cptm_passengers',
         'Passageiros Pagantes':'paying_passengers',
         'Passageiros Int Ônibus->Ônibus':'int_bus_passengers',
         'Passageiros Com Gratuidade':'free_pass_passengers',
         'Passageiros Com Gratuidade Est':'free_pass_student_passengers',
         'Tot Passageiros Transportados':'total_passengers',
         'Passageiros Comum e VT':'vt_and_normal_passengers'
                     }

        keep_trying = True
        tries = 10
        while keep_trying and tries > 0:
            try:
                raw = pd.read_excel(io.BytesIO(requests.get(iurl[0]).content)).dropna(how = 'all')
                keep_trying = False
            except:
                keep_trying = True
                tries -= 1
                pass

        if 'Data' not in raw.columns:
            raw.columns = raw.iloc[0, :].tolist()
            raw.drop(1, inplace = True)

        raw = raw.assign(Data = iurl[1]).rename(columns = col_names)

        col_names_list = [x[1] for x in col_names.items()]
        add_columns = [x for x in col_names_list if x not in raw.columns]

        if len(add_columns) >= 1:
            raw = pd.concat([raw, pd.DataFrame(columns = add_columns)], axis = 1)

        raw = raw.groupby(['date', 'type', 'area', 'company', 'line']).sum().reset_index()

        return raw


    return ExcelFile, urls

# TODO: Fix for 2017 (links[3:-1]) and before. Columns names are different.

def main(url):
    innerfunc, urls = getLinks(url)

    for n in tqdm(range(len(urls))):
        try:
            tb = innerfunc()
        except:
            continue

        with engine.connect() as connection:
            connection.execute(smto, tb.to_dict('records'))


from multiprocessing import Pool
if __name__ == '__main__':

    try:
        with Pool(3) as p:
            r = list(tqdm(p.imap(main, links[0:3]), total=len(links[0:3]) * 365))
    except Exception as exp:
        print(exp)

# ===================================================================================================
# # Consolidating data (Daily YoY change) and exporting as an Excel file
# ===================================================================================================

query_string = 'SELECT `date`, sum(total_passengers) as total_passengers from sptrans GROUP BY `date`'
tb = pd.read_sql_query(query_string, engine, parse_dates = 'date', index_col = 'date')
tb['ma7d'] = tb.rolling(7).mean()
tb['pc_change_yoy'] = tb.ma7d.pct_change(365)
tb.to_excel(r'D:\Projetos\portfolio\SPtrans\sptrans.xlsx')

