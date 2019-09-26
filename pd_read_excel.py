import sqlite3
import pandas as pd
from utils import Conexao_SQLLITE

file_name = "nome.xlsx"
DB  = "nome.db"

#xl_file = pd.ExcelFile(file_name)
#dfs = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}
#dfs1 = pd.read_excel(file_name, sheet_name=None)
#dfs2 = pd.read_excel(file_name, sheetname=None)
#print(dfs)
#print(dfs1)
#print(dfs2)


#con=sqlite3.connect(DB)
AGUA = "./provider/Agua.xlsx"
SEGU = "./provider/Seguranca.xlsx"
SEMI = "./provider/Seminario.xlsx"
VIO  = "./provider/Violencia.xlsx"

con=Conexao_SQLLITE('nome')

EXECELS = [AGUA, SEGU,  SEMI, VIO]
#EXECELS = [SEMI]
for row in EXECELS:
    print(row)

    #wb=pd.ExcelFile(AGUA)
    #for sheet in wb.sheet_names:
    #        df=pd.read_excel(AGUA,sheet_name=sheet)
    #        df.to_sql(sheet,con, index=False,if_exists="replace")

    wb=pd.read_excel(row,sheet_name=None)
    for sheet in wb:
        wb[sheet].to_sql(sheet,con, index=False,if_exists="replace")


con.commit()
con.close()
