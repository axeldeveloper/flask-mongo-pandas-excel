
#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import requests
import sqlite3
import aiosqlite
import asyncio
from dataclasses import dataclass
from unicodedata import normalize
from datetime import datetime

import psycopg2

# cpf not in('2763209130' , '5502964680', '78024269104', '8066171951'); "

TEST_DB = 'nome.db'

URI_PG = ''

query = " SELECT * FROM Pessoa_Fisica \
        where  cep is null \
        order by endereco, bairro LIMIT 400 "

qu = "update Pessoa_Fisica set cep = ? where  cpf = ? "; 

lista_pessoa = []

filename = "Import.txt"

def remover_acentos(txt, codif='utf-8'):
    #return normalize('NFKD', txt.decode(codif)).encode('ASCII', 'ignore')
    return normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')


@dataclass
class Pessoa:
    """
        :TODO   VO de pessoa a ser Importada
    """
    cod: int
    email: str
    cpf: int
    nome_completo: str
    aniversario: str
    endereco: str
    numero_residencia: str
    complemento: str
    bairro: str
    cidade: str
    telefone_contato: int
    profissao: str
    cep: str

class ViaCEP:

    def __init__(self, cep , cidade=None , endereco=None):
        self.cep = cep
        self.cidade =cidade
        self.endereco = endereco

    def getEndereco(self):
        
        _cidade   = remover_acentos(self.cidade.replace(" ", "%20") )
        _endereco = remover_acentos(self.endereco.replace(" ", "%20") )
        url_api = "http://www.viacep.com.br/ws/MS/%s/%s/json"  % (_cidade, _endereco)
        req = requests.get(url_api)
        #print(url_api)
        if req.status_code == 200:
            dados_json = json.loads(req.text) 
            print("Serviço" + str(req.status_code))
            return dados_json
        else:
            print("Serviço fora")
            return None


    def getDadosCEP(self):
        url_api = ('http://www.viacep.com.br/ws/%s/json' % self.cep)
        req = requests.get(url_api)
        if req.status_code == 200:
            dados_json = json.loads(req.text)
            return dados_json

async def fetchall_async(sql, itera=0):
    async with aiosqlite.connect(TEST_DB) as db:
        # await db.execute('INSERT INTO some_table ...')
        # await db.commit()

        async with db.execute(sql) as cursor:

            async for row in cursor:
                itera += 1
                pessoa = Pessoa(
                    cod=itera,
                    email=row[0],
                    cpf=row[1],
                    nome_completo=row[2],
                    aniversario=row[3],
                    endereco=row[4],
                    numero_residencia=row[5],
                    complemento=row[6],
                    bairro=row[7],
                    cidade=row[8],
                    telefone_contato=row[9],
                    profissao=row[10],
                    cep= ""
                )
                lista_pessoa.append(pessoa)
            return lista_pessoa

async def fetchall_task_grava_pessoa_pg(nome, cpf, nascimento):
    """
    update pessoa fisica egab
    :param nome: = Nome completo
    :param cpf: = cpf da pessoa
    :param nascimento: = data de nascimento da pessoa
    :return: commit
    """
    conn = None
    try:
        conn = psycopg2.connect(URI_PG)

        data = (nome, cpf, nascimento, True, 'CURRENT_DATE', 3, 2)

        sql = """INSERT INTO public.pessoa_pessoafisica(
                nome, 
                cpf, 
                nascimento, 
                status,  
                date_joined, 
                estabelecimento_id, 
                vinculo_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s) """

        # create a cursor
        cur = conn.cursor()
        print('PostgreSQL database version:')
        cur.execute(sql, data)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

async def fetchall_task_update_cep(cep, cpf):
    data = (cep, cpf)
    async with aiosqlite.connect(TEST_DB) as db:
        await db.execute(qu, (data) )
        await db.commit()
        return True
        
async def pessoa_endereco_igual(f , ws , row):

    set_endereco   = remover_acentos(ws['logradouro'])
    set_bairro     = remover_acentos(ws['bairro'])
    set_cidade     = remover_acentos(ws['localidade'])
    set_cep        = ws['cep'].replace("-", "") 
    set_cpf        = row.cpf

    db_endereco = remover_acentos(row.endereco)
    db_bairro   = remover_acentos(row.bairro)
    db_cidade   = remover_acentos(row.cidade)

    # Endereço igual       
    if (db_endereco.upper().rstrip() == set_endereco.upper().rstrip()):                                              
        if (db_bairro.upper().rstrip()  == set_bairro.upper().rstrip()):                         
            pessoas_update = await fetchall_task_update_cep(set_cep, row.cpf)
            msg = "Endereço Iguais (1) -> Atualizado  %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))               
            #break    # break here
            return True               
        elif db_bairro.upper().rstrip() in set_bairro.upper().rstrip() : 
            pessoas_update = await fetchall_task_update_cep(set_cep, row.cpf)
            msg = "Endereço Quase Iguais (2) -> Atualizado  %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))                  
            #break    # break here
            return True 
        else:
            pessoas_update = await fetchall_task_update_cep(set_cep, row.cpf)                                           
            msg = "Endereço  Iguais - Bairros diferentes (3) -> Atualizado (1.1) %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))                  
            #break    # break here
            return True 
    else: 
        #break    # break here
        return False 
    
async def pessoa_endereco_semalhante(f , ws , row):
    
    set_endereco   = remover_acentos(ws['logradouro'])
    set_bairro     = remover_acentos(ws['bairro'])
    set_cidade     = remover_acentos(ws['localidade'])
    set_cep        = ws['cep'].replace("-", "") 
    set_cpf        = row.cpf

    db_endereco = remover_acentos(row.endereco)
    db_bairro   = remover_acentos(row.bairro)
    db_cidade   = remover_acentos(row.cidade)

    # Endereço semelhante        
    if db_endereco.upper().rstrip() in set_endereco.upper().rstrip():
        if (db_bairro.upper().rstrip()  == set_bairro.upper().rstrip()): 
            pessoas_update = await fetchall_task_update_cep(set_cep, row.cpf)
            msg = "Endereço Semelhante - Bairro Iguais (4) -> Atualizado  %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))               
            #break    # break here
            return True                
        elif db_bairro.upper().rstrip() in set_bairro.upper().rstrip() : 
            pessoas_update = await fetchall_task_update_cep(set_cep, row.cpf)
            msg = "Endereço Semelhante - Bairro Semelhante (5) -> Atualizado  %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))                  
            #break    # break here
            return True 
        else:
            pessoas_update = await fetchall_task_update_cep(set_cep, row.cpf)
            msg = "Endereço Semelhante - Bairros diferentes (6) -> Atualizado %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))                  
            #break    # break here
            return True 
    else: 
        #break    # break here
        return False

async def pessoa_endereco_diferente(servicos, f, ws , row):
    if len(servicos) == 1:
        if (db_cidade.upper().rstrip() == set_cidade.upper().rstrip()):        
            set_cep    = ws['cep'].replace("-", "") 
        else:                
            set_cep = ""
           
        pessoas_update = await fetchall_task_update_cep(set_cep, set_cpf)
        msg = "Sem Endereço com cidade igual (7) -> Atualizado  %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)  
        f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))                 
        # break here
        return True
    
    elif len(servicos) == 2: 
        msg = "Sem Endereço regsitro 2 (8) %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)
        f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))
        return True     

    elif len(servicos) == 0:
        msg = "Sem Endereço registro zero (9) %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)
        f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))
        # break here
        return True

    else:
        msg = "Deu tudo errado (10) %s-%s-%s-%s-%s" % (set_cpf, set_endereco, set_bairro, set_cep, set_cidade)                           
        f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))
        # break here
        return True  


async def fetchall_task():
    pessoas = await fetchall_async(query, 1)

    f = open(filename, "a", encoding="utf-8")
    f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), "Iniciando Importação"))
    
    for row in pessoas:
        d = ViaCEP("79041080", str(row.cidade), str(row.endereco) )
        servicos = d.getEndereco()

        if servicos is not None:
            
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), "Endereços localizados QTD  = "+str(len(servicos)) ))
     
            for ws in servicos:
                

                if await pessoa_endereco_igual(f , ws , row):
                    break    # break here   

                # Endereço semelhante        
                elif await pessoa_endereco_semalhante(f , ws , row):
                    break    # break here   
                    
                # Endereço não e igual nem semelhante    
                else:
                   
                    if await pessoa_endereco_diferente(servicos, f, ws , row):
                        break    # break here                
                    
        elif servicos == None:  
            msg = "for vazio -> " 
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), msg ))                 
              
        else:
            r1=row.cpf
            r2=row.endereco
            r3=row.bairro   
            texto_final = "Não Existe endereco array cpf => %s endereco => %s bairro => %s." % (r1, r2, r3)     
            f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), texto_final ))
            #print(texto_final)  
               
         

    f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), "Fim da Importação"))
    f.close()

      
async def All_async(sql):
    """
        await fetc all rows
    """
    db = await aiosqlite.connect(TEST_DB)
    cursor = await db.execute(sql)
    row = await cursor.fetchone()
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()
    # return await cursor.fetchall()
    return rows

async def All_task():
    rows = await All_async(query)
    print(rows)

async def Consulta_end_async(cep):
    d = ViaCEP(cep)
    data = d.getDadosCEP()
    return data

async def Consulta_task():
    #conn = aiosqlite.connect('nome.db')
    rows = await Consulta_end_async("79041080")
    print(rows)


try:

    # asyncio.run(some_task())

    # loop.run_forever()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetchall_task())
    # loop.run_until_complete(All_task())
    # loop.run_until_complete(Consulta_task())
    # loop.run_until_complete(use_distinct_command())
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    # pass
