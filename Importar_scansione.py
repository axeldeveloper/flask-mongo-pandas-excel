
#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import requests
import sqlite3
import aiosqlite
import asyncio
from dataclasses import dataclass

TEST_DB = 'nome.db'

query = "select * from Pessoa_Fisica where rowid = 1 "

lista_pessoa = []


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
        url_api  = ('http://www.viacep.com.br/ws/MS/%s/' % self.cidade )
        url_api2 = (url_api + '%s/json' % self.endereco)
        req = requests.get(url_api2)
        if req.status_code == 200:
            dados_json = json.loads(req.text)
            return dados_json


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
                #print(  float( row[1])  )
                #print(  row[0]  )

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


async def fetchall_task():
    students = await fetchall_async(query, 1)
    for row in students:
        # print("Task ", row.cpf)
        # print ( "\"%s\"  %s (%s)" % (row.cpf, row.cidade, row.endereco) )

        d = ViaCEP("79041080", str(row.cidade), str(row.endereco) )
        data = d.getEndereco()
        #row.cep = data

        if row.bairro  in  data[0]['bairro']:
            print("mesmo bairro")

        row.cep = data[0]['cep']
        print( row.bairro )
        print( data[0]['bairro'] )
        print( data[0]['cep'] )


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


"""
async def Crud():
    async with aiosqlite.connect(...) as db:
        await db.execute('INSERT INTO some_table ...')
        await db.commit()
        async with db.execute('SELECT * FROM some_table') as cursor:
            async for row in cursor:
"""

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
