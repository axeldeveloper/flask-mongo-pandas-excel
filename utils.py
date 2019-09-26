# -*- coding: utf-8 -*-

"""
    Copyright 2017, SQLSever vaiariaveis globais s
"""

import datetime
import  sqlite3
import sys
from locale import normalize
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

"""
    TODO: FUNÇOES
"""

def EscreveArquivo(nometxt, texto):
    """
        TODO: FUNÇÃO PARA ESCREVER ARQUIVOS
    """ 
    filename = nometxt
    myfile = open(filename, 'a')
    linha = texto;
    myfile.write(linha + '\n');
    myfile.close()

def remover_acentos(txt, codif='utf-8'):
    """
        TODO: FUNÇÃO PARA REMOVER ACENTOS
    """
    return normalize('NFKD', txt.decode(codif)).encode('ASCII','ignore')

def cursor_to_dict_query(cur, query, args=(), one=False):
    """
    TODO: CONVERTE UM CURSOR EM DICTIONARY
    :param cur: CURSOR
    :param query: SQL
    :param args:  ARGUMETO
    :param one: UM UNICO REGISTRO
    :return: DICTIONARY
    """
    cur.execute(query, args)
    r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
    #print r
    return (r[0] if r else None) if one else r

def cursor_to_dict_simples(cursor , rows ):
    """
        TODO:  CREATE JSON
    """ 
 
    columns = [d[0] for d in cursor.description]
    resp = [dict(zip(columns, row)) for row in rows]
    # for i in resp:
    #    print(i['rolename'])

def dtconverter(o):
    """
        TODO: CONVERTE DATA PARA FORMATO AMERICANO
    """
    if isinstance(o, datetime.datetime):
        return "{}-{}-{}".format(o.year, o.month, o.day)

def descobre_dia_semana():
    """
        TODO:
    """
    hj = datetime.datetime.today()
    dias = ('Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo')
    return str( dias[hj.weekday()] )

def diferenca_entre_datas(hj , futuro):
    """
        TODO:
    """
    #print(hj.toordinal())
    #print(futuro.toordinal())
    diferenca = futuro - hj
    return diferenca.days

def Conexao_Mongo_VM():
    """
    TODO: RETORNA CONEXAO COM O DB MONGO DA VM S1189
    CONEXAO COM MONGO DB
    :return: CONEXAO COM O MONGO DB VM S1189
    """

    try:
        # The ismaster command is cheap and does not require auth.
        #client.admin.command('ismaster')
        client = MongoClient("mongodb://172.20.13.96:27017/")
        return client
    except ConnectionFailure:
        print("Server not available")


def Conexao_SQLLITE(filename):
    """
        TODO: RETORNA CONEXAO COM O DB SQLITE
        CONEXAO COM SQLITE
        :return: CONEXAO COM O SQLITE local
    """
    try:
        client = sqlite3.connect(filename + ".db")
        return client
    except sqlite3.Error as e:
        print("An error occurred:" + e.args[0])
    finally:
        pass


#hj = datetime.datetime.today()
#futuro = datetime.datetime.fromordinal(hj.toordinal() + 15)  # hoje + 15 dias</pre>
#print(  diferenca_entre_datas( hj , futuro) )

