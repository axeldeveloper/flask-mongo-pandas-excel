import asyncio
import sqlite3
from datetime import datetime

import aiosqlite
import asyncpg
import psycopg2

TEST_DB = 'nome.db'

filename = "Import_erro.txt"
file_ok  = "Import_ok.txt"

DATE_CURRENT = datetime.today()


URI_PG = 'postgres://zqliekrizhkwsi:1dec7157e5a2c73b45b1ca758edb4ada4b2844fe60802a2471a9e82f781bb61f@ec2-23-21-160-38.compute-1.amazonaws.com:5432/da0kq4ihbshuko'


SQ_SELECT_PESSOA = '''SELECT * FROM PESSOA_FISICA WHERE CEP IS NOT NULL AND importado IS NULL ORDER BY ENDERECO, BAIRRO LIMIT 100 '''

SQ_UPDATE_IMPORTADO = "update Pessoa_Fisica set IMPORTADO = ? where  cpf = ? "; 


PG_SELECT_PESSOA = '''SELECT * FROM PUBLIC.PESSOA_PESSOAFISICA ORDER BY ID ASC '''
PG_INSERT_PESSOA = '''INSERT INTO PUBLIC.PESSOA_PESSOAFISICA(
                        NOME, 
                        APELIDO,
                        CPF, 
                        NASCIMENTO,
                        SEXO, 
                        STATUS, 
                        DATE_JOINED, 
                        ESTABELECIMENTO_ID,
                        TRATAMENTO_ID,
                        VINCULO_ID  )
                        VALUES ( 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s,
                            %s, 
                            %s ) RETURNING id '''

PG_ENDERECO_PESSOA_INSERT = '''INSERT INTO PUBLIC.PESSOA_PESSOAFISICA_ENDERECO(
                                STATUS, 
                                ENDERECO_ID, 
                                PESSOAFISICA_ID)
                                VALUES (
                                    %s, 
                                    %s, 
                                    %s); '''

PG_ENDERECO_INSERT = ''' INSERT INTO PUBLIC.CADASTRO_ENDERECO(
	                        ENDERECO, 
                            NR_ENDERECO, 
                            COMPLEMENTO, 
                            STATUS, 
                            LOCALIDADE, 
                            BAIRRO, 
                            UF, 
                            CEP)
	                        VALUES (
                                %s, 
                                %s, 
                                %s, 
                                %s, 
                                %s, 
                                %s, 
                                %s, 
                                %s) RETURNING id; '''

# 55 56 57 58 

async def pessoa_temp_fetchall_async():
    conn = await aiosqlite.connect(TEST_DB)
    conn.row_factory = sqlite3.Row  
    cur = await conn.cursor()
    await cur.execute(SQ_SELECT_PESSOA)
    rows = await cur.fetchall()
    await cur.close()
    await conn.close()
    return rows

async def pessoa_temp_update_async(imp, cpf):
    async with aiosqlite.connect(TEST_DB) as db:
        await db.execute(SQ_UPDATE_IMPORTADO, (imp, cpf))
        await db.commit()
        #await db.close()
        return True


async def grava_pessoa_pg(conn, nome, apelido, cpf, nascimento, sexo):

    
    """
    update pessoa fisica egab
    :param nome = Nome completo
    :param apelido = apelido
    :param cpf = cpf da pessoa
    :param nascimento = data de nascimento da pessoa
    :param sexo = M ou F
    :return: commit
    """
    
    try:       
        data = ( 
            str(nome), 
            str(apelido), 
            str(cpf), 
            datetime.strptime(nascimento , '%Y-%m-%d').date(),
            str(sexo), 
            True, 
            DATE_CURRENT, 
            int(3), 
            int(1), 
            int(2) ,
        )

        data2 =  {
            'str': nome, 
            'str': apelido, 
            'str': cpf, 
            'date': datetime.strptime(nascimento , '%Y-%m-%d').date(),
            'str': sexo, 
            'bool': True, 
            'date': DATE_CURRENT,
            'int': 3,
            'int': 1,
            'int': 2,
        }

        #print(data)
  
        cur = conn.cursor()
        cur.execute(PG_INSERT_PESSOA, data)
        #print(cur.mogrify(PG_INSERT_PESSOA, data))
        #conn.commit()
        return cur.fetchone()[0]
    except (Exception, psycopg2.DatabaseError) as error:      
        print("Error -> " , error)
        return None

async def grava_pessoa_endereco_pg(conn, endereco_id, pessoa_id):
    """
        update pessoa_endereco egab
        :param status: = Nome completo
        :param endereco_id: = endereço
        :param pessoa_id: = pessoa
        :return: commit
    """
    
    try:       
        data = (True, endereco_id, pessoa_id)
        cur = conn.cursor()
        cur.execute(PG_ENDERECO_PESSOA_INSERT, data)
        #conn.commit()
        return True
    except (Exception, psycopg2.DatabaseError) as error:      
        print(error)
        return False

async def grava_endereco(conn, end , num , comp,  local, bairro,  cep):
    try:       
        
        data = (end , num , comp, True, local, bairro, "MS", cep)
        cur = conn.cursor()
        cur.execute(PG_ENDERECO_INSERT, data)
        #conn.commit()
        return cur.fetchone()[0]
    except (Exception, psycopg2.DatabaseError) as error: 
        print(error)
        return False


async def run1():
    conn = await asyncpg.connect(
            user='zqliekrizhkwsi', 
            password='1dec7157e5a2c73b45b1ca758edb4ada4b2844fe60802a2471a9e82f781bb61f',
            database='da0kq4ihbshuko', 
            host='ec2-23-21-160-38.compute-1.amazonaws.com', 
            port=5432)
    
    #conn = await asyncpg.connect('postgres://zqliekrizhkwsi:1dec7157e5a2c73b45b1ca758edb4ada4b2844fe60802a2471a9e82f781bb61f@ec2-23-21-160-38.compute-1.amazonaws.com:5432/da0kq4ihbshuko')
    rows = await conn.fetch(PG_SELECT_PESSOA)
    for row in rows:
            id = row[0]
            nome = row[1]
            print (f' ID {id} - Nome {nome}') 

    await conn.close()
  
async def run():
    conn = None
    try:
        conn = psycopg2.connect(URI_PG)
        pessoas = await pessoa_temp_fetchall_async()

        f = open(filename, "a", encoding="utf-8")
        f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"),  "Inicio" ))

        t = open(file_ok, "a", encoding="utf-8")
        t.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"),  "Inicio" ))

    
        for row in pessoas:
            #obj = type('Expando', (object,), {'cpf': row['cpf'], 'nome': row['nome_completo'], 'apelido': row['nome_completo'].split(" ")[0],'aniversario' : datetime.strptime(row['aniversario'], '%Y-%m-%d %H:%M:%S'), 'sexo' : "M" })()
            
            str_date = row['aniversario'].rstrip()

            #value = row['complemento']


            if row['complemento'] is not None:
                value = row['complemento'][0:24]
            else:
                value = None

            #info = (data[:75] + '..') if len(data) > 75 else data


            #print(  datetime.strptime(str_date , '%Y-%m-%d').date()  )
            #print(type(row['aniversario'] )  ) 
            #print(f'cpf {obj.cpf} - nome {obj.nome} - apelido {obj.apelido} - sexo {obj.sexo} - aniversario {obj.aniversario}') 

            obj = lambda:expando
            obj.cpf         = row['cpf']
            obj.nome        = row['nome_completo'].rstrip()
            obj.apelido     = row['nome_completo'].split(" ")[0]
            obj.aniversario = row['aniversario'].rstrip()
            
            
            obj.sexo        = "M"
            obj.endereco    = row['endereco'].rstrip()
            obj.numero      = row['numero_residencia']
            obj.complemento = value
            obj.bairro      = row['bairro']
            obj.cidade      = row['cidade']
            obj.cep         = row['cep']
            obj.telefone    = row['telefone_contato']
            #print(f'Endereco {obj.endereco} - numero {obj.numero} - complemento {obj.complemento} - bairro {obj.bairro} - cidade {obj.cidade} - cep {obj.cep}') 

            #print( obj.aniversario )

            p_ok = await grava_pessoa_pg(conn, 
                                        obj.nome, 
                                        obj.apelido, 
                                        obj.cpf, 
                                        obj.aniversario, 
                                        obj.sexo)
            print("Insert P" , p_ok)

            if p_ok > 0 :
                e_ok = await grava_endereco(conn, obj.endereco, obj.numero,  obj.complemento, obj.cidade, obj.bairro, obj.cep)
                print("Insert E" , e_ok)

                if e_ok > 0: 
                    ok = await grava_pessoa_endereco_pg(conn, e_ok, p_ok)
                    print("Insert O" , ok)
                    t.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"),  obj.cpf ))
                    
                    up = await pessoa_temp_update_async(1, obj.cpf)
                    conn.commit()
     
            else:
                f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"),  obj.cpf ))
    
        f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), "Fim da Importação"))
        f.close()

        t.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M"), "Fim da Importação"))
        t.close()

        
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERRO -> ", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



loop = asyncio.get_event_loop()
loop.run_until_complete(run())