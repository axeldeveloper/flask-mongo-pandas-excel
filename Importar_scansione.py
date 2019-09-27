
import sqlite3
import aiosqlite
import asyncio
from utils import Conexao_SQLLITE


conn=Conexao_SQLLITE('nome')

TEST_DB = 'nome.db'

query = "select * from Pessoa_Fisica"

async def fetchall_async(sql):
    async with aiosqlite.connect(TEST_DB) as db:
        #await db.execute('INSERT INTO some_table ...')
        #await db.commit()

        async with db.execute(sql) as cursor:
            async for row in cursor:
                print(row[1])

 
async def fetchall_task():
    students = await fetchall_async(query)


async def All_async(sql):
    db = await aiosqlite.connect(TEST_DB)
    cursor = await db.execute(sql)
    row = await cursor.fetchone()
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()
    #return await cursor.fetchall()
    return rows
    
async def All_task():
    #conn = aiosqlite.connect('nome.db')
    rows = await All_async(query)
    print( rows )




"""
async def Crud():
    async with aiosqlite.connect(...) as db:
        await db.execute('INSERT INTO some_table ...')
        await db.commit()

        async with db.execute('SELECT * FROM some_table') as cursor:
            async for row in cursor:

"""

try:
    
    #asyncio.run(some_task()) 
    
    #loop.run_forever()   
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetchall_task())
    #loop.run_until_complete(All_task())
    #loop.run_until_complete(do_update_many())
    #loop.run_until_complete(use_distinct_command())
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    #pass






