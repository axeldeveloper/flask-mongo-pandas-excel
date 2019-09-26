import pymongo
import motor.motor_asyncio
import asyncio
import pprint
from datetime import datetime
from bson.objectid import ObjectId
from bson import SON

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
db = client.semaforo


async def do_find2():
    cursor = db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    for document in await cursor.to_list(length=100):
        pprint.pprint(document)

async def do_find1():
    c = db.semaphore_telemetry
    async for document in c.find({'id': '0001A12X1201804'}):
        pprint.pprint(document)

async def do_find():
    cursor = db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    # Modify the query before iterating
    cursor.sort('id', -1).skip(1).limit(2)
    async for document in cursor:
        pprint.pprint(document)

async def do_count():
    n = await db.semaphore_telemetry.count_documents({})
    print('%s documents in collection' % n)
    n = await db.semaphore_telemetry.count_documents({'id': '0001A12X1201804'})
    print('%s documents where id 0001A12X1201804' % n)

async def do_update():
    coll = db.semaphore_telemetry
    result = await coll.update_one({'id': 5}, {'$set': {'data_real': datetime.now() }})
    print('updated %s document' % result.modified_count)
    new_document = await coll.find({'id': 5})
    print('document is now %s' % pprint.pformat(new_document))

async def do_update_many():
    coll = db.cidades  
    result = await coll.update_many({'id': 5}, {'$set': {'data_real': datetime.now() }})
    print('updated %s document' % result.modified_count)
    _id = ObjectId("5cfeb516a6bcf52f78b92fae")
    new_document = await coll.find_one({'_id': _id})
    print('document is now %s' % pprint.pformat(new_document))   

async def do_delete_many():
    coll = db.test_collection
    n = await coll.count_documents({})
    print('%s documents before calling delete_many()' % n)
    result = await db.test_collection.delete_many({'i': {'$gte': 1000}})
    print('%s documents after' % (await coll.count_documents({})))

async def use_distinct_command():
    response = await db.command(SON([("distinct", "cidades"), ("key", "i")]))
    pprint.pprint(response)



loop = asyncio.get_event_loop()
#loop.run_until_complete(do_find())
loop.run_until_complete(do_count())
#loop.run_until_complete(do_update_many())
loop.run_until_complete(use_distinct_command())

