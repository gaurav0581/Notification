import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.websockets import WebSocket, WebSocketDisconnect
import aio_pika
import os
from datetime import datetime,timedelta,timezone
import pytz
import jwt
from starlette.responses import JSONResponse
from fastapi import Request
from dotenv import load_dotenv,find_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import aioredis
import json
import schema
websockets={}
groups={}
load_dotenv(find_dotenv())
app = FastAPI()
tokenkey=os.environ['TOKEN_KEY']
#engine = create_engine(os.environ['DATABASE_URI'])
#SessionLocal = sessionmaker(bind=engine)
pool=aioredis.ConnectionPool.from_url(f'redis://127.0.0.1:6379/0', encoding="utf-8", decode_responses=True)
#def db_engine():
#    try:
#        db = SessionLocal()
#        yield db
#    finally:
#        db.close()

@app.on_event("startup")
async def init_redis_pubsub():
    async def rabbitmq_consumer():
        async def process_message(
                message: aio_pika.abc.AbstractIncomingMessage
        ) -> None:
            print('received message')
            async with message.process():
              try:
                k = json.loads(message.body.decode())
                data = schema.CommandMessageSchema.parse_obj(k)
                t=websockets.keys()
                if data.type == 0:
                    reciever=int(data.reciever)
                    if data.entity in websockets.keys() and reciever in websockets[data.entity].keys():
                        send = False
                        fail = False
                        for ws in websockets[data.entity][reciever]:
                            if fail:
                                break;
                            try:
                                await ws.send_text('this is test user')
                                await ws.send_json(json.dumps(data.data))
                                send = True
                            except Exception as exc:
                                if not send:
                                    await redist.lpush(str(data.entity)+':'+data.reciever, json.dumps(data.data))
                                    fail = True
                    else:
                        await redist.lpush(str(data.entity)+':'+data.reciever, json.dumps(data.data))

                if data.type == 1:
                    if data.entity in groups.keys() and data.reciever in groups[data.entity].keys():
                        for user in groups[data.entity][data.reciever]:
                            if user in websockets[data.entity].keys():
                                for ws in websockets[data.entity][user]:
                                    try:
                                        await ws.send_json(json.dumps(data.data))
                                    except Exception as e:
                                        pass
                            else:
                                await redist.lpush(str(data.entity)+':'+data.reciever, json.dumps(data.data))
                if data.type == 2:
                                    grp=await redist.smembers(str(data.entity) + ':' + str(data.reciever))
                                    if grp:
                                        for user in grp:
                                            if user in websockets[data.entity].keys():
                                                send = False
                                                for ws in websockets[data.entity][user]:
                                                    try:
                                                        await ws.send_json(json.dumps(data.data))
                                                        send = True
                                                    except Exception as e:
                                                        if not send:
                                                            await redist.lpush(str(data.entity) + ':' + data.reciever,
                                                                               json.dumps(data.data))
                                                            pass
                                            else:
                                                await redist.lpush(str(data.entity) + ':' + data.reciever,
                                                                   json.dumps(data.data))
              except Exception as e:
                  pass
        connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/",)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        redist = aioredis.Redis(connection_pool=pool)
        queue = await channel.declare_queue("authentic")
        while True:
           await queue.consume(process_message)
    conn=aioredis.Redis(connection_pool=pool)
#    db = SessionLocal()
    pubsub = conn.pubsub()
    asyncio.create_task(rabbitmq_consumer())

@app.websocket('/ws/{token1}')
async def ws_voting_endpoint(token1: str,websocket: WebSocket,):
    try:
        token = jwt.decode(token1, tokenkey, algorithms="HS256")
        if token["exp"] < datetime.now().strftime('%y%m%d%H%M%S'):  # fix
            return JSONResponse(status_code=401, content={'reason': "expired token"})
    except Exception as e:
        return JSONResponse(status_code=403, content={'reason': "invalid token"})
    user=token["ID"]
    redist=await aioredis.Redis(connection_pool=pool)
    await websocket.accept()
    usergroups, convert, multiple = findnotificationgroup(token, "NOTIFICATION")
    await setgroup(redist, websocket, user, usergroups, multiple)
    persistgroup,convert1, multiple = findnotificationgroup(token, "PERSISTNOT")
    await setpersist(redist, user, persistgroup)
    data=await redist.lrange(str(user),0,-1)
    await redist.delete(str(user))
    for i in range(len(data)):
          try:
              await websocket.send_json(json.dumps(data[i]))
          except:
            for j in range(i,len(data)):
               await redist.lpush(data[j])
    t=groups
    k = websockets
    try:
            while convert:
                data = await websocket.receive_text()
    except WebSocketDisconnect:
        ondisconnect(token)
        print("disconnected")

def findnotificationgroup(token,notType):
    usergroup=[]
    usergroups={}
    convert=False
    multiple=False
    for entity in token["ROLESPERMISSION"]:
        [usergroup.append(perm.split(':')[1]) for perm in entity["PERMISSION"] if notType in perm and (perm.split(':')[1]) not in usergroup]
        if usergroup:
          convert=True
        if 'user' in usergroup:
           usergroup.remove('user')
        if "userAll" in usergroup:
           usergroup.remove('userAll')
           multiple=True
        usergroups[entity['ENTITY']]=usergroup.copy()
        usergroup=[]
    return usergroups,convert,multiple

async def setpersist(redist,user,usergroups):
    for entity in usergroups.keys():
        usergroup = usergroups[entity]
        for group in usergroup:
            await redist.sadd('entgroup:' + str(entity), group)
            await redist.sadd('group:'+str(entity)+':'+group,user)

async def setgroup(redist,websocket,user,usergroups,multiple):
    for entity in usergroups.keys():
      data=await redist.lrange(str(entity)+':'+str(user),0,-1)
      await redist.delete(str(entity)+':'+str(user))
      for i in range(len(data)):
          try:
              await websocket.send_json(data[i])
             # await websocket.send(data[i])
          except:
              for j in range(i, len(data)):
                  await redist.lpush(str(entity)+':'+str(user),data[j])
      if not entity in websockets.keys():
        websockets[entity] = {}
      if not user in websockets[entity].keys() or not multiple:
          websockets[entity][user] = []
      websockets[entity][user].append(websocket)
      usergroup=usergroups[entity]
      for group in usergroup:
        if not entity in groups.keys():
            groups[entity] = {}
        if not group in groups[entity]:
            groups[entity][group]=[]
        if not user in groups[entity][group]:
            groups[entity][group].append(user)

def ondisconnect(user,usergroup):
    del websockets[user]
    for group in usergroup:
        if group in groups.keys():
          if user in groups[group]:
            groups[group].remove(user)

# To run locally
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8001)