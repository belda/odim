import logging
import re
from datetime import datetime
from typing import Optional, Union
from bson import ObjectId as BsonObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
import inspect

from pymongo import ASCENDING, DESCENDING

import settings
from odim import Odim, Operation, SearchParams
from odim.helper import get_connection_info

log = logging.getLogger("uvicorn")

client_connections = {}

async def preinit_mongo_connections():
  global mongo_client
  mongo_client = AsyncIOMotorClient(settings.MONGO_URI)
  log.info("DB connected")

async def shutdown_mongo_client():
  for cc in client_connections:
    cc.close()
  log.info("DB disconnected")


def get_mongo_client(alias):
  global client_connections
  if alias not in client_connections:
    cinf = get_connection_info(alias)
    conn = AsyncIOMotorClient(cinf.url(withdb=False))
    client_connections[alias] = conn[cinf.db]
  return client_connections[alias]


class ObjectId(BsonObjectId):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not BsonObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return BsonObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type='string')



class BaseMongoModel(BaseModel):
  id: Optional[ObjectId] = Field(alias='_id', description="Unique identifier of the object") #

  class Config:
    arbitrary_types_allowed = True
    allow_population_by_field_name = True
    # underscore_attrs_are_private = True
    json_encoders = {
      ObjectId: str,
      BsonObjectId: str,
      datetime: lambda dt: dt.isoformat()
    }


class OdimMongo(Odim):
  protocols = ["mongo","mongodb"]

  def get_collection_name(self):
    if hasattr(self.model, 'Config'):
      if hasattr(self.model.Config, 'collection_name'):
        cn = self.model.Config.collection_name
        return cn
    return self.model.__class__.__name__


  async def get_mongo_client(self):
    return get_mongo_client(self.get_connection_identifier())


  async def save(self) -> ObjectId:
    mongo_client = await self.get_mongo_client()
    if not self.instance:
      raise AttributeError("Can not save, instance not specified ")#describe more how ti instantiate
    if not self.instance.id:
      self.instance.id = BsonObjectId()
    collection = self.get_collection_name()
    ret = await mongo_client[collection].insert_one(self.instance.dict(by_alias=True))
    return ret.inserted_id


  async def update(self):
    ''' Saves only the changed fields leaving other fields alone '''
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    dd = self.instance.dict(exclude_unset=True)
    dd_id = dd["_id"]
    del dd["_id"]
    ret = await mongo_client[collection].find_one_and_update({"_id" : dd_id}, {"$set" : dd})


  async def get(self, id : Union[str, ObjectId], **kwargs):
    if isinstance(id, str):
      id = ObjectId(id)
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    ret = await mongo_client[collection].find_one({"_id" : id})
    return self.model(**ret)

  def get_parsed_query(self, query):
    rsp = {}
    for k, (op, v) in self.parse_query_operations(query).items():
      #perhaps use model to ensure the search value is of correct type
      if op == Operation.exact:
        rsp[k] = v
      elif op == Operation.isnot:
        rsp[k] = { "$ne" : v}
      elif op == Operation.contains:
        rsp[k] = { "$regex" : '.*'+str(v)+'.*', "$options" : "i" }
      elif op == Operation.gt:
        rsp[k] = { "$gt" : v}
      elif op == Operation.gte:
        rsp[k] = { "$gte" : v}
      elif op == Operation.lt:
        rsp[k] = { "$lt" : v}
      elif op == Operation.gt:
        rsp[k] = { "$lte" : v}
      elif op == Operation.null:
        if v:
          rsp["$or"] = [ {k : {"$exists" : False}}, {k : None} ]
        else:
          rsp["$and"] = [ {k : {"$exists" : True}}, {k: { "$ne" : None }} ]
    return rsp


  async def find(self, query : dict, params : SearchParams = None):
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    #TODO use projection on model to limit to only desired fields
    find_params = {}
    if params:
      find_params["skip"] = params.offset
      find_params["limit"] = params.limit
      if params.sort not in (None,''):
        find_params["sort"] = []
        for so in params.sort.split(','):
          if so[0] == "-":
            find_params["sort"].append( (so[1:], DESCENDING) )
          else:
            find_params["sort"].append( (so, ASCENDING) )
    query = self.get_parsed_query(query)
    curs = mongo_client[collection].find(query, **find_params)
    ret = await curs.to_list(None)
    return [self.model(**x) for x in ret ]


  async def count(self, query : dict):
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    return await mongo_client[collection].count_documents(query)


  async def delete(self, obj : Union[str, ObjectId, BaseMongoModel]):
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    if isinstance(obj, str):
      d = {"_id" : ObjectId(obj)}
    elif isinstance(obj, ObjectId):
      d = {"_id" : obj}
    else:
      d = obj.dict()
    return await mongo_client[collection].delete_one(d)