import logging
import re
from datetime import datetime
from typing import Optional, Union
from bson import ObjectId as BsonObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
import inspect

from pymongo import ASCENDING, DESCENDING

from odim import BaseOdimModel, NotFoundException, Odim, Operation, SearchParams, all_json_encoders
from odim.helper import get_connection_info

log = logging.getLogger("uvicorn")

client_connections = {}


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



class BaseMongoModel(BaseOdimModel):
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

all_json_encoders.update( BaseMongoModel.Config.json_encoders)


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
    iii = self.execute_hooks("pre_save", self.instance)
    dd = iii.dict(by_alias=True)
    ret = await mongo_client[collection].insert_one(dd)
    dd = self.execute_hooks("post_save", dd)
    return ret.inserted_id


  async def update(self):
    ''' Saves only the changed fields leaving other fields alone '''
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    iii = self.execute_hooks("pre_save", self.instance)
    dd = iii.dict(exclude_unset=True, by_alias=True)
    if "_id" not in dd:
      raise AttributeError("Can not update document without _id")
    dd_id = dd["_id"]
    if isinstance(dd_id, str):
      dd_id = ObjectId(dd_id)
    del dd["_id"]
    ret = await mongo_client[collection].find_one_and_update({"_id" : dd_id}, {"$set" : dd})
    dd = self.execute_hooks("post_save", dd)
    return ret


  async def get(self, id : Union[str, ObjectId], **kwargs):
    if isinstance(id, str):
      id = ObjectId(id)
    mongo_client = await self.get_mongo_client()
    collection = self.get_collection_name()
    ret = await mongo_client[collection].find_one({"_id" : id})
    if not ret:
      raise NotFoundException()
    ret = self.execute_hooks("pre_init", ret)
    x = self.model(**ret)
    return self.execute_hooks("post_init", x)


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
    rsplist = []
    for x in await curs.to_list(None):
      x2 = self.execute_hooks("pre_init", x)
      m = self.model( **x2 )
      rsplist.append( self.execute_hooks("post_init", m) )
    return rsplist



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
    if self.has_hooks("pre_remove","post_remove"):
      ret = await mongo_client[collection].find_one(d)
      if not ret:
        raise NotFoundException()
      ret = self.execute_hooks("pre_init", ret)
      x = self.model(**ret)
      x = self.execute_hooks("post_init", x)
      x = self.execute_hooks("pre_remove", x)
    rsp = await mongo_client[collection].delete_one(d)
    if self.has_hooks("pre_remove","post_remove"):
      self.execute_hooks("post_remove", x)
    return rsp