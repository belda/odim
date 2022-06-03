import logging
import re
from datetime import datetime
from decimal import Decimal
from time import sleep
from typing import List, Optional, Union

import bson
from bson import ObjectId as BsonObjectId
from pydantic import Field
from functools import wraps, partial
import asyncio
from pymongo import MongoClient, errors

from pymongo import ASCENDING, DESCENDING

from odim import BaseOdimModel, NotFoundException, Odim, Operation, SearchParams, all_json_encoders
from odim.helper import awaited, get_connection_info

log = logging.getLogger("uvicorn")

client_connections = {}


def async_wrap(func):
  @wraps(func)
  async def run(*args, loop=None, executor=None, **kwargs):
      if loop is None:
          loop = asyncio.get_event_loop()
      f = partial(func, *args, **kwargs)
      return await loop.run_in_executor(executor, f)
  return run

# @async_wrap
async def get_mongo_client(alias):
  global client_connections
  if alias not in client_connections:
    cinf = get_connection_info(alias)
    client_connections[alias] = MongoClient(cinf.url(withdb=False), cinf.port)[cinf.db]
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
      datetime: lambda dt: dt.isoformat(),
      Decimal : float #im afraid im loosing precission here or we might output it as string?
    }

all_json_encoders.update( BaseMongoModel.Config.json_encoders)


def convert_decimal(dict_item):
  if dict_item is None:
    return None
  elif isinstance(dict_item, list):
    l = []
    for x in dict_item:
      l.append(convert_decimal(x))
    return l
  elif isinstance(dict_item, dict):
    nd = {}
    for k, v in list(dict_item.items()):
      nd[k] = convert_decimal(v)
    return nd
  elif isinstance(dict_item, Decimal):
    return bson.Decimal128(str(dict_item))
  else:
    return dict_item


class OdimMongo(Odim):
  protocols = ["mongo","mongodb"]

  @property
  def get_collection_name(self):
    if hasattr(self.model, 'Config'):
      if hasattr(self.model.Config, 'collection_name'):
        cn = self.model.Config.collection_name
        return cn
    return self.model.__class__.__name__


  @property
  async def __mongo(self):
    client = await get_mongo_client(self.get_connection_identifier)
    return client[self.get_collection_name]


  async def get(self, id : Union[str, ObjectId], extend_query : dict= {}, include_deleted : bool = False):
    if isinstance(id, str):
      id = ObjectId(id)
    softdel = {self.softdelete(): False} if self.softdelete() and not include_deleted else {}
    
    db = await self.__mongo

    ext = self.get_parsed_query(extend_query)
    qry = {"_id" : id, **softdel, **ext}
    ret = db.find_one(qry)
    if not ret:
      raise NotFoundException()
    ret = self.execute_hooks("pre_init", ret) # we send the DB Object into the PRE_INIT
    x = self.model(**ret)
    x = self.execute_hooks("post_init", x) # we send the Model Obj into the POST_INIT
    return x


  async def save(self, extend_query : dict= {}, include_deleted : bool = False) -> ObjectId:
    if not self.instance:
      raise AttributeError("Can not save, instance not specified ")#describe more how ti instantiate
    iii = self.execute_hooks("pre_save", self.instance, created=(not self.instance.id))
    dd = convert_decimal(iii.dict(by_alias=True))

    if not self.instance.id:
      dd["_id"] = BsonObjectId()
      iii.id = dd["_id"]
      self.instance.id = dd["_id"]
      softdel = {self.softdelete(): False} if self.softdelete() else {}
      db = await self.__mongo
      ret = db.insert_one({**dd, **extend_query, **softdel})
      created = True
    else:
      softdel = {self.softdelete(): False} if self.softdelete() and not include_deleted else {}
      db = await self.__mongo
      ret = db.replace_one({"_id": self.instance.id, **softdel, **self.get_parsed_query(extend_query)}, dd)
      assert ret.modified_count > 0, "Not modified error"
      created = False
    iii = self.execute_hooks("post_save", iii, created=created)
    return self.instance.id


  async def update(self, extend_query : dict= {}, include_deleted : bool = False, only_fields : Optional[List['str']] = None):
    ''' Saves only the changed fields leaving other fields alone '''
    iii = self.execute_hooks("pre_save", self.instance, created=False)
    dd = convert_decimal(iii.dict(exclude_unset=True, by_alias=True))
    if "_id" not in dd:
      raise AttributeError("Can not update document without _id")
    dd_id = dd["_id"]
    if isinstance(dd_id, str):
      dd_id = ObjectId(dd_id)
    del dd["_id"]
    if only_fields and len(only_fields)>0:
      dd = dict([(key, val) for key, val in dd.items() if key in only_fields])
    softdel = {self.softdelete(): False} if self.softdelete() and not include_deleted else {}
    db = await self.__mongo
    ret = db.find_one_and_update({"_id" : dd_id, **softdel, **self.get_parsed_query(extend_query)}, {"$set" : dd})
    iii = self.execute_hooks("post_save", iii, created=False)
    return ret


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

  async def find(self, query: dict, params : SearchParams = None, include_deleted : bool = False, retries=0):
    if self.softdelete() and not include_deleted:
      query = {self.softdelete(): False, **query}
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
    db = await self.__mongo
   
    rsplist = []
    try:
      results = db.find(query, **find_params)
      for x in results:
        x2 = self.execute_hooks("pre_init", x)
        m = self.model( **x2 )
        rsplist.append( self.execute_hooks("post_init", m) )
      return rsplist
    except Exception as e:
      if retries > 5:
            raise
      log.warn(f'Mongo Query returned an error, retrying find({query})! {e}')
      sleep(.2)
      return await self.find(query, params, include_deleted, retries=retries+1)



  async def count(self, query : dict, include_deleted : bool = False, retries=0):
    if self.softdelete() and not include_deleted:
      query = {self.softdelete(): False, **query}

    try:
      db = await self.__mongo
      c = db.count_documents(query)
      return c
    except Exception as e:
      if retries > 5:
        raise
      log.warn(f'Mongo Query returned an error, retrying count({query})! {e}')
      sleep(.2)
      return await self.count(query, include_deleted, retries=retries+1)


  async def delete(self, obj : Union[str, ObjectId, BaseMongoModel], extend_query : dict= {}, force_harddelete : bool = False):
    if isinstance(obj, str):
      d = {"_id" : ObjectId(obj)}
    elif isinstance(obj, ObjectId):
      d = {"_id" : obj}
    else:
      d = obj.dict()
    d.update(self.get_parsed_query(extend_query))
    softdelete = self.softdelete() and not force_harddelete
    db = await self.__mongo
    if self.has_hooks("pre_remove","post_remove"):
      ret = db.find_one(d)
      if not ret:
        raise NotFoundException()
      ret = self.execute_hooks("pre_init", ret)
      x = self.model(**ret)
      x = self.execute_hooks("post_init", x)
      x = self.execute_hooks("pre_remove", x, softdelete=softdelete)
    if softdelete:
      rsp = db.find_one_and_update(d, {"$set": {self.softdelete(): True}})
    else:
      rsp = db.delete_one(d)
    if self.has_hooks("post_remove"):
      self.execute_hooks("post_remove", x, softdelete=softdelete)
    return rsp
