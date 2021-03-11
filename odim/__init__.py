''' ORM and ODM tool for FastApi simplification. It enables the user to define only 1 PyDantic models and work with
 data on multiple sources '''
import enum
import inspect
from enum import Enum
from typing import Any, List, Optional, TypeVar, Union, Generic
from odim.helper import awaited

from pydantic import BaseModel, Field, root_validator
from pydantic.generics import GenericModel
from datetime import datetime
from odim.helper import get_config, get_connection_info, get_connector_for_model

all_json_encoders = {
  datetime: lambda dt: dt.isoformat()
}

T = TypeVar('T')

class SearchParams(BaseModel):
  ''' Describes how to search the details '''
  offset : int = 0
  limit : int = 25
  sort : Optional[str] = Field(default=None, description="Order by field list, separated by comma with - signifying descending order. e.g. name,-created_at  will order by name ASC and created_at DESC", regex="[,a-zA-Z0-9_-]*")


class SearchResponse(GenericModel, Generic[T]):
  search : dict = Field(description="The search data that was performed")
  total : int =  Field(description="The total number of results")
  results : List[T]

  class Config:
    json_encoders = all_json_encoders


class OkResponse(BaseModel):
  ok : bool = Field(default=True)


class Operation(Enum):
  exact = "__is"
  isnot = "__not"
  contains = "__contains"
  gt = "__gt"
  gte = "__gte"
  lt = "__lt"
  lte = "__lte"
  null = "__null"

def parse_fieldop(field):
  for o in Operation:
    if field.endswith(o.value):
      return field.replace(o.value, ""), o
  return field, Operation.exact


class BaseOdimModel(BaseModel):

  @root_validator(pre=True)
  def generic_validators_pre(cls, values):
    if hasattr(cls, "Config") and hasattr(cls.Config, "odim_hooks"):
      for fnc in cls.Config.odim_hooks.get("pre_validate",[]):
        values = awaited(fnc(cls, values))
    return values

  @root_validator()
  def generic_validators_post(cls, values):
    if hasattr(cls, "Config") and hasattr(cls.Config, "odim_hooks"):
      for fnc in cls.Config.odim_hooks.get("post_validate",[]):
        values = awaited(fnc(cls, values))
    return values


  async def save(self, *args, **kwargs):
    return await Odim(self).save(*args, **kwargs)

  async def update(self, *args, **kwargs):
    return await Odim(self).update(*args, **kwargs)

  async def delete(self, force_harddelete = False):
    return await Odim(self).delete(force_harddelete = force_harddelete)

  @classmethod
  async def find(cls, *args, **kwargs):
    return await Odim(cls).find(*args, **kwargs)

  @classmethod
  async def count(cls, *args, **kwargs):
    return await Odim(cls).count(*args, **kwargs)

  @classmethod
  async def get(cls, *args, **kwargs):
    return await Odim(cls).get(*args, **kwargs)

  @classmethod
  def add_hook(cls, hook_type, fnc):
    if not hasattr(cls, "Config"):
      setattr(cls, 'Config', type('class', (), {}))
    if not hasattr(cls.Config, "odim_hooks"):
      cls.Config.odim_hooks = {"pre_init":[], "post_init":[], "pre_save":[], "post_save":[],"pre_remove":[],"post_remove":[],"pre_validate":[],"post_validate":[]}
    if fnc not in cls.Config.odim_hooks[hook_type]:
      cls.Config.odim_hooks[hook_type].append(fnc)

  def __str__(self):
    if hasattr(self, 'id'):
      return "%s<%s>" % (type(self).__name__, self.id)
    else:
      return "%s<???>" % type(self).__name__

  def __repr__(self):
    return self.__str__()


class Odim(object):
  ''' Initiates the wrapper to communicate with backends based on the pydantic model Config metaclass '''
  protocols = []
  model = None
  instance = None

  def __new__(cls, model):
    odimclass = get_connector_for_model(model)
    return super(Odim, cls).__new__(odimclass)


  def __init__(self, model : Union[BaseModel, BaseModel.__class__]):
    if inspect.isclass(model):
      self.model = model
    else:
      self.model = model.__class__
      self.instance = model


  def get_connection_identifier(self):
    if hasattr(self.model, 'Config'):
      if hasattr(self.model.Config, 'db_name'):
        return self.model.Config.db_name
      if hasattr(self.model.Config, 'db_uri'):
        return self.model.Config.db_uri
    for key in get_config('DATABASES', default={}).keys():
      cp = get_connection_info(key)
      if cp.protocol in self.protocols:
        return key
    raise AttributeError("missing database definition")

  def softdelete(self):
    if hasattr(self.model, 'Config') and hasattr(self.model.Config, 'softdelete'):
      return getattr(self.model.Config,'softdelete')

  def execute_hooks(self, hook_type, obj, *args, **kwargs):
    if hasattr(self.model, "Config") and hasattr(self.model.Config, "odim_hooks"):
      for fnc in self.model.Config.odim_hooks.get(hook_type,[]):
        obj2 = awaited(fnc(self.model, obj, *args, **kwargs))
        if obj2!=None:
          obj = obj2
    return obj


  def has_hooks(self, *hook_types):
    ''' Whether there are hooks '''
    for hk in hook_types:
      if hasattr(self.model, "Config") and hasattr(self.model.Config, "odim_hooks") and len(self.model.Config.odim_hooks.get(hk, []))>0:
        return True
    return False


  async def save(self, extend_query : dict= {}, include_deleted : bool = False):
    ''' Saves the document and returns its identifier '''
    raise NotImplementedError("Method not implemented for this connector")


  async def update(self, extend_query : dict= {}, include_deleted : bool = False, only_fields : Optional[List['str']] = None):
    ''' Saves only the changed fields leaving other fields alone '''
    raise NotImplementedError("Method not implemented for this connector")


  async def get(self, id : str, extend_query : dict= {}, include_deleted : bool = False):
    '''
    Retrieves the document by its id
    :param id: id of the docuemnt
    :param extend_query additional search limiters:
    :return: the document as pydantic instance
    '''
    raise NotImplementedError("Method not implemented for this connector")


  def parse_query_operations(self, query : dict):
    ''' Gets the normalized search operations from the query fields '''
    rsp = {}
    for k, v in query.items():
      key, op = parse_fieldop(k)
      rsp[key] = (op, v)
    return rsp


  async def find(self, query : dict, params : SearchParams = None, include_deleted : bool = False):
    ''' Performs search using a dictionary qury to find documents on that particular collection/table
    :param query: dictionary of field:value pairs
    :param params: additional search params like ordering and limit offset
    :return: the list of documents as per pydantic type    '''
    raise NotImplementedError("Method not implemented for this connector")


  async def count(self, query : dict, include_deleted : bool = False) -> int:
    ''' Do the search and count the documents

    :param query: dictionary of field:value pairs
    :return: the number of results '''
    raise NotImplementedError("Method not implemented for this connector")


  async def delete(self, obj : str, extend_query = {}, force_harddelete : bool = False):
    ''' Delete the document from storage '''
    raise NotImplementedError("Method not implemented for this connector")



class NotFoundException(Exception):
  pass


class hook(object):
  ''' Decorator that connects function as a hook
  :param hook_type: individual or list of signal types
  :param sender: individual or list of Odim model classes to hook to

  @hook([post_save,post_remove], [Class1,Class2])
  def notify_user(sender, instance, *args, **kwargs):
     ...
  '''
  def __init__(self, hook_type, sender, **kwargs):
    self.hook_type = hook_type if isinstance(hook_type, (list, tuple)) else [ hook_type ]
    self.sender = sender if isinstance(sender, (list, tuple)) else [ sender ]


  def __call__(self, func):
    self.func = func
    for cls in self.sender:
      for ht in self.hook_type:
        cls.add_hook(ht, func)
    decorator_self = self
    return func


