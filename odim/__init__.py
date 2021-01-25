''' ORM and ODM tool for FastApi simplification. It enables the user to define only 1 PyDantic models and work with
 data on multiple sources '''
import inspect
from enum import Enum
from typing import List, Optional, TypeVar, Union, Generic

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

from odim.helper import get_config, get_connection_info, get_connector_for_model

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



  async def save(self):
    ''' Saves the document and returns its identifier '''
    raise NotImplementedError("Method not implemented for this connector")


  async def update(self):
    ''' Saves only the changed fields leaving other fields alone '''
    raise NotImplementedError("Method not implemented for this connector")


  async def get(self, id : str, **kwargs):
    '''
    Retrieves the document by its id
    :param id: id of the docuemnt
    :param kwargs:
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


  async def find(self, query : dict, params : SearchParams = None):
    ''' Performs search using a dictionary qury to find documents on that particular collection/table
    :param query: dictionary of field:value pairs
    :param params: additional search params like ordering and limit offset
    :return: the list of documents as per pydantic type    '''
    raise NotImplementedError("Method not implemented for this connector")


  async def count(self, query : dict) -> int:
    ''' Do the search and count the documents

    :param query: dictionary of field:value pairs
    :return: the number of results '''
    raise NotImplementedError("Method not implemented for this connector")


  async def delete(self, obj : str):
    ''' Delete the document from storage '''
    raise NotImplementedError("Method not implemented for this connector")



class NotFoundException(Exception):
  pass