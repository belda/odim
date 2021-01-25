'''
Contains the extended FastAPI router, for simplified CRUD from a model
'''
from typing import List, Optional, Sequence, Set, Type, Union

import fastapi
from fastapi import Depends, params
from pydantic import BaseModel

from odim import Odim, SearchResponse
from odim.dependencies import SearchParams


class OdimRouter(fastapi.APIRouter):
  ''' Simplified FastAPI router for easy CRUD '''

  def mount_crud(self,
                 path: str,
                 *,
                 model : Type[BaseModel],
                 tags: Optional[List[str]] = None,
                 dependencies : Optional[Sequence[params.Depends]] = None,
                 include_in_schema: bool = True,
                 methods : Optional[Union[Set[str], List[str]]] = ('create','get','search','save','update','delete'),
                 methods_exclude : Optional[Union[Set[str], List[str]]] = []):
    ''' Add endpoints for CRUD operations for particular model
    :param path: base_path, for the model resource location eg: /api/houses/
    :param model: pydantic/Odim BaseModel, that is used for eg. Houses
    :param tags: Starlette/FastAPI tags for endpoints
    :param dependencies: Starlette/FastAPI dependencies for all endpoints
    :param include_in_schema: whether to include in docs
    :param methods: methods to automatically generate ('create','get','search','save','update','delete')
    :param methods_exclude: methods to NOT automatically generate ('create','get','search','save','update','delete')
    '''
    add_methods = [ x for x in methods if x not in methods_exclude ]

    if 'create' in add_methods:
      async def create(obj : model):
        await Odim(obj).save()
        return obj
      self.add_api_route(path = path,
                         endpoint=create,
                         response_model=model,
                         status_code=201,
                         tags=tags,
                         dependencies = dependencies,
                         summary="Create new %ss" % model.schema().get('title'),
                         description = "Create new instance of %s " %  model.schema().get('title'),
                         methods = ["POST"],
                         include_in_schema = include_in_schema)

    if 'get' in add_methods:
      async def get(id : str):
        return await Odim(model).get(id=id)
      self.add_api_route(path = path+"{id}",
                         endpoint=get,
                         response_model=model,
                         tags=tags,
                         dependencies = dependencies,
                         summary="Get %s" % model.schema().get('title'),
                         description = "Return individual %s details " % model.schema().get('title'),
                         methods = ["GET"],
                         include_in_schema = include_in_schema)

    if 'search' in add_methods:
      async def search(search_params : dict = Depends(SearchParams)):
        rsp = { "results" : await Odim(model).find(search_params.q, search_params),
                "total" : await Odim(model).count(search_params.q),
                "search" : search_params.dict()}
        return rsp
      self.add_api_route(path = path,
                         endpoint=search,
                         response_model=SearchResponse[model],
                         tags=tags,
                         dependencies = dependencies,
                         summary="Search for %ss" % model.schema().get('title'),
                         description = "Performs a listing search for %s " %  model.schema().get('title'),
                         methods = ["GET"],
                         include_in_schema = include_in_schema)

    if 'save' in add_methods:
      async def save(id : str, obj : model):
        obj.id = id
        await Odim(obj).save()
        return obj
      self.add_api_route(path = path+"{id}",
                     endpoint=save,
                     response_model=model,
                     tags=tags,
                     dependencies = dependencies,
                     summary="Replace %s" % model.schema().get('title'),
                     description = "PUT replaces the original %s as whole  " %  model.schema().get('title'),
                     methods = ["PUT"],
                     include_in_schema = include_in_schema)

    if 'update' in add_methods:
      async def update(id : str, obj : model):
        obj.id = id
        await Odim(obj).update()
        return obj
      self.add_api_route(path = path+"{id}",
                     endpoint=update,
                     response_model=model,
                     tags=tags,
                     dependencies = dependencies,
                     summary="Partial update %s" % model.schema().get('title'),
                     description = "Just updates individual fields of %s " %  model.schema().get('title'),
                     methods = ["Patch"],
                     include_in_schema = include_in_schema)

    if 'delete' in add_methods:
      async def delete(id : str):
        await Odim(model).delete(id)
      self.add_api_route(path = path+"{id}",
                     endpoint=delete,
                     response_model=None,
                     status_code=204,
                     tags=tags,
                     dependencies = dependencies,
                     summary="Deletes %s" % model.schema().get('title'),
                     description = "Deletes individual instance of %s " %  model.schema().get('title'),
                     methods = ["DELETE"],
                     include_in_schema = include_in_schema)



  def generate(self,
                 path: str,
                 *,
                 model : Type[BaseModel],
                 tags: Optional[List[str]] = None,
                 dependencies : Optional[Sequence[params.Depends]] = None,
                 include_in_schema: bool = True,
                 methods : Optional[Union[Set[str], List[str]]] = ('create','get','search','save','update','delete'),
                 methods_exclude : Optional[Union[Set[str], List[str]]] = []):
    ''' Generates the code for the endpoints
    :param path: base_path, for the model resource location eg: /api/houses/
    :param model: pydantic/Odim BaseModel, that is used for eg. Houses
    :param tags: Starlette/FastAPI tags for endpoints
    :param dependencies: Starlette/FastAPI dependencies for all endpoints
    :param include_in_schema: whether to include in docs
    :param methods: methods to automatically generate ('create','get','search','save','update','delete')
    :param methods_exclude: methods to NOT automatically generate ('create','get','search','save','update','delete')
    '''
    add_methods = [ x for x in methods if x not in methods_exclude ]
    model_name = model.__name__
    other=""
    if tags:
      other+= ", tags="+str(tags)
    if dependencies:
      other+= ", dependencies="+str(dependencies)
    if not include_in_schema:
      other+= ", include_in_schema=False"

    if 'get' in add_methods:
      print(f'''
@router.get("{path}{{id}}", response_model={model_name}{other})
async def get_{model_name}(id : str):
  \'\'\' Returns the individual {model_name} details\'\'\'
  return await Odim(TestTable).get(id=id)
''')

    if 'search' in add_methods:
      print(f'''
@router.get("{path}", response_model=SearchResponse[{model_name}]{other})
async def search_{model_name}(search : dict = Depends(SearchParams)):
  rsp = {{ "results" : await Odim({model_name}).find(search.q, search),
          "total" : await Odim({model_name}).count(search.q),
          "search" : search.dict()}}
  return rsp
''')


    if 'create' in add_methods:
      print(f'''
@router.post("{path}", status_code=201, response_model={model_name}{other})
async def create_{model_name}(obj : {model_name}):
  await Odim(obj).save()
  return obj
''')

    if 'save' in add_methods:
      print(f'''
@router.put("{path}{{id}}", response_model={model_name}{other})
async def save_{model_name}(id : str, obj : {model_name}):
  obj.id = id
  await Odim(obj).save()
  return obj
''')

    if 'update' in add_methods:
      print(f'''
@router.patch("{path}{{id}}", response_model={model_name}{other})
async def update_{model_name}(id : str, obj : {model_name}):
  obj.id = id
  await Odim(obj).update()
  return obj
''')

    if 'delete' in add_methods:
      print(f'''
@router.delete("{path}{{id}}", status_code=204, response_model={model_name}{other})
async def delete_{model_name}(id : str):
  await Odim(obj).delete(id)
  return obj
''')

