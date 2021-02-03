import inspect
import json
import random
import string
import importlib
import sys
from decimal import Decimal
from enum import Enum
from os import path, getcwd
from typing import Any, List, Optional, Type, Union
from pydantic import BaseModel, Field, create_model
from odim.mongo import BaseMongoModel, ObjectId
from datetime import datetime
from odim import dynmodels

def get_class_by_name(classname):
  ''' Returns the loaded instance of a class '''
  mod_name = classname[:classname.rindex('.')]
  module = __import__(mod_name, fromlist=[mod_name, ])
  the_class = getattr(module, classname[classname.rindex('.') + 1:])
  return the_class


MM_TYPE_MAPPING = {
  "String" : 'str',
  "Number" : 'float',
  "Boolean" : "bool",
  "Array" : "List",
  "Date" : "datetime",
  "Int" : "int",
  "Decimal" : "Decimal",
  "Decimal128" : "Decimal",
  "ObjectId" : "ObjectId",
  "Enum" : "str"}


DM_TYPE_MAPPING = {
  "String" : str,
  "Number" : int,
  "Boolean" : bool,
  "Array" : list,
  "Date" : datetime,
  "ObjectId" : ObjectId,
  "Parent": dict,
  "Int" : int,
  "Decimal" : Decimal,
  "Decimal128" : Decimal,
  "Enum" : str
}

class SEnum(str, Enum):
  pass

def encode(k, v):
  if isinstance(v, list):
    if len(v) == 0:
      return List[Any], None
    else:
      enc = encode("sub", v[0])
      return List[enc[0]], enc[1]

  elif isinstance(v, dict):
      if v.get("type") == "Parent":
        subcls = {}
        for ks,vs in v.get("child", {}).items():
          subcls[ks] = encode(ks, vs)
        m = create_model(__model_name=v.get("__title", k+(''.join(random.choices(string.ascii_uppercase + string.digits, k=6)))).capitalize(),
                         __module__ = "odim.dynmodels",
                         __base__=BaseModel,
                         **subcls)
        if "__description" in v:
          m.__doc__ = v.get("__description")
        dt = m
      elif v.get("type") == "Enum":
        subcls = {}
        for opt in v.get("options",[]):
          subcls[opt] = opt

        m = SEnum(v.get("__title", k.capitalize()+"Enum"+(''.join(random.choices(string.ascii_uppercase + string.digits, k=6)))), subcls)
        if "__description" in v:
          m.__doc__ = v.get("__description")
        dt = m
      else:
        dt = DM_TYPE_MAPPING.get(v.get("type"), str)

      field = Field(description=v.get("__description",v.get("description","")), title=v.get("__title", v.get("title")))
      if v.get("regex"):
        field.regex = v.get("regex")
      if v.get("required", False) or v.get("default", False) not in ('', False, None):
        field.default = v.get("default",...) #TODO default value removes the required attribute
        return dt, field
      else:
        field.default = v.get("default", None)
        return Optional[dt], field
  else:
    return Optional[DM_TYPE_MAPPING.get(v, str)], None


def location_tester(file_uri):
  tryfiles = [file_uri,
              path.join(getcwd(), file_uri),
              path.join(getcwd(), "models", file_uri),
              path.join(path.dirname(path.realpath(__file__)), file_uri)]
  for f in tryfiles:
    if path.exists(f):
      return f



class ModelFactory(object):
  ''' Utility  for  generating stub code for Pydantic models based on their JSON definition and vice-versa'''

  @classmethod
  def load_mongo_model(cls, class_name=None,
                       description=None,
                       db_name=None, db_uri=None,
                       database=None, collection_name=None,
                       file_uri=None, signal_file=None) -> Type[BaseMongoModel]:

    assert db_name or db_uri, "Either database_name or database_uri must be specified"
    assert database and collection_name, "database and collection_name must be set"
    if not file_uri:
      file_uri = "schemas/src/"+database+"/"+collection_name.lower() +".json"
    file = location_tester(file_uri)
    assert file, "No schema json was found."

    if not signal_file:
      signal_file = "schemas/dist/python3/odim/hooks/"+database+"/"+collection_name.lower() +".py"
    signal_file = location_tester(signal_file)

    with open(file, "r") as f:
      data = json.loads(f.read())
      newcls = {}
      for k,v in data.items():
        if k in ("__class_name","__title"):
          if not class_name:
            class_name = v
        elif k in ("__description"):
          if not description:
            description = v
        else:
          newcls[k] = encode(k, v)

      if not class_name:
        class_name = collection_name
      m = create_model(class_name,
                       __module__ = "odim.dynmodels",
                       __base__=BaseMongoModel,
                       **newcls)
      meta_attrs = {"collection_name": collection_name, **vars(BaseMongoModel.Config)}
      if db_name:
        meta_attrs["db_name"] = db_name
      if db_uri:
        meta_attrs["db_uri"] = db_uri
      if signal_file: # now handle the signals
        spec = importlib.util.spec_from_file_location("odim.dynmodels.%s.signals" % class_name, signal_file)
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo)
        for n,x in inspect.getmembers(foo, inspect.isfunction):
          if not "odim_hooks" in meta_attrs:
            meta_attrs["odim_hooks"] = {"pre_init":[], "post_init":[], "pre_save":[], "post_save":[],"pre_remove":[],"post_remove":[],"pre_validate":[],"post_validate":[]}
          meta_attrs["odim_hooks"][n].append(x)
      setattr(m, 'Config', type('class', (), meta_attrs))
      m.__doc__ = description
      m.update_forward_refs()
      return m


  @classmethod
  def model_to_json(cls, model : Union[str, BaseModel, BaseModel.__class__]):
    if isinstance(model, str):
      newcls = get_class_by_name(model)
    elif not inspect.isclass(model):
      newcls = model.__class__
    else:
      newcls = model
    pydschema = newcls.schema()
    out = {}
    for propname, vals in pydschema["properties"].items():
      out[propname] = {}
      if vals["type"] == "string" and vals.get("format") == "date-time":
        out[propname]["type"] = "Date"
      if vals["type"] == "string" and propname.endswith("_id"):
        out[propname]["type"] = "ObjectId"
      elif vals["type"] == "string":
        out[propname]["type"] = "String"
      elif vals["type"] == "boolean":
        out[propname]["type"] = "Boolean"
      elif vals["type"] in ("integer","number"):
        out[propname]["type"] = "Number"
      elif vals["type"].lower() in ("float","double",'decimal'):
        out[propname]["type"] = "Decimal128"
      elif vals["type"] == "array":
        out[propname]["type"] = "Array"

      if "default" in vals:
        out[propname]["default"] = vals["default"]
      if "description" in vals:
        out[propname]["description"] = vals["description"]
    print(json.dumps(out, indent=4))


  @classmethod
  def json_to_fields(cls, js_data):
    if js_data.endswith(".js") or js_data.endswith(".json"):
      filename = js_data[js_data.rindex("/")+1:] if "/" in js_data else js_data
      filename = filename.replace(".json","").replace(".js","")
      with open(js_data, "r") as f:
        data = json.loads(f.read())
    else:
      filename = None
      data = json.loads(js_data)

    if filename:
      print(f"class {filename}(BaseMongoModel):")
    for k,v in data.items():
      if not isinstance(v, dict):
        print(f"  {k} : Optional[{MM_TYPE_MAPPING[v]}]")
      else:
        if v.get("type") == "Number" and v.get("integer"):
          dt = "int"
        else:
          dt = MM_TYPE_MAPPING.get(v.get("type"), "str")
        if v.get("required", False) or v.get("default", False):
          dt = f"Optional[{dt}]"
        rest = ""
        if "description" in v:
          rest+= f" = Field(description='{v['description']}')"
        print(f"  {k} : {dt} {rest}")




if __name__ == "__main__":
  import os.path, sys
  sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
  if len(sys.argv) != 2:
    print("The script requires 1 parameter and that is either the package full name, or json file path")
    sys.exit()
  if sys.argv[1].endswith(".js") or sys.argv[1].endswith(".json"):
    ModelFactory.json_to_fields(sys.argv[1])
  else:
    ModelFactory.model_to_json(sys.argv[1])