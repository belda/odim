import inspect
import json
from typing import Type, Union

from pydantic import BaseModel


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
  "ObjectId" : "ObjectId"}



class ModelFactory(object):
  ''' Utility  for  generating stub code for Pydantic models based on their JSON definition and vice-versa'''

  @classmethod
  def model_to_json(cls, model : Union[str, BaseModel, BaseModel.__class__]):
    if isinstance(model, str):
      cls = get_class_by_name(model)
    elif not inspect.isclass(model):
      cls = model.__class__
    else:
      cls = model
    pydschema = cls.schema()
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