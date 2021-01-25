import json
from typing import Optional
from fastapi import Query
from pydantic import BaseModel, Field, ValidationError
import re

class SearchParams:

  def __init__( self,
                q: Optional[str] = Query(None, description="Query string to search. Urlencoded key=value or JSON dictionary with fields"),
                limit: Optional[int] = Query(description="Limit the number of results to x", default=25),
                offset: Optional[int] = Query(description="From which record to start", default=0),
                sort: Optional[str] = Query(None, description="Fields to sort by. Comma separated list of fields (- determines DESC order). ", example="name,-created_at")
              ):
    self.limit = limit
    self.offset = offset
    self.sort = sort
    if q:
      try:
        self.q = json.loads(q)
      except:
        if "=" in q:
          key, value = q.split("=")
          self.q = {key : value}
        else:
          raise ValidationError("q parameter must be either a urlencoded JSON or key=value entry")
      for key in self.q:
        if not re.match("[a-zA-Z0-9_]+", key):
          raise ValidationError("The field in q param seems to be incorrect")
    else:
      self.q = {}


  def __getitem__(self, item):
    return getattr(self, item)


  def dict(self):
    return {"q":self.q, "limit":self.limit, "offset":self.offset, "sort":self.sort}

  def __str__(self):
    return json.dumps(self.dict())