# odim
Simple Python ORM/ODM specifically designed to be used with Pydantic and FastAPI


## Simple syntax
In order to nicely work with databases you just create your Pydantic models. Odim does not care if it is 
MongoDB or SQL.


```python3
from pydantic import BaseModel

class MyModel(BaseModel):
    id : int
    field : str

    class Config:
        db_uri = "mongodb://user:pwd@10.0.0.1/db1"
        collection_name = "mymodel"
```
Then you can easily perform CRUD operations.

```python3
obj = MyModel(id=1, field="asdf 213")
await Odim(obj).save()

obj2 = await Odim(MyModel).get(123)

for x in await Odim(MyModel).find({"field" : "asdf 213"}):
  print(x)
  
await Odim(MyModel).count({"field" : 1})

```

In case you are using amazin FastAPI. We have our extended router, that gives you CRUD API endpoint

```python3
from odim.router import OdimRouter

router = OdimRouter()

router.mount_crud("/api/mymodel/", model=MyModel, tags=["mymodel"])
```

Or you can generate these API stubs with
```python3
router.generate("/api/mymodel/", model=MyModel, tags=["mymodel"])
```