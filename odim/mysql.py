import logging
import re
from enum import Enum
from typing import Union

import aiomysql.cursors
from pydantic import BaseModel
from pymysql import escape_string
from pymysql.converters import escape_bytes_prefixed, escape_item

import settings
from odim import NotFoundException, Odim, Operation, SearchParams, get_connection_info

log = logging.getLogger("uvicorn")
pools = {}


async def connected_pool(db):
  global pools
  if not db in pools:
    cn = get_connection_info(db)
    if not cn.port:
      cn.port = 3306
    pools[db] =  await aiomysql.create_pool(host=cn.host, port=cn.port, user=cn.username, password=cn.password,
                                            db=cn.db, cursorclass=aiomysql.cursors.DictCursor)
  return pools[db]


class Op(Enum):
  execute = 0
  fetchone = 1
  fetchall = 2


async def execute_sql(db, sql, co : Op = Op.execute):
  log.info(sql)
  pool = await connected_pool(db)
  async with pool.acquire() as conn:
    cursor = await conn.cursor()
    executed = await cursor.execute(sql)
    if co == Op.fetchone:
      return await cursor.fetchone()
    elif co == Op.fetchall:
      return await cursor.fetchall()
    else: #execute
      await conn.commit()
      return cursor

    #TODO handle disconnects and ends
    #TODO pymysql.err.OperationalError



class BaseMysqlModel(BaseModel):
  pass


class OdimMysql(Odim):
  protocols = ["mysql"]

  def escape(self, obj):
    """ Escape whatever value you pass to it"""
    if isinstance(obj, str):
      return "'" + escape_string(obj) + "'"
    if isinstance(obj, bytes):
      return escape_bytes_prefixed(obj)
    return escape_item(obj, getattr(self.model.Config, 'charset', 'utf-8'))


  def get_table_name(self):
    ci = self.get_connection_identifier()
    if hasattr(self.model, 'Config'):
      if hasattr(self.model.Config, 'table_name'):
        cn = self.model.Config.table_name
        return ci, cn
    return ci, self.model.__class__.__name__


  async def get(self, id : str, **kwargs):
    '''
    Retrieves the document by its id
    :param id: id of the docuemnt
    :param kwargs:
    :return: the document as pydantic instance '''
    #TODO just the desired fields
    db, table = self.get_table_name()
    rsp = await execute_sql(db, "SELECT * FROM %s WHERE id=%s" % (escape_string(table), self.escape(id)), Op.fetchone)
    if not rsp:
      raise NotFoundException()
    return self.model(**rsp)


  async def save(self):
    ''' Saves the document and returns its identifier '''
    db, table = self.get_table_name()
    do = self.instance.dict(by_alias=True)
    inss = []
    for k, v in do.items():
      if not re.match("[a-zA-Z0-9_]+", k):
        raise AttributeError("Writing a non ASCII field name")
      if k!="id":
        inss.append( "`"+k+"`="+str(self.escape(v)) )
    upff = ",".join(inss)
    if self.instance.id in (None, ""):
      rsp = await execute_sql(db, "INSERT INTO %s SET %s" % (escape_string(table), upff), Op.execute)
      self.instance.id = rsp.lastrowid
      return rsp.lastrowid
    else:
      idf = "`id`="+str(self.escape(self.instance.id))
      sql = "INSERT INTO %s SET %s ON DUPLICATE KEY UPDATE %s  " % ( escape_string(table), idf+","+upff, upff )
      rsp = await execute_sql(db, sql, Op.execute)
      return self.instance.id


  async def update(self):
    ''' Updates just the partial document '''
    db, table = self.get_table_name()
    dd = self.instance.dict(exclude_unset=True)
    dd_id = dd["id"]
    del dd["id"]
    updates = []
    for k,v in dd.items():
      updates.append( "`"+k+"`="+str(self.escape(v)) )
    sql = "UPDATE %s SET %s WHERE id=%s" % (escape_string(table), " , ".join(updates), self.escape(dd_id))
    #TODO detect not found
    await execute_sql(db, sql, Op.execute)


  def get_where(self, query):
    whr = []
    for k, (op, v) in self.parse_query_operations(query).items():
      if not re.match("[a-zA-Z0-9_]+", k):
        raise AttributeError("Searching on a non ASCII field name")
      if op == Operation.exact:
        whr.append( "`"+k+"`="+str(self.escape(v)) )
      elif op == Operation.isnot:
        whr.append( "`"+k+"`!="+str(self.escape(v)) )
      elif op == Operation.contains:
        whr.append( "`"+k+"` LIKE '%"+str(escape_string(v)+"%'") )
      elif op == Operation.gt:
        whr.append( "`"+k+"` > "+str(self.escape(v)) )
      elif op == Operation.gte:
        whr.append( "`"+k+"` >= "+str(self.escape(v)) )
      elif op == Operation.lt:
        whr.append( "`"+k+"` < "+str(self.escape(v)) )
      elif op == Operation.lte:
        whr.append( "`"+k+"` <= "+str(self.escape(v)) )
      elif op == Operation.null:
        if v:
          whr.append( "`"+k+"` IS NULL" )
        else:
          whr.append( "`"+k+"` IS NOT NULL" )
    return  "1" if len(whr) == 0  else " AND ".join(whr)


  async def find(self, query : dict, params : SearchParams = None):
    ''' Performs search using a dictionary qury to find documents on that particular collection/table
    :param query: dictionary of field:value pairs
    :param params: additional search params like ordering and limit offset
    :return: the list of documents as per pydantic type    '''
    db, table = self.get_table_name()
    where = self.get_where(query)
    sql_params = ""
    if params:
      if params.sort not in (None, ''):
        sql_params+= " ORDER BY "
        paramslist = []
        for x in params.sort.split(","):
          paramslist.append( (x[1:]+" DESC ") if x[0] == "-" else (x+" ASC ") )
        sql_params+= ",".join(paramslist)
      if params.limit:
        sql_params+= " LIMIT "+str(params.limit)
      if params.offset:
        sql_params+= " OFFSET "+str(params.offset)
    rsp = await execute_sql(db, "SELECT * FROM %s WHERE %s %s" % (escape_string(table), where, sql_params), Op.fetchall)
    return [ self.model(**row) for row in rsp ]


  async def count(self, query : dict) -> int:
    ''' Do the search and count the documents
    :param query: dictionary of field:value pairs
    :return: the number of results '''
    db, table = self.get_table_name()
    where = self.get_where(query)
    rsp = await execute_sql(db, "SELECT COUNT(*) as cnt FROM %s WHERE %s" % (escape_string(table), where), Op.fetchone)
    return rsp["cnt"]


  async def delete(self, obj : Union[str, int, BaseModel]):
    ''' Delete the document from storage '''
    db, table = self.get_table_name()
    id = obj if not isinstance(obj, BaseModel) else obj.id
    await execute_sql(db, "DELETE FROM %s WHERE id=%s" % (escape_string(table), self.escape(id)), Op.execute)
    #TODO detect not found


