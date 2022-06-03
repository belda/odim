'''
This module holds the ModelFactory generated pydantic instances
'''
from datetime import datetime
from typing import Optional, Union
from pydantic import Field

from libs.odim.odim.mongo import ObjectId

v= None

used_model_names = {}