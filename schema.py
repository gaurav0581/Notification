from datetime import datetime

from pydantic import BaseModel, Json,constr
from typing import Optional, List, Any
from enum import Enum
from uuid import uuid4, UUID
from typing import Set
indianmobnumber =constr(regex="^[a-z]$")


class grouptype(int,Enum):
    group = 1
    user = 0
    role = 2

class CommandMessageSchema(BaseModel):
    type : grouptype
    reciever : str
    entity : int
    data : Optional[Any]
    persistance : int