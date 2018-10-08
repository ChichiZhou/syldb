# 核心模块

import json
from enum import Enum


class SerializedInterface:
    json = json

    @staticmethod
    def deserialized(obj):
        raise NotImplementedError

    def serialized(self):
        raise NotImplementedError


# 字段类型枚举
class FieldType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT = float = 'float'

TYPE_MAP = {
    'int': int,
    'float': float,
    'str': str,
    'INT': int,
    'FLOAT': float,
    'VARCHAR': str
}


# 字段主键
class FieldKey(Enum):
    PRIMARY = 'PRIMARY KEY',
    INCREMENT = 'AUTO_INCREMENT'
    UNIQUE = 'UNIQUE'
    NOT_NULL = 'NOT NULL'
    NULL = 'NULL'
