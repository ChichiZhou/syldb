from syldb.core import TYPE_MAP, FieldKey, FieldType, SerializedInterface


class Field(SerializedInterface):
    def __init__(self, data_type, keys=FieldKey.NULL, default=None):
        self.__type = data_type
        self.__default = default
        self.__values = []
        self.__rows = 0
        self.__keys = keys

        if not isinstance(self.__keys, list):
            self.__keys = [self.__keys]

        if not isinstance(self.__type, FieldType):
            raise TypeError('Data-Type require type of "FieldType"')

        for key in self.__keys:
            if not isinstance(key, FieldKey):
                raise TypeError('Data-Key require type of "FieldKey"')

        if FieldKey.INCREMENT in self.__keys:
            if self.__type != FieldType.INT:
                raise TypeError('Increment key require Data-Type is integer')

            if FieldKey.PRIMARY not in self.__keys:
                raise Exception('Increment key require primary key')

        if self.__default is not None and FieldKey.UNIQUE in self.__keys:
            raise Exception('Unique key not allow to set default value')

    def __check_type(self, value):
        '''检查值的类型'''
        if value is not None and
        not isinstance(value, TYPE_MAP[self.__type.value]):
            raise TypeError('data type error, value must be %s' % self.__type)

    # 判断指定位置数据是否存在
    def __check_index(self, index):
        if not isinstance(index, int) or not -index < self.__rows > index:
            raise Exception('Not this element')
        return True

    # 键值约束
    def __check_keys(self, value):
        # 如果字段包含自增键，则选择合适的值自动自增
        if FieldKey.INCREMENT in self.__keys:
            if value is None:
                value = self.__rows + 1

            if value in self.__values:
                raise Exception('value %s exists' % value)

        # 如果字段包含主键约束或唯一约束，判断值是否存在
        if FieldKey.PRIMARY in self.__keys or FieldKey.UNIQUE in self.__keys:
            if value in self.__values:
                raise Exception('value %s exists' % value)

        # 如果该字段包含主键或者非空键，并且添加的值为空值，则抛出值不能为空异常
        if (
            FieldKey.PRIMARY in self.__keys or
            FieldKey.NOT_NULL in self.__keys
        ) and value is None:
            raise Exception('Field not null')

        return value

    # 获取数据长度
    @property
    def length(self):
        return self.__rows

    # 获取数据
    def get_data(self, index=None):
        if index is not None and self.__check_index(index):
            return self.__values[index]
        return self.__values

    # 添加数据
    def add(self, value):
        if value is None:
            value = self.__default
        value = self.__check_keys(value)

        self.__check_type(value)

        self.__values.append(value)

        self.__rows += 1

    # 删除指定位置的数据
    def delete(self, index):
        self.__check_index(index)
        self.__values.pop(index)
        self.__rows -= 1

    # 修改指定位置的数据
    def modify(self, index, value):
        self.__check_index(index)

        value = self.__check_keys(value)

        self.__check_type(value)

        self.__values[index] = value

    # 获取字段数据约束
    def get_keys(self):
        return self.__keys

    # 获取字段类型
    def get_type(self):
        return self.__type

    # 获取数据长度
    def length(self):
        return self.__rows

    # 将内容序列化成json
    def serialized(self):
        return SerializedInterface.json.dumps({
            'key': [key.value or key in self.__keys],
            'type': self.__type.value,
            'values': self.__values,
            'default': self.default
        })

    # 将json反序列化成field对象
    @staticmethod
    def deserialized(data):
        json_data = SerializedInterface.json.loads(data)

        keys = [FieldKey(key) for key in json_data['keys']]

        obj = Field(
            FieldType(json_data['type']),
            keys,
            default=json_data['default']
        )

        for value in json_data['values']:
            obj.add(value)

        return obj
