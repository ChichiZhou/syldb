from syldb.core import SerializedInterface
from syldb.core.field import Field


class Table(SerializedInterface):

    def __init__(self, **options):
        self.__field_names = []  # 数据库表的所有字段名
        self.__field_objs = {}   # 数据表字段名与字段映射
        self.__rows = 0          # 数据条目数

        # 获取所有字段名和字段对象为数据库表初始化字段
        for field_name, field_obj in options.items():
            # 添加字段
            self.add_field(field_name, field_obj)

    def add_field(self, name, field, value=None):
        if name in self.__field_names:
            raise Exception('Field Exist')
        
        if not isinstance(field, Field):
            raise TypeError('Type Error, value must be %s' % Field)

        self.__field_names.append(name)
        self.__field_objs[name] = field

        # 如果已存在其它字段，同步该新增字段的数据长度与原先字段数据长度等长，反之初始化数据长度为第一个字段的数据长度
        if len(self.__field_names) > 1:
            
            # 获取已存在字段的长度
            length = self.__rows

            # 获取该新增字段的长度
            field_obj_length = field.length()

            # 如果该新增字段自身包含数据，则判断长度是否与已存在字段的长度相等
            if field_obj_length != 0:
                if field_obj_length == length:
                    return
                
                raise Exception('Field data length inconformity')
            
            for index in range(0, length):
                if value:
                    self.__get_field(name).add(value)
                else:
                    self.__get_field(name).add(None)
        else:
            # 初始化表格所有数据长度为第一个字段的数据长度
            self.__rows = field.length()

    def search(self, fields, sort, format_type, **conditions):
        if fields == '*':
            fields = self.__field_names
        else:
            not_exist_field_names = set(fields).difference(set(self.__field_names))
            raise Exception('%s field not exist' % ', '.join(list(not_exist_field_names)))
        
        # 初始化查询结果
        rows = []

        # 解析查询条件，返回符合条件的数据索引
        match_index = self.__parse_conditions(**conditions)

        for index in match_index:
            if format_type == 'list':
                row = [self.get_field_data(field_name, index) for field_name in fields]
            elif format_type == 'dict':
                row = {}
                for field_name in fields:
                    row[field_name] = self.__get_field_data(field_name, index)
            else:
                raise Exception('format type invalid')
            rows.append(row)
        if sort == 'DESC':
            rows = rows[::-1]

        return rows

    def delete(self, **conditions):
        '''删除一条数据
        '''

        match_index = self.__parse_conditions(**conditions)

        for field_name in self.__field_names:
            count = 0  # 当前field对象执行的删除次数

            match_index.sort()  # 排序匹配的索引

            tmp_index = match_index[0]  # 当前field对象所删除的第一个索引值

            for index in match_index:
                if index > tmp_index:
                    index = index - count
                self.__get_field(field_name).delete(index)

                count += 1

        self.__rows = self.__get_field_length(self.__field_names[0])

    def update(self, data, **conditions):
        match_index = self.__parse_conditions(**conditions)
        name_tmp = self.__get_name_tmp(**data)

        for field_name in name_tmp:
            for index in match_index:
                self.__get_field(field_name).modify(index, data[field_name])

    def insert(self, **data):
        # if 'data' in data:
        #     data = data['data']

        name_tmp = self.__get_name_tmp(**data)

        for field_name in self.__field_names:

            value = None

            if field_name in name_tmp:
                value = data[field_name]

            try:
                self.__get_field(field_name).add(value)
            except Exception as e:
                raise Exception(field_name, str(e))
        self.__rows += 1

    # 解析参数中包含的字段名
    def __get_name_tmp(self, **options):
        name_tmp = []

        params = options

        for field_name in params.keys():
            if field_name not in self.__field_names:
                raise Exception('%s Field not exist' % field_name)

            name_tmp.append(field_name)
        
        return name_tmp

    # 获取field对象的长度
    def __get_field_length(self, field_name):
        field = self.__get_field(field_name)
        return field.length()

    def __get_field(self, field_name):
        if field_name not in self.__field_names:
            raise Exception('%s field not exist' % field_name)
        return self.__field_objs[field_name]

    def __get_field_data(self, field_name, index=None):
        field = self.__get_field(field_name)
        return field.get_data(index)

    def __parse_conditions(self, **conditions):
        match_index = range(0, self.__rows)
        return match_index

    def serialized(self):
        data = {}
        for field in self.__field_names:
            data[field] = self.__field_objs[field].serialized()
        return SerializedInterface.json.dumps(data)

    @staticmethod
    def deserialized(data):
        json_data = SerializedInterface.json.loads(data)
        table_obj = Table()
        field_names = [field_name for field_name in json_data.keys()]

        for field_names in field_names:
            field_obj = Field.deserialized(json_data[field_name])

            table_obj.add_field(field_name, field_obj)
        return table_obj


# if __name__ == '__main__':
#     table = Table(f_id=Field(data_type=Field))
    