from syldb.core import SerializedInterface
from syldb.core.table import Table


class Database(SerializedInterface):
    def __init__(self, name):
        self.__name = name
        self.__table_names = []
        self.__table_objs = {}

    def create_table(self, table_name, **options):
        if table_name in self.__table_objs:
            raise Exception('table exist')

        self.__table_names.append(table_name)
        self.__table_objs[table_name] = Table(**options)

    def drop_tables(self, table_name):
        if table_name not in self.__table_names:
            raise Exception('table not exist')

        self.__table_name.remove(table_name)

        self.__table_objs.pop(table_name, True)

    def get_table_obj(self, name):
        return self.__table_objs.get(name, None)

    def get_name(self):
        return self.__name

    def serialized(self):
        # 初始化返回数据
        data = {'name': self.__name, 'tables': []}

        # 遍历所有 Table 对象并调用对应的序列化方法
        for tb_name, tb_data in self.__table_objs.items():
            data['tables'].append(
                [tb_name, tb_data.serialized()]
            )

        # 返回 Json 字符串
        return SerializedInterface.json.dumps(data)

    # 添加数据表
    def add_table(self, table_name, table):
        # 如果数据表名字不存在，则开始添加绑定
        if table_name not in self.__table_objs:
            # 追加数据表名字到 __table_names 中
            self.__table_names.append(table_name)

            # 版定数据表名字与数据表对象
            self.__table_objs[table_name] = table

    @staticmethod
    def deserialized(obj):
        # 解析 Json 字符串为 dict 字典
        data = SerializedInterface.json.loads(obj)

        # 使用解析出来的数据库名字实例化一个 Database 对象
        obj_tmp = Database(data['name'])

        # 遍历所有 Table Json 字符串，依次调用 Table 对象的反序列化方法，再添加到刚刚实例化出来的 Database 对象中
        for table_name, table_obj in data['tables']:
            obj_tmp.add_table(table_name, Table.deserialized(table_obj))

        # 返回 Database 对象
        return obj_tmp
