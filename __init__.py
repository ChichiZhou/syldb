from syldb.core.database import Database
from syldb.core import SerializedInterface
import base64


def _decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]


def __encode(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)


class Engine(SerializedInterface):
    def __init__(self, db_name=None, format_type='dict', path='db.data'):
        self.path = path
        self.__database_objs = {}  # 数据库映射表
        self.__database_names = []  # 数据库名称的集合
        self.__format_type = format_type  # 数据库返回格式
        self.__current_db = None  # 标记当前使用的数据库

        if db_name is not None:
            self.select_db(db_name)

    def create_database(self, name):
        if name in self.__database_names:
            raise Exception('Database %s exist' % name)

        self.__database_names.append(name)
        self.__database_objs[name] = Database(name)

    def drop_database(self, name):
        if name not in self.__database_names:
            raise Exception('Database %s does not exist' % name)

        self.__database_names.remove(name)
        self.__database_objs.pop(name, True)

    def select_db(self, db_name):
        if db_name not in self.__database_names:
            raise Exception('Database %s does not exist' % db_name)
        self.__current_db = self.__database_objs[db_name]

    def serialized(self):
        return SerializedInterface.json.dumps([
            database.serialized() for database in self.__database_objs.values()
        ])

    def __dump_database(self):
        with open(self.path, 'wb') as f:
            content = _encode_db(self.serialized())
            f.write(content)

    def deserialized(self, content):
        data = SerializedInterface.json.loads(content)

        for obj in data:
            database = Database.deserialized(obj)

            db_name = database.get_name()

            self.__database_names.append(db_name)

            self.__database_objs[db_name] = database

    def __load_databases(self):
        if not os.path.exist(self.path):
            return

        with open(self.path, 'rb') as f:
            content = f.read()

        if content:
            self.deserialized(_decode_db(content))

    def commit(self):
        self.__dump_database()

    def rollback(self):
        self.__load_databases()

    def create_table(self, name, **options):
        self.__check_is_choose()
        self.__current_db.create_table(name, **options)

    def insert(self, table_name, **data):
        return self.__get_table(table_name).insert(**data)

    def search(self, table_name, fields='*', sort='ASC', **conditions):
        return self.__get_table(table_name).search(
            fields=fields,
            sort=sort,
            format_type=self.__format_type,
            **conditions
        )

    def __get_table(self, table_name):
        self.__check_is_choose()

        table = self.__current_db.get_table_obj(table_name)

        if table is None:
            raise Exception('not table %s' % table_name)

        return table

    def get_tables(self, table_name, format_type="list"):
        self.__check_is_choose()
        tables = self.__current_db.get_table()

        if format_type == 'dict':
            tmp = []
            for table in tables:
                tmp.append({'name': table})
            tables = tmp
        return tables

    def __check_is_choose(self):
        if self.__current_db is None:
            raise Exception('current db is None, please choose a db first')
