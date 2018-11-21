from __future__ import annotations

import copy
import pyodbc
from enum import Enum

class Field:
    """
    Схема ячейки
    """

    def __init__(self, cell: str, join=None):
        """
        :param cell: Название столбца в файле таблицы
        :param join: Имя таблицы для привзяки
        """
        self.cell = cell
        self.join: Field = join

        self.name: str = None
        self.parent: Table = None
        self.__alias: str = None
        self.__func: str = None

    @property
    def sql_name(self):
        """

        :return:
        """
        if self.parent.name:
            value = "%s.%s" % (self.parent.name, self.cell)
        else:
            value = "%s.%s" % (self.parent.table, self.cell)
        if self.__func:
           value = self.__func % value
        return value


    @property
    def human_name(self):
        """

        :return:
        """
        if self.__alias is not None:
            return self.__alias
        return "%s.%s" % (self.parent.__name__, self.name)

    def __eq__(self, other):
        return WhereType.EQUAL, self, other

    def __lt__(self, other):
        return WhereType.LT, self, other

    def __le__(self, other):
        return WhereType.LE, self, other

    def __gt__(self, other):
        return WhereType.GT, self, other

    def __ge__(self, other):
        return WhereType.GE, self, other

    def in_(self, values):
        if type(values[0]) == str:
            return WhereType.IN, self, "(%s)" % (','.join(map(lambda x: "'%s'" % x, values)))
        return WhereType.IN, self, "(%s)" % (','.join(values))

    def alias(self, title):
        """
        Задает человеческое имя ячейки
        :param title: новое имя
        :return:
        """
        field = copy.deepcopy(self)
        field.__alias = title
        return field

    def right(self, position):
        """
        Задает человеческое имя ячейки
        :param title: новое имя
        :return:
        """
        field = copy.deepcopy(self)
        field.__func = "RIGHT(%s, " + str(position) + ")"
        return field


class MetaTable(type):

    def __new__(cls, name, bases, dict):
        prefix = None
        for base in bases:
            for field in base.__dict__:
                if field == 'prefix':
                    prefix = base.__dict__["prefix"]
                if not field.startswith("_") and not field.endswith("_"):
                    value = base.__dict__[field]
                    if not callable(value) and issubclass(value.__class__, (Field,)):
                        dict[field] = copy.deepcopy(value)
        if 'index' in dict and dict['index']:
            dict['table'] = "%s%i" % (prefix, dict['index'])
        else:
            dict['table'] = dict['prefix']
        return super(MetaTable, cls).__new__(cls, name, bases, dict)

    def __init__(cls, name, bases, dict):
        for field in dict:
            if not field.startswith("_") and not field.endswith("_"):
                value = dict[field]
                if not callable(value) and issubclass(value.__class__, (Field,)):
                    value.name = field
                    value.parent = cls


class Table(metaclass=MetaTable):

    prefix: str = None
    index: int = None
    name: str = None


class WhereType(Enum):
    EQUAL = "="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "IN"

class Where:

    def __init__(self, where_type: WhereType, left: Field, right):
        self.where_type = where_type
        if not issubclass(left.__class__, Field):
            raise Exception("Left must be Field!")
        self.left = left
        if type(right) == str:
            self.right = right.encode("cp1251").decode("cp866")
        else:
            self.right = right

    @property
    def value(self):
        if issubclass(self.right.__class__, Field):
            return self.right.sql_name
        return self.right

    def __str__(self):
        return " %s %s ?" % (self.left.sql_name, self.where_type.value)


class Select:

    def __init__(self, engine: Engine, table: Table, *cells):
        """
        Запрос
        :param engine: движок
        :param table: Главная таблица SELECT * FROM table
        :param cells: Ячейки для отбора
        """
        self.__engine = engine
        self.__main_table = table
        self.__select_fields: [Field] = []
        self.__extends = []
        self.__conditions: [Where] = []

        for cell in cells:
            try:
                if issubclass(cell, Table):
                    for field_name in cell.__dict__:
                        self.__add_cell(cell.__dict__[field_name])
            except Exception as e:
                pass
                #print(1, cell,  e)
            try:
                if issubclass(cell.__class__, Field):
                    self.__select_fields.append(cell)
            except Exception as e:
                pass

    def extend(self, field: Field):
        """
        Делает JOIN к запросу для выбранной ячейки
        :param field: Ячейка
        """
        self.__extends.append(field)
        return self

    def where(self, expression):
        self.__conditions.append(Where(*expression))
        return self

    def __extends_str(self, extends, table_name=None):
        if len(extends) == 0:
            return table_name
        if len(extends) == 1:
            extend = extends[0]
            first = table_name
        elif len(extends) > 1:
            extend = extends[-1]
            first = self.__extends_str(extends[0:-1], table_name)
        join_field = extend.join
        table = join_field.parent.table
        if join_field.parent.name:
            table = "%s as %s" % (join_field.parent.table, join_field.parent.name)
        return "(%s LEFT OUTER JOIN %s ON %s = %s) " % (first, table, extend.sql_name, join_field.sql_name)

    def __query(self):
        result = "SELECT %s FROM " % (','.join(map(lambda x: x.sql_name, self.__select_fields)))

        result = result + self.__extends_str(self.__extends, self.__main_table.table)
        if len(self.__conditions) > 0:
            result = result + " WHERE "
        filter_fields = []
        filter_args = []
        for where in self.__conditions:
            if where.where_type == WhereType.IN:
                filter_fields.append(str(where)[:-1] + where.value)
            else:
                filter_fields.append(str(where))
                filter_args.append(where.value)
        result = result + " AND ".join(filter_fields)
        return result, filter_args

    def all(self):
        """
        Возвращает все записи списком
        :return:
        """
        query, args = self.__query()
        records = []
        for line in self.__engine._run(query, args):
            record = {}
            for i, field in enumerate(self.__select_fields):
                value = line[i]
                if type(value) == str:
                    record[field.human_name] = line[i].encode("cp866").decode("cp1251")
                else:
                    record[field.human_name] = line[i]
            records.append(record)
        return records

    def dict(self, field_id: Field):
        """
        Возвращает словарь с заданным ключом
        :param field_id: поле-ключ должен быть в выборке
        :return:
        """
        query, args = self.__query()
        records = {}
        for line in self.__engine._run(query, args):
            record = {}
            record_id = None
            for i, field in enumerate(self.__select_fields):
                value = line[i]
                if type(value) == str:
                    record[field.human_name] = line[i].encode("cp866").decode("cp1251")
                else:
                    record[field.human_name] = line[i]
                if field.name == field_id.name:
                    record_id = line[i]

            records[record_id] = record
        return records


class Engine:

    def __init__(self, root):
        connect = "Driver={Microsoft dBASE Driver (*.dbf)};DefaultDir=" + root
        self.reader = pyodbc.connect(connect, autocommit=True)

    def select(self, table: Table, *cells):
        return Select(self, table, *cells)

    def _run(self, query_str: str, *args):
        print(query_str)
        cursor = self.reader.cursor()
        cursor.execute(query_str, *args)
        for line in cursor.fetchall():
            yield line
        cursor.close()
