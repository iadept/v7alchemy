from __future__ import annotations

import copy
import pyodbc
from enum import Enum

class QueryException(Exception):
    pass

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
        self.join = join

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

    def __ne__(self, other):
        return WhereType.NEQ, self, other

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
                    print(cls)
                    value.parent = cls


class Table(metaclass=MetaTable):

    prefix: str = None
    index: int = None
    name: str = None


class WhereType(Enum):
    EQUAL = "="
    NEQ = "<>"
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "IN"
    NULL_IS = "IS NULL"
    NULL_NOT = "IS NOT NULL"


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

    @property
    def sql(self):
        if self.right is None:
            if self.where_type == WhereType.EQUAL:
                return "%s IS NULL" % self.left.sql_name, None
            if self.where_type == WhereType.NEQ:
                return "%s IS NOT NULL" % self.left.sql_name, None
            else:
                raise AttributeError
        if self.where_type == WhereType.IN:
            return "%s IN ?" % self.left.sql_name, self.right

        return " %s %s ?" % (self.left.sql_name, self.where_type.value), self.right


class JoinType(Enum):
    LEFT_OUTER = "LEFT OUTER"
    RIGHT_OUTER = "RIGHT OUTER"
    INNER = "INNER"


class Join:

    def __init__(self, join_type: JoinType, join_field: Field, field: Field):
        self.join_type: JoinType = join_type
        if type(join_field) == str:
            # TODO
            pass
        else:
            self.join_field = join_field
        self.field = field

    def __str__(self):

        if self.join_field.parent.name:
            table = "%s AS %s" % (self.join_field.parent.table, self.join_field.parent.name)
            field = "%s.%s" % (self.join_field.parent.name, self.join_field.cell)
        else:
            table = self.join_field.parent.table
            field = "%s.%s" % (self.join_field.parent.table, self.join_field.cell)

        return "%s JOIN %s ON %s = %s.%s" % (
            self.join_type.value, table, field, self.field.parent.table, self.field.cell
        )


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
        self.__joins = []
        self.__conditions: [Where] = []

        for cell in cells:
            try:
                if issubclass(cell, Table):
                    for field_name in cell.__dict__:
                        self.__add_cell(cell.__dict__[field_name])
            except Exception as e:
                pass
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
        self.__joins.append(Join(JoinType.LEFT_OUTER, field.join, field))
        return self

    def inner(self, field1, field2):
        self.__joins.append(Join(JoinType.INNER, field1, field2))
        return self

    def right_outer(self, field1, field2):
        self.__joins.append(Join(JoinType.RIGHT_OUTER, field1, field2))
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

    def __join_str(self, extends, table_name=None):
        if len(extends) == 0:
            return table_name
        if len(extends) == 1:
            extend = extends[0]
            first = table_name
        elif len(extends) > 1:
            extend = extends[-1]
            first = self.__join_str(extends[0:-1], table_name)
        return "(%s %s) " % (first, extend)

    def __query(self):
        result = "SELECT %s FROM " % (','.join(map(lambda x: x.sql_name, self.__select_fields)))

        result = result + self.__join_str(self.__joins, self.__main_table.table)
        if len(self.__conditions) > 0:
            result = result + " WHERE "
        filter_fields = []
        filter_args = []
        for where in self.__conditions:
            where_str, where_arg = where.sql
            filter_fields.append(where_str)
            if where_arg:
                filter_args.append(where_arg)
        result = result + " AND ".join(filter_fields)
        return result, filter_args

    def all(self):
        query, args = self.__query()
        records = []
        #print(query, args)
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

    def one(self):
        query, args = self.__query()
        records = []
        line = next(self.__engine._run(query, args))
        record = {}
        for i, field in enumerate(self.__select_fields):
            value = line[i]
            if type(value) == str:
                record[field.human_name] = line[i].encode("cp866").decode("cp1251")
            else:
                record[field.human_name] = line[i]
        records.append(record)
        return record

    def dict(self, field_id: Field):
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
        try:
            cursor = self.reader.cursor()
            print(query_str)


            cursor.execute(query_str, *args)
            for line in cursor.fetchall():
                yield line
        except pyodbc.Error:
            print("Ошибка в запросе!")
            print("Запрос:", query_str)
            print("Аргументы:", args)

        finally:
            cursor.close()
