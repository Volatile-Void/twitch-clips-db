import datetime

def format_sql_value(value):
    if value is None:
        return 'null'
    elif isinstance(value, bool):
        value = int(value)
    elif isinstance(value, datetime.datetime):
        value = value.strftime('%Y-%m-%dT%H:%M:%S.%f')
    elif isinstance(value, datetime.date):
        value = value.strftime('%Y-%m-%d')
    return repr(value)

class Column:
    def __init__(self, name, data_type, constraint=None):
        self.name = name
        self.data_type = data_type
        self.constraint = constraint

    def create_column(self):
        return '%s %s%s' % (self.name, self.data_type, ' %s' % self.constraint if self.constraint else '')

class Constraint:
    def create_constraint(self):
        raise NotImplementedError()

class PrimaryKeyConstraint(Constraint):
    def __init__(self, *columns):
        self.columns = columns

    def create_constraint(self):
        return 'primary key(%s)' % ','.join(self.columns)

class UniqueConstraint(Constraint):
    def __init__(self, *columns):
        self.columns = columns

    def create_constraint(self):
        return 'unique(%s)' % ','.join(self.columns)

class CheckConstraint(Constraint):
    def __init__(self, expression):
        self.expression = expression

    def create_constraint(self):
        return 'check(%s)' % self.expression

class Table:
    strict = True
    with_rowid = False
    columns = []
    constraints = []

    def __init_subclass__(cls):
        cls.column_list = tuple(c.name for c in cls.columns)

    @classmethod
    def create_table_sql(cls, schema=None, if_not_exists=False):
        sql = 'create table%(if_not_exists)s %(schema)s%(table_name)s(%(columns)s%(constraints)s)%(options)s'
        options = []
        if cls.strict:
            options.append('strict')
        if not cls.with_rowid:
            options.append('without rowid')
        parts = {
            'if_not_exists': ' if not exists' if if_not_exists else '',
            'schema': '%s.' % schema if schema else '',
            'table_name': cls.name,
            'columns': ','.join(c.create_column() for c in cls.columns),
            'constraints': ','.join(('', *(c.create_constraint() for c in cls.constraints))),
            'options': ' %s' % ','.join(options) if options else '',
        }
        return sql % parts

    @classmethod
    def create_insert_sql_part(cls, obj):
        return '(%s)' % ','.join(format_sql_value(obj.get(col)) for col in cls.column_list)

    @classmethod
    def create_insert_sql(cls, objects):
        if not isinstance(objects, (list, tuple)):
            objects = objects,
        sql = 'insert into %(table)s(%(columns)s) values %(rows)s'
        parts = {
            'table': cls.name,
            'columns': ','.join(cls.column_list),
            'rows': ','.join(cls.create_insert_sql_part(obj) for obj in objects),
        }
        return sql % parts
