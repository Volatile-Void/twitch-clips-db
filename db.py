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

class Comparison:
    operator = None

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __str__(self):
        return '%s%s%s' % (self.a, self.operator, format_sql_value(self.b))

class Eq(Comparison):
    operator = '='

class Ne(Comparison):
    operator = '!='

class Gt(Comparison):
    operator = '>'

class Ge(Comparison):
    operator = '>='

class Lt(Comparison):
    operator = '<'

class Le(Comparison):
    operator = '<='

class IsNull(Comparison):
    operator = ' is '

    def __init__(self, a):
        self.a = a
        self.b = None

class IsNotNull(IsNull):
    operator = ' is not '

class LogicalGroup:
    def __init__(self, *filters):
        self.filters = filters

    def __str__(self):
        return (' %s ' % self.sep).join(s for s in (self._filter_to_str(f) for f in self.filters) if s)

    @staticmethod
    def _filter_to_str(f):
        if isinstance(f, Comparison):
            return str(f)
        if isinstance(f, LogicalGroup):
            if len(f.filters) == 0:
                return ''
            if len(f.filters) == 1:
                return str(f.filters[0])
        return '(%s)' % str(f)

class AndGroup(LogicalGroup):
    sep = 'and'

class OrGroup(LogicalGroup):
    sep = 'or'

class Column:
    def __init__(self, name, data_type, constraint=None):
        self.name = name
        self.data_type = data_type
        self.constraint = constraint

    def __str__(self):
        return '%s %s%s' % (self.name, self.data_type, ' %s' % self.constraint if self.constraint else '')

class Constraint:
    def __str__(self):
        return self.create_constraint()

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
        cls.column_lookup = {c.name: c for c in cls.columns}

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
            'columns': ','.join(str(c) for c in cls.columns),
            'constraints': ','.join(('', *(str(c) for c in cls.constraints))),
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

    @classmethod
    def create_select_sql(cls, columns=None, filter_obj=None, filter_logic=AndGroup, custom_where='', custom_where_logic=AndGroup):
        if columns is None:
            columns = cls.columns_list
        elif not isinstance(columns, (list, tuple)):
            columns = columns,
        where = custom_where
        if filter_obj:
            generated_where = filter_logic(*(Eq(col, val) for col, val in filter_obj.items() if col in cls.column_lookup))
            if where:
                where = custom_where_logic(generated_where, where)
            else:
                where = generated_where
        sql = 'select %(columns)s from %(table)s%(where)s'
        parts = {
            'columns': ','.join(columns),
            'table': cls.name,
            'where': ' where %s' % where if where else '',
        }
        return sql % parts
