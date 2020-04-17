from .SQLObjects import Row, Table, nRow
import psycopg2


class EmptyCursor(psycopg2.extensions.cursor):
    """
    An empty cursor for when a database hasn't been connected. Returns 
    and empty tuple or None where appropriate.
    """

    __slots__ = ()

    DEFAULT_ONE = None
    DEFAULT_MANY = tuple() 

    def __init__(self, *args, **kwargs): 
        pass
    
    def fetchone(self): 
        return self.DEFAULT_ONE
    
    def fetchmany(self, size = None): 
        return self.DEFAULT_MANY

    def fetchall(self): 
        return self.DEFAULT_MANY

    def scroll(self):
        return None

    def execute(self): 
        return self.DEFAULT_ONE
    
    def executemany(self): 
        return self.DEFAULT_MANY
    
    def callproc(self): 
        return None
    
    def mogrify(self): 
        return ""
    
    @property
    def description(self): 
        return tuple()


def storage_key(kwargs):
    """
    Generate the storage key for the cache. 
    :param kwargs: Keyword arguments dict 
    :return: AND statement
    """
    return frozenset(f"{hash(i)}|{hash(kwargs[i])}" for i in kwargs)


_cache_dict = {}


def cache(func): 
    """
    Creates an entry in the cache if one does not already exist, 
    so that information that is not from the database can be stored and retrieved. 
    :param func: function to be converted into the cache 
    :return: new function 
    """

    _cache_dict[func] = {}

    def check_cache(self, t_type, *args, **kwargs):
        o_type = t_type
 
        try: 
            t_type = t_type.row_type()
        except: 
            pass

        _cache = _cache_dict[func].setdefault(t_type, {})
        cache_key = storage_key(kwargs)
        if _cache.get(cache_key, None) is not None:
            return _cache[cache_key]
        res = func(self, o_type, *args, **kwargs)
        _cache[cache_key] = res
        return res

    return check_cache


def invalidate(func): 
    """
    Invalidates the cache if it is changed. 
    :param func: The function passed in to invalidate. 
    :return: new function
    """
    def invalidate_cache(self, *args, **kwargs):
        if len(args) > 0: 
            if not isinstance(args[0], type): 
                t_type = type(args[0])
            else:
                t_type = args[0]
            
            try: 
                t_type = t_type.row_type()
            except:
                pass
    
            for key in _cache_dict:
                try:
                    del _cache_dict[key][t_type]
                except KeyError:
                    pass
        
        return func(self, *args, **kwargs)

    return invalidate_cache


class GenericDatabase:
    """
    Generic database designed to work with Postgresql and psycopg2.
    """

    def __init__(self, db_name, address, port, username, password, schema, gen_cursor = False): 
        self.__host = address
        self.__port = port
        self.__username = username
        self.__password = password
        self.__db_name = db_name
        self.__schema = schema
        self.__conn = None
        if gen_cursor:
            self.__cursor = self.cursor_gen()
        else: 
            self.__cursor = EmptyCursor()

    def and_convert(self, kwargs): 
        """
        Generates an SQL statement of ANDed together parameters. 
        :param kwargs: Keyword argument dict
        :return: ANDed string
        """

        if len(kwargs) > 0: 
            out = " WHERE"
        else: 
            out = ""
        
        for key, value in kwargs.items(): 
            out += f" {key} = {value} AND"
        out = out[:-4]
        return out 
        
    def cursor_gen(self): 
        """
        Generates a cursor for the database.
        :return: a psycopg2 cursor. If an error is raised, an EmptyCursor.
        """

        try: 
            conn = psycopg2.connect(host = self.__host, port = self.__port, user = self.__username, password = self.__password, database = self.__db_name)
            conn.set_session(autocommit = True)
            self.__conn = conn
            return conn.cursor()

        except Exception as e:
            print(f"CURSOR GEN: {e}")
            return EmptyCursor()

    def is_connected(self):  
        return self.__conn.poll() == psycopg2.extensions.POLL_OK
    
    def gen_row(self, t_type, result):
        """
        Generates the type of role specified. 
        :param t_type: the type of row to generate
        :param result: the information to load the row with
        :return: the row, loaded with the result
        """
        try:
            t_type = t_type.row_type()
        except:
            pass

        if t_type is not nRow:
            return t_type(*result)
        else:
            if len(t_type.__columns__):
                c_names = t_type.__columns__
            else:
                c_names = [desc[0] for desc in self.__cursor.description]
            row = t_type(*result, columns = c_names)
            return row

    def execute(self, statement, args = None):
        """
        Executes an SQL statement to a pyscopg2 database. 
        :return: None
        """

        try: 
            self.__cursor.execute(statement, args)
        except psycopg2.Error as e: 
            print(e)
            if e == 8006: 
                self.__cursor = self.cursor_gen()
                self.execute(statement, args)
        except Exception as e: 
            print(f"EXECUTE: {e}")

    def schema_exists(self): 
        if self.is_connected():
            self.execute("""
            SELECT exists(SELECT schema_name FROM information_schema.schemata
            WHERE schema_name = %s);""",
                (str(self.__schema),)) 
        
        return self.__cursor.fetchone()[0]  
        
    def create_schema(self, schema): 
        self.execute("""CREATE SCHEMA %s;""", 
        (schema,))
        
    # generic methods for managing a database
    @cache
    def get_item(self, t_type, default = None, **kwargs):
        """
        Used to retrieve an item from the database. Always returns a row.  
        :param t_type: Type that should be returned. Subclasses Row or Table*. 
        :param default: Return type if nothing is found. Defaults to None. 
        :param kwargs: Parameters to filter by.
        :return: row, loaded with information
        *Note that while a table can be passed, it will always return 
        (in order of if they exist or not), the ROW_TYPE of the Table, the type
        of the first row in the table, or an nRow that mimics a normally created
        row with all of the columns and behaviors of a normal row. 
        """
        if not isinstance(t_type, type): 
            t_type = type(t_type) 
        kwargs = self.and_convert(kwargs)
        query = f"SELECT * FROM {self.__schema}.{t_type.table_name()}"

        if kwargs: 
            query += kwargs
        
        query += " LIMIT 1;"
        self.execute(query)
        result = self.__cursor.fetchone()
        if result is None: 
            return default

        if issubclass(t_type, Table): 
            return self.gen_row(t_type, result)
        else: 
            try: 
                return t_type(*result)
            except Exception as e: 
                print(f"GET ITEM: {e}")

    @cache
    def get_items(self, t_type, default = None, **kwargs): 
        """
        Get several items from the database. 
        :param t_type: Type that should be returned. Subclasses Table or Row*. 
        :param default: Return type if nothing is found. Defaults to None. 
        :param kwargs: Parameters to filter by. 
        :return: if t_type subclasses table, a table of the same type. If not, a row. 
        """
        kwargs = self.and_convert(kwargs)
        query = f"SELECT * FROM {self.__schema}.{t_type.table_name()}"
        
        if kwargs: 
            query += kwargs
        
        self.execute(query)
        result = self.__cursor.fetchall()
        r = [] 

        try: 
            for res in result: 
                r.append(self.gen_row(t_type, res))
            
            return t_type(*r)
        except: 
            for res in result:
                try: 
                    r.append(t_type(*res))
                except Exception as e: 
                    print(f"GET ITEMS: {e}")
            return r

    @invalidate
    def update_item(self, data, **kwargs):
        kwargs = self.and_convert(kwargs)
        query = f"UPDATE {self.__schema}.{data.table_name()} SET "
        for cr in data:
            try:
                for column in cr:
                    if isinstance(getattr(cr, column), str):
                        query += f"{column} = '{getattr(cr, column)}',"
                    else:
                        query += f"{column} = {getattr(cr, column)},"
            except (TypeError, AttributeError):
                if isinstance(getattr(data, cr), str):
                    query += f"{cr} = '{getattr(data, cr)}',"
                else:
                    query += f"{cr} = {getattr(data, cr)},"

        query = query[:-1] + kwargs + ';'
        self.execute(query)

    @invalidate
    def insert_item(self, data):
        """
        Save an item to the database. 
        :param data: the data to load in. Subclasses Row or Table.  
        """
        query = f"INSERT INTO {self.__schema}.{data.table_name()} ({','.join(data.__columns__)}) VALUES ("

        try: 
            if len(data.__rows__) == 1: 
                data = data.__rows__[0]
            else: 
                raise ValueError()
        except ValueError as e: 
            print(f"INSERT ITEM: {e}")
        except Exception as e: 
            pass
        
        for column in data.__columns__:
            query += f"{getattr(data, column)},"
        query = query[:-1]
        query += ");"
        self.execute(query)

    @invalidate
    def insert_items(self, data):
        """
        Save several items off to the databse. 
        :param data: The data to be saved. Expects a list or a class that subclasses data.
        """
        try: 
            for item in data.__rows__:
                self.save_item(item)
        except Exception: 
            for item in data: 
                self.save_item(data)

    @invalidate
    def remove_rows(self, t_type, limit = None, **kwargs): 
        """
        Remove specified object from the database.
        :param t_type: type of object to be removed.
        :param limit: limit on rows to remove.
        :param kwargs: Parameters to filter by. 
        """
        kwargs = self.and_convert(kwargs)
        query = f"DELETE FROM {self.__schema}.{t_type.table_name()}"

        if kwargs: 
            query += kwargs

        if limit is not None: 
            try: 
                limit = f"{limit[0]}, {limit[1]}"
            except ValueError: 
                pass
            query += f" LIMIT {limit}"

        self.execute(query)
    
    def load_table(self, t_type):
        """
        :param t_type: Type of table to load
        :return: a table with information loaded from the database
        """
        if not isinstance(t_type, type):
            t_type = type(t_type)

        query = f"SELECT * FROM {self.__schema}.{t_type.table_name()};"
        self.execute(query)
        res = self.__cursor.fetchall()
        r = []
        for row in res: 
            r.append(self.gen_row(t_type, row))
        return t_type(*r)

    def create_table(self, data, **columns): 
        """
        Creates a table in the database loaded with all of the information from the given data.
        :param data: Table to create in the database
        :param columns: list of column definitions
        """

        query = f"CREATE TABLE {self.__schema}.{data.table_name()} ("
        key_list = []
        for key in columns: 
            key_list.append(f"{key} {columns[key]},\n")
        query = f"{query}{''.join(key_list)[:-2]});"
        self.execute(query)

        try: 
            self.insert_items(data)
        except TypeError: 
            try:    
                self.insert_item(data)
            except: 
                pass
    
    @invalidate
    def drop_table(self, table): 
        """
        Drop a table from the schema. 
        :param table: name or type of table to drop.
        """
        if isinstance(table, str): 
            self.execute(f"DROP TABLE {self.__schema}.{table};")
        else: 
            self.execute(f"DROP TABLE {self.__schema}.{table.table_name()};")

    @invalidate
    def remove_column(self, data, column):
        """
        Removes the specified column from the database and alters all existing 
        local Rows and Tables containing the column. 
        :param data: The Row or Table to delete the column from
        :param column: which column to delete
        """
        if not isinstance(data, str): 
            data.remove_column(column)
            data = data.table_name()
        
        self.execute(f"ALTER TABLE {self.__schema}.{data} DROP COLUMN {column}")

    def add_column(self, data, column, column_type, default = None): 
        """
        Adds the specified column to the database and alters all existing local
        Rows and Tables to add the column.
        :param data: the Row or Table to add the column too 
        :param column: the column name to add 
        :param column_type: the column type 
        :param default: the default value for the column. Defaults to None.
        """
        if default is None: 
            default = ""
        else: 
            default = f" SET DEFAULT {default}"

        if not isinstance(data, str): 
            data.add_column(column, column_type)
            data = data.table_name()
        
        self.execute(f"ALTER TABLE {data} ADD COLUMN {column} {column_type}{default}")

    def get_tables(self): 
        """
        Returns all of the table names in the database.
        :return: a tuple containing strings.
        """

        self.execute(f"SELECT table_name FROM information_schema.TABLES WHERE table_schema = '{self.__schema}'")
        return tuple([f[0] for f in self.__cursor.fetchall()])
