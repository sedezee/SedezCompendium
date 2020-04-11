from abc import ABCMeta
import warnings

# thanks to @Craftspider for the initial form of the Row class

class Row(metaclass = ABCMeta):
    __columns__ = ()

    def __init__(self, *row):
        if self.__class__ == Row: 
            raise TypeError("Abstract classes cannot be instantized.")
        
        for i in range(len(self.__columns__)):
            # map every available slot when given sufficient values
            column = self.__columns__[i]
            slot_value = row[i]
            # as column is hard-encoded, it will hold the individual slot_values,
            # not the rows.
            setattr(self, column, slot_value)
    
    def __str__(self): 
        out = ""
        for column in self.__columns__: 
            out += f"{column}: {getattr(self, column)}\n"
        return out

    def __eq__(self, other):
        def column_check(columns, o_columns, other): 
            for i in range(len(self.__columns__)):
                column = columns[i]
                o_column = o_columns[i]
                slot = getattr(self, column)
                o_slot = getattr(other, o_column)
                if column != o_column or slot != o_slot:
                    return False
            return True

        if not issubclass(type(other), Row) and not issubclass(type(other), Table): 
            raise NotImplementedError
        
        if self.__columns__ != other.__columns__:
            return False

        if issubclass(type(other), Table):
            if len(other.__rows__) > 1:
                return False
            return column_check(self.__columns__, other.__rows__[0].__columns__, other.__rows__[0])
        else: 
            return column_check(self.__columns__, other.__columns__, other)
    
    @classmethod
    def table_name(cls): 
        if hasattr(cls, "TABLE_NAME"):
            return getattr(cls, "TABLE_NAME")
        else:
            return None
        
    @classmethod
    def remove_column(cls, name): 
        c = list(cls.__columns__)
        try: 
            c.remove(name)
        except: 
            pass
        cls.__columns__ = tuple(c)

    @classmethod
    def add_column(cls, column_name, default = None): 
        c = list(cls.__columns__)
        c.append(column_name)
        setattr(c, column_name, default)
        cls.__columns__ = tuple(c)


class Table(metaclass = ABCMeta):
    __rows__ = []
    __columns__ = () 

    def __init__(self, *args):
        if self.__class__ == Table: 
            raise TypeError("Abstract classes cannot be instantized.")
        
        if args is not None: 
            for r in args: 
               self.__rows__.append(r)
        
        if len(self.__rows__): 
            if self.__columns__ is None or len(self.__columns__) == 0:
                self.__columns__ = self.__rows__[0].__columns__
            if not hasattr(self, "ROW_TYPE"): 
                setattr(self, "ROW_TYPE", type(self.__rows__[0]))
        elif hasattr(self, "ROW_TYPE"): 
            self.__columns__ = getattr(self, "ROW_TYPE").__columns__

        for row in self.__rows__:
            self.check_row(row)
    
    def check_row(self, row): 
        if not issubclass(type(row), Row):
            raise TypeError(f"Rows need to subclass Row. You're subclassing {type(row)}")
            
        if hasattr(self, "ROW_TYPE") and not isinstance(row, getattr(self, "ROW_TYPE")): 
            warnings.warn(f"{row} is not of the saved ROW_TYPE.")
        
        if len(row.__columns__) > 0 and len(self.__columns__)> 0:
            if row.__columns__ != self.__columns__: 
                raise ValueError("Columns must be the same for every row in the table.")
                
    def __str__(self): 
        out = ""
        for column in self.__columns__: 
            out += f"{column}: "
            for row in self.__rows__: 
                out += f"{getattr(row, column)},"
            out = out[:-1]
            out+= f"\n"
        return out
    
    def __eq__(self, other): 
        if not issubclass(type(other), Table) and not issubclass(type(other), Row): 
            raise NotImplementedError
      
        if issubclass(type(other), Row):
            if len(self.__rows__) > 1: 
                return False
            if self.__rows__[0] != other:
                return False

        if self.__columns__ != other.__columns__: 
            return False 
        
        if issubclass(type(other), Table):
            if self.__rows__ != other.__rows__:
                return False
        
            for index in range(len(self.__rows__)):
                if self.__rows__[index] != other.__rows__[index]: 
                    return False
    
        return True

    def __iter__(self): 
        return iter(self.__rows__)

    def __contains__(self, other): 
        return other in self.__rows__
    
    @classmethod
    def table_name(cls): 
        if hasattr(cls, "TABLE_NAME"):
            return getattr(cls, "TABLE_NAME")
        else:
            return None
        
    @classmethod
    def row_type(cls): 
        if hasattr(cls, "ROW_TYPE"): 
            return getattr(cls, "ROW_TYPE")
        elif len(cls.__rows__) != 0: 
            return type(cls.__rows__[0])
        else: 
            return nRow

    @classmethod
    def remove_column(cls, column_name): 
        for row in cls.__rows__: 
            row.remove_column(column_name)

        c = list(cls.__columns__)
        try: 
            c.remove(column_name)
        except: 
            pass

        cls.__columns__ = tuple(c)

    @classmethod 
    def add_column(cls, column_name, column_type): 
        for row in cls.__rows__: 
            row.add_column(column_name)
        
        c = list(cls.__columns__)
        c.append(column_name)
        cls.__columns__ = tuple(c)

    @classmethod 
    def add_row(cls, row): 
        cls.check_row(row)
        cls.__rows__.append(row)


class nRow(Row): 
    def __init__(self, *rows, columns = None): 
        c = []
        if columns is None:
            for i in range(1, len(rows) + 1): 
                c.append(str(i))
        if columns is not None: 
            for i in columns: 
                c.append(i)
        self.__columns__ = tuple(c)
        super().__init__(*list(rows))