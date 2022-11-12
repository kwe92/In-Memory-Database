import pandas as pd
from sqlalchemy import create_engine


class _Error(Exception):
    """Base class for other exceptions."""
    pass


class _DuplicateTableNameError(_Error):
    """Raised when the name of a table already exsists within current connection."""
    pass


class InMemDB():
    ''' 
    InMemDB is a python package that allows you to create a sqlite relational database in memory
    out of a pandas.DataFrame object or iterable.
    '''

    def __init__(self):
        self._conn = create_engine('sqlite://')

    def createTableFromDF(self, table_name: str, df: pd.DataFrame, index: bool = False) -> None:
        '''
        Creates a new table in the current database from a pandas.DataFrame object.

        Parameters
        ----------     
        table_name: The table name of the newly created table. If the table name already exists
            within the database an error is raised.

        df: A pandas.DataFrame object that is used to construct table attributes and tuples.

        index: Set to False, if set to True the index will become an attribute of the new table.
        '''

        if self._conn.has_table(table_name):
            errorMsg = _errorMsg(table_name=table_name)
            raise _DuplicateTableNameError(errorMsg)
        try:
            df.to_sql(table_name, self._conn, index=index)
            return '{} table has been created successfully'.format(table_name)
        except AttributeError:
            raise AttributeError(
                'the df parameter must be a pandas.DataFrame object.')

    def createTableSeq(self, table_name: str, iterable, columns=None, index=None) -> None:
        '''
        Creates a new table in the current database from a python iterable object.

        Parameters
        ----------     
        table_name: The table name of the newly created table. If the table name already exists
            within the database an error is raised.

        iterable: python Iterable object.

        index : Index or array-like
            Index to use for resulting frame. Will default to np.arange(n) if
            no indexing information is provided.

        columns : Index or array-like
            Column labels to use for resulting frame. Will default to
            np.arange(n) if no column labels are provided.    
        '''

        if self._conn.has_table(table_name):
            errorMsg = _errorMsg(table_name=table_name)
            raise _DuplicateTableNameError(errorMsg)
        try:
            df = pd.DataFrame(iterable, columns=columns, index=index)
            df.to_sql(table_name, self._conn, index=False)
            return '{} table has been created successfully'.format(table_name)
        except ValueError:
            raise AttributeError(
                'The iterable parameter must be a dict object.')

    def dropTable(self, table_name: str) -> None:
        '''
        Drops the table from the current database if it exists.

        Parameters
        ----------     
        table_name: The table name of the table you want to drop. If the table name does not exist
            within the database an error is raised.
        '''

        if self._conn.has_table(table_name) == False:
            return '%s is not a table in the current database.' % (table_name)
        try:
            pd.read_sql_query(f'DROP TABLE {table_name}', self._conn)
        except:
            pass
        return f'{table_name} has been deleted'

    def query(self, query: str) -> pd.DataFrame:
        '''
        Queries the current database.
        Returns a pandas.DataFrame Object.

        Parameters
        ----------  
        query: SQL query string literal using SQLite syntax.
        '''
        if isinstance(query, str):
            return pd.read_sql_query(query, self._conn)
        else:
            raise AttributeError(
                'The query parameter must be a string literal.')

    def tableNames(self):
        '''
        Lists the current table names in the database.
        '''
        return self._conn.table_names()


def _errorMsg(table_name: str) -> str:
    errorMsg = f'{table_name} is already an existing table name in the database.'
    return errorMsg
