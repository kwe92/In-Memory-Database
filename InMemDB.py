import pandas as pd
from sqlalchemy import create_engine


class _Error(Exception):
    """Base class for other exceptions."""
    pass


class _DuplicateTableNameError(_Error):
    """Raised when the name of a table already exists within current connection."""
    pass


class _TableNameDoesNotExist(_Error):
    """Raised when the table does not exist within current connection."""
    pass


class InMemDB():
    ''' 
    InMemDB is a python package that allows you to create a sqlite relational database in memory
    out of a pandas.DataFrame object or iterable.
    '''

    def __init__(self):
        '''
        Initalize a sqlite database.
        '''
        self._conn = create_engine('sqlite://')

    def createTableFromDF(self, tableName: str, df: pd.DataFrame, index: bool = False) -> None:
        '''
        Creates a new table in the current database from a pandas.DataFrame object.

        Parameters
        ----------     
        tableName: The table name of the newly created table. If the table name already exists
            within the database an error is raised.

        df: A pandas.DataFrame object that is used to construct table attributes and tuples.

        index: Set to False, if set to True the index will become an attribute of the new table.
        '''

        if self._conn.has_table(tableName):
            errorMsg = _errorMsg(tableName=tableName)
            raise _DuplicateTableNameError(errorMsg)
        try:
            df.to_sql(tableName, self._conn, index=index)
            return '{} table has been created successfully'.format(tableName)
        except AttributeError:
            raise AttributeError(
                'the df parameter must be a pandas.DataFrame object.')

    def createTableSeq(self, tableName: str, iterable, columns=None, index=None) -> None:
        '''
        Creates a new table in the current database from a python iterable object.

        Parameters
        ----------     
        tableName: The table name of the newly created table. If the table name already exists
            within the database an error is raised.

        iterable: python Iterable object.

        index : Index or array-like
            Index to use for resulting frame. Will default to np.arange(n) if
            no indexing information is provided.

        columns : Index or array-like
            Column labels to use for resulting frame. Will default to
            np.arange(n) if no column labels are provided.    
        '''

        if self._conn.has_table(tableName):
            errorMsg = _errorMsg(tableName=tableName)
            raise _DuplicateTableNameError(errorMsg)
        try:
            df = pd.DataFrame(iterable, columns=columns, index=index)
            df.to_sql(tableName, self._conn, index=False)
            return '{} table has been created successfully'.format(tableName)
        except ValueError:
            raise AttributeError(
                'The iterable parameter must be a dict object.')

    def dropTable(self, tableName: str) -> None:
        '''
        Drops the table from the current database if it exists.

        Parameters
        ----------     
        tableName: The table name of the table you want to drop. If the table name does not exist
            within the database an error is raised.
        '''

        if self._conn.has_table(tableName) == False:
            raise _TableNameDoesNotExist(
                '%s is not a table in the current database.' % (tableName))

        try:
            pd.read_sql_query(f'DROP TABLE {tableName}', self._conn)
        except:
            pass
        return f'{tableName} has been deleted'

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
        return self._conn.tableNames()


def _errorMsg(tableName: str) -> str:
    errorMsg = f'{tableName} is already an existing table name in the database.'
    return errorMsg
