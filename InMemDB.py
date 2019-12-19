from sqlalchemy import create_engine
import pandas as pd

class InMemDB():
    ''' 
    InMemDB is a python package that allows you to create a sqlite relational database in memory
    out of a pandas.DataFrame object or any array-like sequence type.
    '''
       
    def __init__(self):
        self._conn = create_engine('sqlite://')
        
    def CreateTableDF(self, table_name:'str', df:'pandas.DataFrame',index=False)->None:
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
            return f'{table_name} is already an existing table name in the database'
        try:
            df.to_sql(table_name,self._conn,index=index)
            return '{} table has been created successfully'.format(table_name)
        except AttributeError as e:
            raise AttributeError('the df parameter must be a pandas.DataFrame object.')

    
    def CreateTableSeq(self, table_name:'str', seq:'dict or array like object', columns=None, index=None)->None:
        
        '''
        Creates a new table in the current database from a dict or array like object.
        
        Parameters
        ----------     
        table_name: The table name of the newly created table. If the table name already exists
            within the database an error is raised.
        
        seq: dict or array like object
        
        index : Index or array-like
            Index to use for resulting frame. Will default to np.arange(n) if
            no indexing information part of input data and no index provided.
        
        columns : Index or array-like
            Column labels to use for resulting frame. Will default to
            np.arange(n) if no column labels are provided.    
        '''   
        
        if self._conn.has_table(table_name):
            return f'{table_name} is already an existing table name in the database'
        try:
            df = pd.DataFrame(seq,columns=columns,index=index)
            df.to_sql(table_name, self._conn,index=False)
            return '{} table has been created successfully'.format(table_name)
        except ValueError:
            raise AttributeError('The seq parameter must be a dict object.')

    
    def DropTable(self, table_name:'str')->None:
        '''
        Drops the table from the current database if it exists.
        
        Parameters
        ----------     
        table_name: The table name of the table you want to drop. If the table name does not exists
            within the database an error is raised.
    
        '''
        
        if self._conn.has_table(table_name)==False:
            return '%s is not a table in the current database.' % (table_name)
        try:
            pd.read_sql_query(f'DROP TABLE {table_name}', self._conn)
        except:
            pass
        return f'{table_name} has been deleted'
    
    def query(self, query:'str')->'pandas.DataFrame':
        '''
        Queries the current database.
        Returns a pandas.DataFrame Object.
        
        Parameters
        ----------  
        query: SQL query using SQLite syntax.
        '''
        if isinstance(query,str):
            return pd.read_sql_query(query,self._conn)
        else:
            raise AttributeError('The query parameter must be a str object')
    
    def TableNames(self):
        '''
        Lists the current table names in the database.
        '''
        return self._conn.table_names()
    
