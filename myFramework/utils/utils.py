import myFramework.source.posgresql.connect as conn
import pandas as pd
from multipledispatch import dispatch
from pandas_scd import scd2
from datetime import datetime

def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'", conn.getConnection(dbname))

def posgreExecute(dbName, query):
        engine = conn.getConnection(dbName)
        engine.execute(query)

@dispatch(str, str, str)
def getDF( source_dbname, tablename, schema):
    query = f"select T.* from {schema}.{tablename} T"
    return pd.read_sql(query ,conn.getConnection(source_dbname))

# @dispatch(str, str, str, str, str)
# def getDF( source_dbname, tablename,schema,filterColumn, dateFrom):
#     query = f"select T.* from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}'  "
#     return pd.read_sql(query ,conn.getConnection(source_dbname))

@dispatch(str, str, str, str, str,str)
def getDF( source_dbname, tablename,schema,filterColumn, dateFrom, dateTo):
    query = f"select T.* from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}' and {filterColumn} < '{dateTo}' "
    return pd.read_sql(query ,conn.getConnection(source_dbname))

def addInsertionDate(df: pd.DataFrame ):
      return df.assign(insertion_date = lambda x: datetime.now())


def generateSurogateKey(df, code, SurogatekeyList, dest_col_list):
      newdf = pd.DataFrame(df)
      for key in SurogatekeyList:
        # print(key)
        Surogatekey = newdf[key]+int(str(1000000) + str(code))
        newdf = newdf.assign(tmpkey = pd.Series(Surogatekey).values).drop(f'{key}', axis=1)
        newdf.rename(columns={'tmpkey':f'gen_{key}'}, inplace=True)
      newdf = newdf.reindex(dest_col_list, axis=1)
      return newdf

def GenerateNaturalKey(df, Naturalkey):
    newdf = pd.DataFrame(df)
    NaturalValue = newdf[Naturalkey]
    newdf = newdf.assign(tmpkey = pd.Series(NaturalValue).values)
    newdf.rename(columns={'tmpkey':f'source_{Naturalkey}'}, inplace=True)
    return newdf


def fillPosgres( df, dst_dbname, schema, tablename, insertiontype):
        df.to_sql(tablename, conn.getConnection(dst_dbname)
                , schema=f"{schema}", if_exists=insertiontype, index=False)
        
def toSCD2(srcDF: pd.DataFrame, targetDF: pd.DataFrame):
      final_df = scd2(srcDF, targetDF)
      return final_df