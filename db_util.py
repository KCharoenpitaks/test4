import pandas as pd
import requests
import json
from psycopg2.extras import Json
import sqlite3
import psycopg2
import pymssql
import math
    
class mssql_simulation_database(object):
    def __init__(self, dbname):
        self.conn = ""
        self.dbname = dbname
        self.table = ""
        self.cursor = ""
        self.temp_table = ""
        self.temp_table_name = "t_bp_creditassessment_temp_sim1"
        print ("Initialized database successfully")
        
    def connect(self):
        self.conn = pymssql.connect("192.168.70.60:3200/BPDEV", "korawit", "Bp2020f+26*", "thaibma_bp") #("IP/Port","Username","Password","dbname")
        print ("Connected database successfully")
        self.cursor = self.conn.cursor()
        return self.cursor
        
    def close(self):
        self.cursor.close()
        self.conn.close()
        self.conn = ""
        self.cursor = ""
        print ("Closed database successfully")
        
    def create_table(self,table):
        self.table = table
        self.cursor = self.connect()
        self.cursor.execute('''CREATE TABLE '''+str(self.table)+''' 
                 (asof DATE NOT NULL,
                 cluster_id INT,
                 box_id INT,
                 symbol VARCHAR (15) NOT NULL,
                 issuer VARCHAR (15),
                 sector_abbr VARCHAR (15),
                 rating VARCHAR(15),
                 ttm FLOAT,
                 issuer_type_code VARCHAR (15),
                 attribute VARCHAR (15),
                 is_traded_today VARCHAR (15),
                 is_pivot VARCHAR (15),
                 trade_date DATE,
                 prev_bd DATE,
                 prev_trade_static_spread FLOAT,
                 diff_static_spread FLOAT,
                 adj_spread_pre FLOAT,
                 corr_factor FLOAT,
                 adj_spread FLOAT,
                 status VARCHAR (50),
                 prev_m2m_static_spread FLOAT,
                 today_m2m_static_spread FLOAT,
                 limit_5bd FLOAT,
                 last_5bd_spread FLOAT,
                 used_5bd FLOAT,
                 percent_breach_5bd FLOAT,
                 limit_10bd FLOAT,
                 last_10bd_spread FLOAT,
                 used_10bd FLOAT,
                 percent_breach_10bd FLOAT,
                 limit_22bd FLOAT,
                 last_22bd_spread FLOAT,
                 used_22bd FLOAT,
                 percent_breach_22bd FLOAT,
                 PRIMARY KEY(asof,symbol));''')
        self.conn.commit()
        print ("Create table successfully")
        self.close()

    def query(self,query_str):
        self.cursor = self.connect()
        self.cursor = self.cursor.execute(query_str)
        print ("Query to database successfully")
        return self.cursor
    
    def commit(self):
        self.conn.commit()
        print ("Committed database successfully")
        #self.close()
        
    def insert(self,table,obj):
        self.table = table
        #obj = json.loads(json_text.text)
        sql_str = self.get_insert_string(obj,self.table) #depend on data structure
        self.query(sql_str)
        self.conn.commit()
        
    def read(self,query_str):
        self.cursor = self.connect()
        self.cursor.execute(query_str)
        data = self.cursor.fetchall()
        column_names = [i[0] for i in self.cursor.description]
        df_read = pd.DataFrame(data, columns = column_names)
        print ("Read database successfully")
        self.conn.commit()
        self.close()
        return df_read
    
    def drop_table(self,table):
        self.table = table
        query_str = '''DROP TABLE '''+ table+''';'''
        self.query(query_str)
        print ("Dropped database successfully")
        self.conn.commit()
        


    def update_table(self,table,json_text):
        #self.drop_table(self.temp_table_name)
        self.create_table(self.temp_table_name)
        self.insert(self.temp_table_name, json_text)
        string_test = """MERGE %s t
                    USING t_bp_creditassessment_temp_sim1 s
                ON (s.asof = t.asof) AND (s.symbol = t.symbol)
                WHEN MATCHED
                    THEN UPDATE SET 
                        t.cluster_id = s.cluster_id,
                        t.box_id = s.box_id,
                        t.issuer = s.issuer,
                        t.sector_abbr = s.sector_abbr,
                        t.rating = s.rating,
                        t.ttm = s.ttm,
                        t.issuer_type_code = s.issuer_type_code,
                        t.attribute = s.attribute,
                        t.is_traded_today = s.is_traded_today,
                        t.is_pivot = s.is_pivot,
                        t.trade_date = s.trade_date,
                        t.prev_bd = s.prev_bd,
                        t.prev_trade_static_spread = s.prev_trade_static_spread,
                        t.diff_static_spread = s.diff_static_spread,
                        t.adj_spread_pre = s.adj_spread_pre,
                        t.corr_factor = s.corr_factor,
                        t.adj_spread = s.adj_spread,
                        t.status = s.status,
                        t.prev_m2m_static_spread = s.prev_m2m_static_spread,
                        t.today_m2m_static_spread = s.today_m2m_static_spread,
                        t.limit_5bd = s.limit_5bd,
                        t.last_5bd_spread = s.last_5bd_spread,
                        t.used_5bd = s.used_5bd,
                        t.percent_breach_5bd = s.percent_breach_5bd,
                        t.limit_10bd = s.limit_10bd,
                        t.last_10bd_spread = s.last_10bd_spread,
                        t.used_10bd = s.used_10bd,
                        t.percent_breach_10bd = s.percent_breach_10bd,
                        t.limit_22bd = s.limit_22bd,
                        t.last_22bd_spread = s.last_22bd_spread,
                        t.used_22bd = s.used_22bd,
                        t.percent_breach_22bd = s.percent_breach_22bd 
                WHEN NOT MATCHED BY TARGET 
                    THEN INSERT (asof,cluster_id, box_id, symbol, issuer, sector_abbr, rating, ttm, issuer_type_code, attribute, is_traded_today, is_pivot, trade_date, prev_bd, prev_trade_static_spread, diff_static_spread, adj_spread_pre, corr_factor, adj_spread, status, prev_m2m_static_spread, today_m2m_static_spread, limit_5bd, last_5bd_spread, used_5bd, percent_breach_5bd, limit_10bd, last_10bd_spread, used_10bd, percent_breach_10bd, limit_22bd, last_22bd_spread, used_22bd, percent_breach_22bd)
                        VALUES (s.asof,s.cluster_id, s.box_id, s.symbol, s.issuer, s.sector_abbr, s.rating, s.ttm, s.issuer_type_code, s.attribute,s.is_traded_today, s.is_pivot, s.trade_date, s.prev_bd, s.prev_trade_static_spread, s.diff_static_spread, s.adj_spread_pre, s.corr_factor, s.adj_spread, s.status, s.prev_m2m_static_spread, s.today_m2m_static_spread, s.limit_5bd, s.last_5bd_spread, s.used_5bd, s.percent_breach_5bd, s.limit_10bd, s.last_10bd_spread, s.used_10bd, s.percent_breach_10bd, s.limit_22bd, s.last_22bd_spread, s.used_22bd, s.percent_breach_22bd);""" %(table)
        self.query(string_test)
        self.commit()
        self.drop_table(self.temp_table_name)
        self.temp_table = ""
        print ("Update database successfully")
    
    def get_insert_string(self,obj,table_name):
        values = [list(x.values()) for x in obj]
        
        # get the column names
        columns = [list(x.keys()) for x in obj][0]
        
        # value string for the SQL string
        values_str = ""
        
        # enumerate over the records' values
        for i, record in enumerate(values):
        
            # declare empty list for values
            val_list = []
           
            # append each value to a new list of values
            for v, val in enumerate(record):
                #print("val", val)
                #print("type", type(val))
                #type(val) == float
                #math.isnan(val)
                #if type(val) == str:
                #    val = str(Json(val)).replace('"', '')
                if (type(val) == float):
                    if math.isnan(val):
                        val = "NULL"
                elif (((type(val) == float) or (type(val) == int)) and (math.isnan(val)==False)):
                    pass
                elif (val  == None):
                    val = "NULL"
                else:
                    val = str(Json(val)).replace('"', '')
                val_list += [ str(val) ]
        
        
            # put parenthesis around each record string
            values_str += "(" + ', '.join( val_list ) + "),\n"
        
        # remove the last comma and end SQL with a semicolon
        values_str = values_str[:-2]
        
        # concatenate the SQL string
        """
        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
            table_name,
            ', '.join(columns),
            values_str
        )
        """
        
        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
            table_name,
            ', '.join(columns),
            values_str+
            ";"
        )
        return sql_string

class mssql_database_box(object):
    def __init__(self, dbname):
        self.conn = ""
        self.dbname = dbname
        self.table = ""
        self.cursor = ""
        self.temp_table = ""
        self.temp_table_name = "t_bp_creditassessment_temp"
        print ("Initialized database successfully")
        
    def connect(self):
        self.conn = pymssql.connect("192.168.70.60:3200/BPDEV", "korawit", "Bp2020f+26*", "thaibma_bp") #("IP/Port","Username","Password","dbname")
        print ("Connected database successfully")
        self.cursor = self.conn.cursor()
        return self.cursor
        
    def close(self):
        self.cursor.close()
        self.conn.close()
        self.conn = ""
        self.cursor = ""
        print ("Closed database successfully")
        
    def create_table(self,table):
        self.table = table
        self.cursor = self.connect()
        self.cursor.execute('''CREATE TABLE '''+str(self.table)+''' 
                 (cluster_id INT NOT NULL,
                 box_id INT NOT NULL,
                 issue VARCHAR (15) NOT NULL,
                 issuer VARCHAR (15) NOT NULL,
                 as_of date NOT NULL,
                 ttm FLOAT NOT NULL,
                 rating VARCHAR (15) NOT NULL,
                 PRIMARY KEY(issue,ttm));''')
        self.conn.commit()
        print ("Create table successfully")
        self.close()

    def query(self,query_str):
        self.cursor = self.connect()
        self.cursor = self.cursor.execute(query_str)
        print ("Query to database successfully")
        return self.cursor
    
    def commit(self):
        self.conn.commit()
        print ("Committed database successfully")
        #self.close()
        
    def insert(self,table,json_text):
        self.table = table
        obj = json.loads(json_text)
        sql_str = self.get_insert_string(obj,self.table) #depend on data structure
        self.query(sql_str)
        self.conn.commit()
        
    def read(self,query_str):
        self.cursor = self.connect()
        self.cursor.execute(query_str)
        data = self.cursor.fetchall()
        column_names = [i[0] for i in self.cursor.description]
        df_read = pd.DataFrame(data, columns = column_names)
        print ("Read database successfully")
        self.conn.commit()
        self.close()
        return df_read
    
    def drop_table(self,table):
        self.table = table
        query_str = '''DROP TABLE '''+ table+''';'''
        self.query(query_str)
        print ("Dropped database successfully")
        self.conn.commit()
        
    def update_table(self,table,json_text):
        #self.drop_table(self.temp_table_name)
        self.create_table(self.temp_table_name)
        self.insert(self.temp_table_name, json_text)
        string_test = """MERGE %s t
                    USING t_bp_creditassessment_temp s
                ON (s.ttm = t.ttm) AND (s.issue = t.issue)
                WHEN MATCHED
                    THEN UPDATE SET 
                        t.cluster_id = s.cluster_id,
                        t.box_id = s.box_id,
                        t.issue = s.issue,
                        t.issuer = s.issuer,
                        t.as_of = s.as_of,
                        t.ttm = s.ttm,
                        t.rating = s.rating
                WHEN NOT MATCHED BY TARGET 
                    THEN INSERT (cluster_id, box_id, issue, issuer, as_of, ttm, rating)
                        VALUES (s.cluster_id, s.box_id, s.issue, s.issuer, s.as_of, s.ttm, s.rating);""" %(table)
        self.query(string_test)
        self.commit()
        self.drop_table(self.temp_table_name)
        self.temp_table = ""
        print ("Update database successfully")
    
    def get_insert_string(self,obj,table_name):
        
        # get the row names
        obj_ = list(obj.values())
        values = [list(x.values()) for x in obj_]
        
        # get the column names
        columns = list(obj['0'].keys())
        
        # value string for the SQL string
        values_str = ""
        
        # enumerate over the records' values
        for i, record in enumerate(values):
        
            # declare empty list for values
            val_list = []
           
            # append each value to a new list of values
            for v, val in enumerate(record):
                #if type(val) == str:
                if ((type(val) == float) or (type(val) == int)):
                    pass
                elif (val  == None):
                    val = "NULL"
                else:
                    val = str(Json(val)).replace('"', '')
                    #print(val)
                    
                val_list += [ str(val) ]
            #print(val_list)
        
            # put parenthesis around each record string
            values_str += "(" + ', '.join( val_list ) + "),\n"
            #print(values_str)
            
        # remove the last comma and end SQL with a semicolon
        values_str = values_str[:-2] + ";"
        
        # concatenate the SQL string
        table_name = table_name
        #sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
            table_name,
            ', '.join(columns),
            values_str
        )
        return sql_string

class mssql_trade_database(object):
    def __init__(self, dbname):
        self.conn = ""
        self.dbname = dbname
        self.table = ""
        self.cursor = ""
        self.temp_table = ""
        self.temp_table_name = "t_bp_creditassessment_temp"
        print ("Initialized database successfully")
        
    def connect(self):
        self.conn = pymssql.connect("192.168.70.60:3200/BPDEV", "korawit", "Bp2020f+26*", "thaibma_bp") #("IP/Port","Username","Password","dbname")
        print ("Connected database successfully")
        self.cursor = self.conn.cursor()
        return self.cursor
        
    def close(self):
        self.cursor.close()
        self.conn.close()
        self.conn = ""
        self.cursor = ""
        print ("Closed database successfully")
        
    def create_table(self,table):
        self.table = table
        self.cursor = self.connect()
        self.cursor.execute('''CREATE TABLE '''+str(self.table)+''' 
                 (asof DATE NOT NULL,
                 symbol VARCHAR (15) NOT NULL,
                 ttm FLOAT,
                 trade_date DATE,
                 prev_bd DATE,
                 static_spread FLOAT,
                 diff_static_spread FLOAT,
                 prev_m2m_static_spread FLOAT,
                 today_m2m_static_spread FLOAT,
                 limit_5bd FLOAT,
                 last_5bd_spread FLOAT,
                 used_5bd FLOAT,
                 percent_breach_5bd FLOAT,
                 limit_10bd FLOAT,
                 last_10bd_spread FLOAT,
                 used_10bd FLOAT,
                 percent_breach_10bd FLOAT,
                 limit_22bd FLOAT,
                 last_22bd_spread FLOAT,
                 used_22bd FLOAT,
                 percent_breach_22bd FLOAT,
                 is_traded_today VARCHAR (15),
                 PRIMARY KEY(asof,symbol));''')
        self.conn.commit()
        print ("Create table successfully")
        self.close()

    def query(self,query_str):
        self.cursor = self.connect()
        self.cursor = self.cursor.execute(query_str)
        print ("Query to database successfully")
        return self.cursor
    
    def commit(self):
        self.conn.commit()
        print ("Committed database successfully")
        #self.close()
        
    def insert(self,table,obj):
        self.table = table
        #obj = json.loads(json_text.text)
        sql_str = self.get_insert_string(obj,self.table) #depend on data structure
        self.query(sql_str)
        self.conn.commit()
        
    def read(self,query_str):
        self.cursor = self.connect()
        self.cursor.execute(query_str)
        data = self.cursor.fetchall()
        column_names = [i[0] for i in self.cursor.description]
        df_read = pd.DataFrame(data, columns = column_names)
        print ("Read database successfully")
        self.conn.commit()
        self.close()
        return df_read
    
    def drop_table(self,table):
        self.table = table
        query_str = '''DROP TABLE '''+ table+''';'''
        self.query(query_str)
        print ("Dropped database successfully")
        self.conn.commit()
        


    def update_table(self,table,json_text):
        #self.drop_table(self.temp_table_name)
        self.create_table(self.temp_table_name)
        self.insert(self.temp_table_name, json_text)
        string_test = """MERGE %s t
                    USING t_bp_creditassessment_temp s
                ON (s.asof = t.asof) AND (s.symbol = t.symbol)
                WHEN MATCHED
                    THEN UPDATE SET 
                        t.ttm = s.ttm,
                        t.trade_date = s.trade_date,
                        t.prev_bd = s.prev_bd,
                        t.static_spread = s.static_spread,
                        t.diff_static_spread = s.diff_static_spread,
                        t.prev_m2m_static_spread = s.prev_m2m_static_spread,
                        t.today_m2m_static_spread = s.today_m2m_static_spread,
                        t.limit_5bd = s.limit_5bd,
                        t.last_5bd_spread = s.last_5bd_spread,
                        t.used_5bd = s.used_5bd,
                        t.percent_breach_5bd = s.percent_breach_5bd,
                        t.limit_10bd = s.limit_10bd,
                        t.last_10bd_spread = s.last_10bd_spread,
                        t.used_10bd = s.used_10bd,
                        t.percent_breach_10bd = s.percent_breach_10bd,
                        t.limit_22bd = s.limit_22bd,
                        t.last_22bd_spread = s.last_22bd_spread,
                        t.used_22bd = s.used_22bd,
                        t.percent_breach_22bd = s.percent_breach_22bd,
                        t.is_traded_today = s.is_traded_today
                WHEN NOT MATCHED BY TARGET 
                    THEN INSERT (asof, symbol, ttm, trade_date, prev_bd, static_spread, diff_static_spread, prev_m2m_static_spread, today_m2m_static_spread, limit_5bd, last_5bd_spread, used_5bd, percent_breach_5bd, limit_10bd, last_10bd_spread, used_10bd, percent_breach_10bd, limit_22bd, last_22bd_spread, used_22bd, percent_breach_22bd, is_traded_today)
                        VALUES (s.asof, s.symbol, s.ttm, s.trade_date, s.prev_bd, s.static_spread, s.diff_static_spread, s.prev_m2m_static_spread, s.today_m2m_static_spread, s.limit_5bd, s.last_5bd_spread, s.used_5bd, s.percent_breach_5bd, s.limit_10bd, s.last_10bd_spread, s.used_10bd, s.percent_breach_10bd, s.limit_22bd, s.last_22bd_spread, s.used_22bd, s.percent_breach_22bd, s.is_traded_today);""" %(table)
        self.query(string_test)
        self.commit()
        self.drop_table(self.temp_table_name)
        self.temp_table = ""
        print ("Update database successfully")
    
    def get_insert_string(self,obj,table_name):
        values = [list(x.values()) for x in obj]
        
        # get the column names
        columns = [list(x.keys()) for x in obj][0]
        
        # value string for the SQL string
        values_str = ""
        
        # enumerate over the records' values
        for i, record in enumerate(values):
        
            # declare empty list for values
            val_list = []
           
            # append each value to a new list of values
            for v, val in enumerate(record):
                #print("val", val)
                #print("type", type(val))
                #type(val) == float
                #math.isnan(val)
                #if type(val) == str:
                #    val = str(Json(val)).replace('"', '')
                if (type(val) == float):
                    if math.isnan(val):
                        val = "NULL"
                elif (((type(val) == float) or (type(val) == int)) and (math.isnan(val)==False)):
                    pass
                elif (val  == None):
                    val = "NULL"
                else:
                    val = str(Json(val)).replace('"', '')
                val_list += [ str(val) ]
        
        
            # put parenthesis around each record string
            values_str += "(" + ', '.join( val_list ) + "),\n"
        
        # remove the last comma and end SQL with a semicolon
        values_str = values_str[:-2]
        
        # concatenate the SQL string
        """
        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
            table_name,
            ', '.join(columns),
            values_str
        )
        """
        
        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
            table_name,
            ', '.join(columns),
            values_str+
            ";"
        )
        return sql_string


if __name__ == "__main__":
    pass


