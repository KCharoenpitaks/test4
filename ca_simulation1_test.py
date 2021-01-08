import numpy as np
import pandas as pd
import json
import psycopg2
import requests
#from db_util import mssql_simulation_database

from db_util_postgres import mssql_database_simulationdb_postgres
from psycopg2.extras import Json
import math
import pymssql
import datetime
import time
from APIclass import APIDATA_PRODUCTION, APIDATA

"""
api_url = "/api/v1/dailyreport/tradetrx/view/2020-05-13"
#payload = {
#             "start_period": "2020-05-13",
#             "end_period": "2020-05-14"
#          }
payload = {
             "asof_date": "2020-05-13"
          }

api = APIDATA_PRODUCTION()

df = api.get_api_data(api_url,payload)
"""

##################### TO DO USE deque to store last trading day


def split_insert(df):
    tablename = 'ca_simulationdb'
    db1 = mssql_database_simulationdb_postgres(tablename)
    #db1.drop_table(tablename)
    #db1.create_table(tablename)
    try:
        db1.drop_table('t_bp_creditassessment_temp5')
    except:
        pass
    max_len = 1000
    steps = int(len(df)/max_len) + 1
    split = int(len(df)/steps)+1
   
    for i in range(steps):
        df_temp_split = df[(df.index < split*(i+1)) & (df.index >= split*(i))]
        df_temp_split = df_temp_split.reset_index(drop=True)
        obj = df_temp_split.to_json(orient='index')
        #obj = df_temp_split.to_dict(orient='records')
        db1.update_table(tablename,obj)

#from collections import defaultdict,deque
#dict_mem = defaultdict(lambda: [np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]) # Create dict to be memory with Fixed Range = 23

"""
def insert_accumulative_limit(dict_mem, bond,today_m2m):
    dict_mem[str(bond)].insert(0,today_m2m)
    dict_mem[str(bond)] = dict_mem[str(bond)][:23] # Reserve list 0:22 --> 0 for today m2m, 1 for yesterday
    return dict_mem
    """


def get_data_from_postgres(date):
    
    
    conn = psycopg2.connect(
        host='192.168.70.40',
        port='5432',
        dbname='credit_assessment',
        user='postgres',
        password='password'
    )
    
    
    cur = conn.cursor()
    cur.execute("""SELECT * FROM ca_simulationdb WHERE asof = '"""+date+"""';""")
    #cur.execute("DROP TABLE ca_simulationdb;")
    result = cur.fetchall()
    column_names = [i[0] for i in cur.description]
    df_read = pd.DataFrame(result, columns = column_names)

    conn.commit()
    cur.close()
    conn.close()
    return df_read
    

def get_previous_data_from_db(prev_date):
    
    conn = psycopg2.connect(
        host='192.168.70.40',
        port='5432',
        dbname='credit_assessment',
        user='postgres',
        password='password'
    )
    cur = conn.cursor()
    sql_string = """
                    SELECT a.symbol,
                        a.today_m2m_static_spread,
                        a.issuer_type_code,
                        a.attribute,
                        a.limit_5bd,
                        a.last_5bd_spread,
                        a.used_5bd,
                        a.percent_breach_5bd,
                        a.limit_10bd,
                        a.last_10bd_spread,
                        a.used_10bd,
                        a.percent_breach_10bd,
                        a.limit_22bd,
                        a.last_22bd_spread,
                        a.used_22bd,
                        a.percent_breach_22bd
                    FROM ca_simulationdb as a
                    WHERE asof = '"""+prev_date+"""'
                """
    
    cur.execute(sql_string)
    #cur.execute("SELECT * FROM ca_simulationdb;")
    #cur.execute("DROP TABLE ca_simulationdb;")
    result = cur.fetchall()
    column_names = [i[0] for i in cur.description]
    df_read_ = pd.DataFrame(result, columns = column_names)
    conn.commit()
    cur.close()
    conn.close()
    return df_read_


def get_trade_sum(date):
    ### get trading data
    today_str = date
    today_str = '2020-12-18'
    api_url = r'https://192.168.50.41:8443/api/v1/dailyreport/tradetrx/view/%(date)s' % {'date': today_str}
    r = requests.get(api_url, verify=False)
    list_data = json.loads(r.content)["result"]["data"]
    trade_summary_data_df = pd.DataFrame(list_data)
    return trade_summary_data_df

def get_m2m(date):
    today_str = date
    api_url = r"https://192.168.20.228/api/v1/mtm/plain-vanilla"
    #api_url = r'https://192.168.50.41:8443/api/v1/dailyreport/tradetrx/view'
       
    
    api = APIDATA()
    head = api.generate_token()  
    payload = {
                 "start_period": today_str,
                 "end_period": today_str
              }
    
    list_data = api.post_data(api_url, payload, head)
                            
    json_data = json.loads(list_data)["result"]["data"]
    m2m_data_df = pd.DataFrame(json_data)
    return m2m_data_df

def fill_table(date):
    #date = '2020-12-16'
    
    df_trade_sum = get_trade_sum(date)
    df_m2m = get_m2m(date)
    
    df_m2m = df_m2m[['asof','symbol','static_spread','last_trade_date','ttm']]
    df_trade_sum = df_trade_sum[['asof','symbol','attribute']]
    df_m2m = df_m2m.rename(columns={'last_trade_date':'trade_date', 'static_spread':'today_m2m_static_spread'})
    
    df_m2m['asof'] = pd.to_datetime(df_m2m['asof'])
    df_m2m['trade_date'] = pd.to_datetime(df_m2m['trade_date'])
    df_trade_sum['asof'] = pd.to_datetime(df_trade_sum['asof'])
    
    df_merge = pd.merge(df_m2m,df_trade_sum , how='left', left_on =['asof','symbol'], right_on =['asof','symbol'])
    
    #df_merge = df_merge.replace(np.nan, '', regex=True)
    
    
    df_merge.loc[df_merge['asof'] ==df_merge['trade_date'] , 'is_traded_today'] = 'true'
    
    df_merge.loc[df_merge['trade_date'].isna(),'trade_date'] =""
    df_merge['asof'] = df_merge['asof'].astype(str)
    df_merge['trade_date'] = df_merge['trade_date'].astype(str)
    
    df_merge['attribute'] = df_merge['attribute'].replace(np.nan, '', regex=True)
    df_merge['is_traded_today'] = df_merge['is_traded_today'].replace(np.nan, '', regex=True)
    df_merge['attribute'] = df_merge['attribute'].astype(str)
    df_merge['is_traded_today'] = df_merge['is_traded_today'].astype(str)
    
    return df_merge

def get_accumlimit():
    
    conn = psycopg2.connect(
        host='192.168.70.40',
        port='5432',
        dbname='credit_assessment',
        user='postgres',
        password='password'
    )
    cur = conn.cursor()
    
    sql_string = """

                    WITH date_rank AS
                    (
                    	SELECT symbol,
                    			--CAST('2020-12-17' AS DATE) as asof,
                    			asof, 
                    			today_m2m_static_spread,
                    			rank() over (partition by symbol order by asof desc) as ranking
                    	FROM ca_simulationdb
                    ),
                    
                    temp_date_symbol_1 AS
                    (
                    	SELECT a.symbol,
                    			 a.asof as asof
                    	FROM date_rank as a
                    	WHERE a.ranking = 1
                    		AND a.asof = '2020-12-17'
                    ),
                    
                    temp_date_symbol_5 AS
                    (
                    	SELECT DISTINCT a.symbol,
                    			b.last_5bd as last_5bd
                    	FROM date_rank as a
                    	LEFT JOIN (
                    				SELECT symbol, asof as last_5bd
                    				FROM date_rank
                    				WHERE ranking = 5
                    			) as b
                    	ON a.symbol = b.symbol
                    	WHERE b.last_5bd IS NOT NULL
                    ),
                    
                    temp_date_symbol_10 AS
                    (
                    	SELECT DISTINCT a.symbol,
                    			b.last_10bd as last_10bd
                    	FROM date_rank as a
                    	LEFT JOIN (
                    				SELECT symbol, asof as last_10bd
                    				FROM date_rank
                    				WHERE ranking = 10
                    			) as b
                    	ON a.symbol = b.symbol
                    	WHERE b.last_10bd IS NOT NULL
                    ),
                    
                    temp_date_symbol_22 AS
                    (
                    	SELECT DISTINCT a.symbol,
                    			b.last_22bd as last_22bd
                    	FROM date_rank as a
                    	LEFT JOIN (
                    				SELECT symbol, asof as last_22bd
                    				FROM date_rank
                    				WHERE ranking = 22
                    			) as b
                    	ON a.symbol = b.symbol
                    	WHERE b.last_22bd IS NOT NULL
                    ),
                    
                    temp_date_symbol_sum1 AS 
                    (
                    	SELECT a.*,
                    			b.last_5bd
                    	FROM temp_date_symbol_1 as a
                    	LEFT JOIN temp_date_symbol_5 as b 
                    	ON a.symbol = b.symbol
                    ),
                    
                    temp_date_symbol_sum2 AS 
                    (
                    	SELECT a.*,
                    			b.last_10bd
                    	FROM temp_date_symbol_sum1 as a
                    	LEFT JOIN temp_date_symbol_10 as b 
                    	ON a.symbol = b.symbol
                    ),
                    
                    temp_date_symbol_sum3 AS 
                    (
                    	SELECT a.*,
                    			b.last_22bd
                    	FROM temp_date_symbol_sum2 as a
                    	LEFT JOIN temp_date_symbol_22 as b 
                    	ON a.symbol = b.symbol
                    ),
                    
                    temp_date_symbol_sum4 AS 
                    (
                    	SELECT a.*,
                    			b.today_m2m_static_spread 
                    	FROM temp_date_symbol_sum3 as a
                    	LEFT JOIN date_rank as b 
                    	ON a.symbol = b.symbol
                    		AND ranking = 1
                    		--AND a.asof = b.asof
                    ),
                    
                    temp_date_symbol_sum5 AS 
                    (
                    	SELECT a.*,
                    			b.today_m2m_static_spread as last_5bd_spread
                    	FROM temp_date_symbol_sum4 as a
                    	LEFT JOIN date_rank as b 
                    	ON a.symbol = b.symbol
                    		AND ranking = 5
                    		--AND a.last_5bd = b.asof
                    ),
                    
                    temp_date_symbol_sum6 AS 
                    (
                    	SELECT a.*,
                    			b.today_m2m_static_spread as last_10bd_spread
                    	FROM temp_date_symbol_sum5 as a
                    	LEFT JOIN date_rank as b 
                    	ON a.symbol = b.symbol
                    		AND ranking = 10
                    		--AND a.last_10bd = b.asof
                    ),
                    
                    temp_date_symbol_sum7 AS 
                    (
                    	SELECT a.*,
                    			b.today_m2m_static_spread as last_22bd_spread
                    	FROM temp_date_symbol_sum6 as a
                    	LEFT JOIN date_rank as b 
                    	ON a.symbol = b.symbol
                    		AND ranking = 22
                    		--AND a.last_22bd = b.asof
                    ),
                    
                    temp1 AS 
                    (
                    	SELECT a.asof,
                    			a.symbol,
                    			a.is_traded_today,
                    			a.prev_trade_static_spread,
                    			a.adj_spread,
                    			a.prev_m2m_static_spread,
                    			a.today_m2m_static_spread,
                    			a.limit_5bd,
                    			b.last_5bd_spread,
                    			a.used_5bd,
                    			a.percent_breach_5bd,
                    			a.limit_10bd,
                    			b.last_10bd_spread,
                    			a.used_10bd,
                    			a.percent_breach_10bd,
                    			a.limit_22bd,
                    			b.last_22bd_spread,
                    			a.used_22bd,
                    			a.percent_breach_22bd
                    	FROM ca_simulationdb as a
                    	LEFT JOIN temp_date_symbol_sum7 as b
                    	ON a.asof = b.asof
                    		AND a.symbol = b.symbol
                    )
                    
                    UPDATE ca_simulationdb
                    SET 
                    	asof = s.asof,
                    	symbol = s.symbol,
                    	is_traded_today = s.is_traded_today,
                    	prev_trade_static_spread = s.prev_trade_static_spread,
                    	adj_spread = s.adj_spread,
                    	prev_m2m_static_spread = s.prev_m2m_static_spread,
                    	today_m2m_static_spread = s.today_m2m_static_spread,
                    	limit_5bd = s.limit_5bd,
                    	last_5bd_spread = s.last_5bd_spread,
                    	used_5bd = s.used_5bd,
                    	percent_breach_5bd = s.percent_breach_5bd,
                    	limit_10bd = s.limit_10bd,
                    	last_10bd_spread = s.last_10bd_spread,
                    	used_10bd = s.used_10bd,
                    	percent_breach_10bd = s.percent_breach_10bd,
                    	limit_22bd = s.limit_22bd,
                    	last_22bd_spread = s.last_22bd_spread,
                    	used_22bd = s.used_22bd,
                    	percent_breach_22bd = s.percent_breach_22bd
                    FROM temp1 as s
                    WHERE s.asof = ca_simulationdb.asof
                    	AND s.symbol = ca_simulationdb.symbol
                    	
                """
    cur.execute(sql_string)
    #cur.execute("SELECT * FROM ca_simulationdb;")
    #cur.execute("DROP TABLE ca_simulationdb;")
    conn.commit()
    cur.close()
    conn.close()


def simulation_1day(date):

        
    ########### Read data from Database for startup data (First round)
    #start_time = time.time()
    #today = datetime.date.today()
    
    #start_date = str(today)
    start_date = date
    
    
    #start_date = '2020-12-18'
    #start_date = "'2019-06-14'"  #"'2018-12-28'" #"'2019-03-28'" ###########################
    prev_date = str((pd.to_datetime(start_date) - pd.DateOffset(1)).date())
    #prev_date = "'2019-06-13'" #"'2018-12-27'" #"'2019-03-27'"
    
    df_read = fill_table(prev_date)
    #df_read.loc[df_read['trade_date']== '','trade_date'] = None
    #df_read.loc[df_read['trade_date'].notnull(),'trade_date'] = df_read.loc[df_read['trade_date'].notnull(),'trade_date'].astype(str)

    #split_insert(df_read)
    
    #df_read = get_bond_data(prev_date)
    df_read_prevdb = get_previous_data_from_db(prev_date)
    
    df_read_prevdb = df_read_prevdb.rename(columns={"today_m2m_static_spread": "prev_m2m_static_spread"})
    
    df_starting_data_ = df_read[['symbol','today_m2m_static_spread','attribute']]
    df_starting_data_ = df_starting_data_.rename(columns={"today_m2m_static_spread": "prev_m2m_static_spread"})
    
    df_starting_data = pd.merge(df_starting_data_, df_read_prevdb, how = 'outer', left_on = 'symbol', right_on = 'symbol' )
    
    df_starting_data.loc[df_starting_data['prev_m2m_static_spread_x'].notnull(),'prev_m2m_static_spread'] = df_starting_data.loc[df_starting_data['prev_m2m_static_spread_x'].notnull(),'prev_m2m_static_spread_x'].astype(float)
    df_starting_data.loc[df_starting_data['prev_m2m_static_spread_y'].notnull(),'prev_m2m_static_spread'] = df_starting_data.loc[df_starting_data['prev_m2m_static_spread_y'].notnull(),'prev_m2m_static_spread_y'].astype(float)
    df_starting_data = df_starting_data.drop(columns=['prev_m2m_static_spread_y', 'prev_m2m_static_spread_x'])
    
    df_starting_data.loc[df_starting_data['attribute_x'].notnull(),'attribute'] = df_starting_data.loc[df_starting_data['attribute_x'].notnull(),'attribute_x'].astype(str)
    df_starting_data.loc[df_starting_data['attribute_y'].notnull(),'attribute'] = df_starting_data.loc[df_starting_data['attribute_y'].notnull(),'attribute_y'].astype(str)
    df_starting_data = df_starting_data.drop(columns=['attribute_x', 'attribute_y'])
    ####################################################
    
    #print("2")
    #dbname = 'thaibma_bp'
    #tablename = 't_bp_creditassessment_simulationdb'#########################
    
    ########
    
    Date = start_date #start_date_ #'2019-02-05' #'2018-12-28'#'2019-03-28'
    #Date = '2020-12-25'
    """
    #Date = '2020-12-17'
    url = r'http://192.168.70.40:5001/ca_3/post'
    #url = r'http://192.168.10.151:5000/ca_3/post'
    json_ = requests.post(url, json={'asof_date':Date,'lambda':0.7})
    df = pd.read_json(json_.text)
    """
    
    #url = r'http://192.168.70.40:5001/ca_3/web?asof_date=2020-12-17&lambda=0.7'
    url = r'http://192.168.70.40:5001/ca_3/web?asof_date='+Date+'&lambda=0.7'
    r = requests.get(url)
    
    df = pd.read_html(r.content)[0]
    
    
    
    ######
    
    """
    import pickle
    
    with open('ca_df.pickle', 'rb') as fp:
        df = pickle.load(fp)
    df = df.drop(columns={'Unnamed: 0'})
    """
    ##########################################################################################
    
    df_update = df_starting_data.merge(df, how = 'left', left_on = 'symbol', right_on = 'symbol' )
    #df_update = pd.merge(df_starting_data,df,how='left', left_on = 'symbol', right_on = 'sydmbol')
    df_update = df_update[~(df_update['symbol'].duplicated(keep='last'))]
    df_update['asof'] = start_date #start_date_
    
    df_update['limit_5bd'] = 5
    df_update['limit_10bd'] = 10
    df_update['limit_22bd'] = 30
    
    
    df_update = df_update.reset_index(drop=True)
    
    #################
    ##############
    
    df_update.loc[df_update['adj_spread'].isnull(),'adj_spread'] = 0
    
    df_update['today_m2m_static_spread'] = df_update['prev_m2m_static_spread'].astype(float) + df_update['adj_spread']
    
    df_update = df_update[['asof', 'cluster_id','box_id','symbol','issuer','sector_abbr','rating','ttm','issuer_type_code','attribute'
                           ,'is_traded_today','is_pivot','trade_date','prev_bd','prev_trade_static_spread','diff_static_spread'
                           ,'adj_spread_pre','corr_factor','adj_spread','status','prev_m2m_static_spread','today_m2m_static_spread'
                           ,'limit_5bd','last_5bd_spread','used_5bd','percent_breach_5bd'
                           ,'limit_10bd','last_10bd_spread','used_10bd','percent_breach_10bd'
                           ,'limit_22bd','last_22bd_spread','used_22bd','percent_breach_22bd']]
    
    # apply accumulative limit
    
    df_update['trade_date'][df_update['trade_date']=='NaT'] = None
    df_update['trade_date'][df_update['trade_date']=='None'] = None
    df_update['prev_bd'][df_update['prev_bd']=='None'] = None
    df_update['prev_bd'][df_update['prev_bd']=='NaT'] = None
    
    split_insert(df_update)
    
    get_accumlimit()
    
    df_update = get_data_from_postgres(start_date)
    
    
    df_update.loc[((df_update['last_5bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & (df_update['status'] != "decay_mid")),'today_m2m_static_spread'] = df_update.loc[((df_update['last_5bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & (df_update['status'] != "decay_mid")),'today_m2m_static_spread'].clip(df_update['last_5bd_spread']-df_update['limit_5bd'],df_update['last_5bd_spread']+df_update['limit_5bd'])
    df_update.loc[((df_update['last_10bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & (df_update['status'] != "decay_mid")),'today_m2m_static_spread'] = df_update.loc[((df_update['last_10bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & (df_update['status'] != "decay_mid")),'today_m2m_static_spread'].clip(df_update['last_10bd_spread']-df_update['limit_10bd'],df_update['last_10bd_spread']+df_update['limit_10bd'])
    df_update.loc[((df_update['last_22bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & (df_update['status'] != "decay_mid")),'today_m2m_static_spread'] = df_update.loc[((df_update['last_22bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & (df_update['status'] != "decay_mid")),'today_m2m_static_spread'].clip(df_update['last_22bd_spread']-df_update['limit_22bd'],df_update['last_22bd_spread']+df_update['limit_22bd'])
    
    df_update.loc[((df_update['last_5bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & ((df_update['status'] == "outbox_corr_left")|(df_update['status'] == "outbox_corr_right"))),'today_m2m_static_spread'] = df_update.loc[((df_update['last_5bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & ((df_update['status'] == "outbox_corr_left")|(df_update['status'] == "outbox_corr_right"))),'today_m2m_static_spread'].clip(df_update['last_5bd_spread']-df_update['limit_5bd']*0.5,df_update['last_5bd_spread']+df_update['limit_5bd']*0.5)
    df_update.loc[((df_update['last_10bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & ((df_update['status'] == "outbox_corr_left")|(df_update['status'] == "outbox_corr_right"))),'today_m2m_static_spread'] = df_update.loc[((df_update['last_10bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & ((df_update['status'] == "outbox_corr_left")|(df_update['status'] == "outbox_corr_right"))),'today_m2m_static_spread'].clip(df_update['last_10bd_spread']-df_update['limit_10bd']*0.5,df_update['last_10bd_spread']+df_update['limit_10bd']*0.5)
    df_update.loc[((df_update['last_22bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & ((df_update['status'] == "outbox_corr_left")|(df_update['status'] == "outbox_corr_right"))),'today_m2m_static_spread'] = df_update.loc[((df_update['last_22bd_spread'].notnull()) & (df_update['is_traded_today'] != True) & ((df_update['status'] == "outbox_corr_left")|(df_update['status'] == "outbox_corr_right"))),'today_m2m_static_spread'].clip(df_update['last_22bd_spread']-df_update['limit_22bd']*0.5,df_update['last_22bd_spread']+df_update['limit_22bd']*0.5)
    
    df_update.loc[~(df_update['today_m2m_static_spread'].between(df_update['last_5bd_spread']-df_update['limit_5bd'],df_update['last_5bd_spread']+df_update['limit_5bd'])& df_update['today_m2m_static_spread'].between(df_update['last_10bd_spread']-df_update['limit_10bd'],df_update['last_10bd_spread']+df_update['limit_10bd'])& df_update['today_m2m_static_spread'].between(df_update['last_22bd_spread']-df_update['limit_22bd'],df_update['last_22bd_spread']+df_update['limit_22bd'])),'today_m2m_static_spread'] = df_update.loc[~(df_update['today_m2m_static_spread'].between(df_update['last_5bd_spread']-df_update['limit_5bd'],df_update['last_5bd_spread']+df_update['limit_5bd'])& df_update['today_m2m_static_spread'].between(df_update['last_10bd_spread']-df_update['limit_10bd'],df_update['last_10bd_spread']+df_update['limit_10bd'])&df_update['today_m2m_static_spread'].between(df_update['last_22bd_spread']-df_update['limit_22bd'],df_update['last_22bd_spread']+df_update['limit_22bd'])) ,'prev_m2m_static_spread']
    
    #df_update.loc[(df_update['is_traded_today'] == True),'last_5bd_spread'] = df_update.loc[(df_update['is_traded_today'] == True),'today_m2m_static_spread'] 
    #df_update.loc[(df_update['is_traded_today'] == True),'last_10bd_spread'] = df_update.loc[(df_update['is_traded_today'] == True),'today_m2m_static_spread']
    #df_update.loc[(df_update['is_traded_today'] == True),'last_22bd_spread'] = df_update.loc[(df_update['is_traded_today'] == True),'today_m2m_static_spread']
    
    #df_update.loc[df_update['last_5bd_spread'].isnull(),'today_m2m_static_spread'] = df_update['today_m2m_static_spread'].clip(df_update['prev_m2m_static_spread']-df_update['limit_5bd'],df_update['prev_m2m_static_spread']+df_update['limit_5bd'])
    #df_update.loc[df_update['last_10bd_spread'].isnull(),'today_m2m_static_spread'] = df_update['today_m2m_static_spread'].clip(df_update['prev_m2m_static_spread']-df_update['limit_10bd'],df_update['prev_m2m_static_spread']+df_update['limit_10bd'])
    #df_update.loc[df_update['last_22bd_spread'].isnull(),'today_m2m_static_spread'] = df_update['today_m2m_static_spread'].clip(df_update['prev_m2m_static_spread']-df_update['limit_22bd'],df_update['prev_m2m_static_spread']+df_update['limit_22bd'])
    
    #df_update.loc[20,'today_m2m_static_spread'] = df_update.loc[20,'today_m2m_static_spread']+1
    
    
    
    
        
    #df_update[df_update['cluster_id'].isna()]
    #df_update = df_update.merge(df_acculimit_bydate, how = 'left', left_on = 'symbol', right_on = 'symbol' )
    
    #df_update = df.merge(df_starting_data, how = 'left', left_on = 'symbol', right_on = 'symbol' )
    
    #df_update = df_update.merge(df_acculimit_bydate, how = 'left', left_on = 'symbol', right_on = 'symbol' )
    #df_fill = df_update
    
    ########### MERGE COLUME จากมาใส่
    
    #df_update = df_update[~(df_update['issue'].duplicated(keep='last'))] #TTM need update from transaction database
    
    #df_update = df_fill[~(df_fill['symbol'].str.contains('pivot'))] # no need to delete pivot
    df_update['used_5bd'] = np.abs(df_update['today_m2m_static_spread'] - df_update['last_5bd_spread']) 
    df_update['percent_breach_5bd'] = np.abs(df_update['today_m2m_static_spread'] - df_update['last_5bd_spread'])*100/df_update['limit_5bd']
    df_update['used_10bd'] = np.abs(df_update['today_m2m_static_spread'] - df_update['last_10bd_spread'])
    df_update['percent_breach_10bd'] = np.abs(df_update['today_m2m_static_spread'] - df_update['last_10bd_spread'])*100/df_update['limit_10bd']
    df_update['used_22bd'] = np.abs(df_update['today_m2m_static_spread'] - df_update['last_22bd_spread'])
    df_update['percent_breach_22bd'] = np.abs(df_update['today_m2m_static_spread'] - df_update['last_22bd_spread'])*100/df_update['limit_22bd']
    
    
    #df_update['trade_date'][df_update['trade_date']=='NaT'] = None
    df_update['trade_date'][df_update['trade_date']=='NaT'] = None
    df_update['trade_date'][df_update['trade_date']=='None'] = None
    df_update['prev_bd'][df_update['prev_bd']=='None'] = None
    df_update['prev_bd'][df_update['prev_bd']=='NaT'] = None
    
    
    df_update['asof'] = df_update['asof'].astype(str)
    df_update['prev_bd'][df_update['prev_bd']=='NaT'] = None
    df_update.loc[df_update['prev_bd'].notnull(),'prev_bd'] = df_update.loc[df_update['prev_bd'].notnull(),'prev_bd'].astype(str)
    df_update.loc[df_update['trade_date'].notnull(),'trade_date'] = df_update.loc[df_update['trade_date'].notnull(),'trade_date'].astype(str)
    df_update['prev_m2m_static_spread'] = df_update['prev_m2m_static_spread'].astype(float)
    df_update['today_m2m_static_spread'] = df_update['today_m2m_static_spread'].astype(float)
    
    df_update.loc[df_update['today_m2m_static_spread'].isnull(),'today_m2m_static_spread'] = df_update.loc[df_update['today_m2m_static_spread'].isnull(),'prev_m2m_static_spread'].astype(float)
    #df_update.loc[df_update['is_traded_today'] == 'true','today_m2m_static_spread'] = df_update.loc[df_update['is_traded_today'] == 'true','static_spread'].astype(float)
    
    df = df_update
    
    split_insert(df)


if __name__ == '__main__':
    
    today = datetime.date.today()
    start_date = str(today)
    start_date = '2020-12-21'
    simulation_1day(start_date)