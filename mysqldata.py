# -*- coding: utf-8 -*-
"""
Created on Thu August 4, 2015

@author: ziyan

Version: 1d
Updates:
1. added a new function (get opening print time)
"""

import MySQLdb
import pandas as pd
import time
import datetime as dt
import numpy as np




path = 'C:/Users/ziyan/Documents/Project/project4/'    # store directory



""" 
1. build up MySQL connection 
"""
def connection():
    con = MySQLdb.connect (host = "108.60.133.15", 
                           user = "ziyan", 
                           passwd = "123", 
                           db = "coastal")
    
    return con
#c = con.cursor()



""" 
2. retrieve universe list of symbol
"""
# Get universe list of symbol
testDate = '2013-01-02'     
query1 = ('select Symbol, DollarAdv from adv '
          'where Date = "' + testDate + '" and DollarAdv > 5000;')
con = connection()
sym_list = pd.read_sql(query1, con)



""" 
3. retrieve opening and closing prints
"""
def get_earnings_info(symbol, start_date, end_date):
    query2 = ('select Symbol, date, time from earnings '
              'where date between "' + start_date + '" and "' + end_date + '"  and '
              'Symbol = "' + symbol + '";')
    con = connection()
    earnings_df = pd.read_sql(query2, con)
    
    return earnings_df


def get_daily_info(symbol, start_date, end_date):
    
    query3 = ('select * from daily '
              'where Symbol = "' + symbol + '" and '
              'Date between "' + start_date + '" and "' + end_date + '";')
    con = connection()
    daily_df = pd.read_sql(query3, con)
    
    # calculate adjusted open, high and low
    daily_df['AdjustOpen'] = daily_df['AdjustClose'] / daily_df['Close'] * daily_df['Open']
    daily_df['AdjustHigh'] = daily_df['AdjustClose'] / daily_df['Close'] * daily_df['High']
    daily_df['AdjustLow'] = daily_df['AdjustClose'] / daily_df['Close'] * daily_df['Low']
    
    return daily_df



def get_open_time(symbol, date):
    """ get stock opening print time """
    
    con = connection()
    query = ('select * from exgp where Symbol = "' + symbol + '" and Date = "' + date + '";')
    open_df = pd.read_sql(query, con).iloc[0]
    
    sec = open_df.Time / np.timedelta64(1, 's') + 1     # round to next second because no access to ms precision 
    open_time = dt.time(int(sec/3600), int(sec%3600/60), int(sec%3600%60)).strftime('%H:%M:%S')
    
    return open_time




def get_beta(symbol, start_date, end_date):
    
    query4 = ('select * from beta '
              'where Symbol = "' + symbol + '" and '
              'Date between "' + start_date + '" and "' + end_date + '";')
    con = connection()
    beta_df = pd.read_sql(query4, con)
    
    return beta_df



def get_spy(start_date, end_date):
    
    query5 = ('select * from daily '
              'where Symbol = "SPY" and '
              'Date between "' + start_date + '" and "' + end_date + '";')
    con = connection()
    spy_df = pd.read_sql(query5, con)
    
    return spy_df

"""
4. Main
"""
#earnings = pd.DataFrame([[]])
#start_time = time.time()
#for sym in sym_list.Symbol:
#    
#    earnings_df = get_earnings_info(sym, '2013-01-01', '2013-12-31')
#    earnings = earnings.append(earnings_df, ignore_index=True)
#    
#    sym_daily_df = get_daily_info(sym, '2012-01-01', '2014-12-31')
#    sym_daily_df.to_csv(path + 'Data/' + sym + '_daily.csv', index=False)
#    
#    beta_df = get_beta(sym, '2013-01-01', '2014-12-31')
#    beta_df.to_csv(path + 'Data/' + sym + '_beta.csv', index=False)
#    
#    print sym
#
#earnings.to_csv(path + 'Universe/earnings.csv', index=False)
#spy_df = get_spy('2013-01-01', '2014-12-31')
#spy_df.to_csv(path + 'Data/spy_daily.csv', index=False)
#print str(time.time()-start_time)
