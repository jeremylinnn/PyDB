# -*- coding: utf-8 -*-
"""
Created on Thu August 5, 2015

@author: ziyan

Version: 1b
Updates:
1. download_price added two parameters: start_time, end_time
"""

#import config_1 as config
import datetime
import os, os.path
import time
import pandas as pd
from qpython import qconnection


#version = config.version
#universe_path = config.universe_path
#data_path = config.data_path
#earnings = pd.read_csv(universe_path + 'earnings.csv') 

version = '1a'

""" 
1. build up KDB connection 
"""
def connection():
    
    # connect to localhost first
    q = qconnection.QConnection(host = 'localhost', port = 5000)
    # The connection is initialized explicitly by calling the open() method.
    q.open()    
    # connect to the KDB 
    q.async('h: hopen`:108.60.133.23:5003:ziyan:kxGuest88;')  
    return q



""" 
2. functions
""" 
    
def timeInRange(x, start, end):
    if x >= start and x <= end:
        return True
    else:
        return False
        
        
def download_price(symbol, date, start_time, end_time, data_path):
    """ get nbbo ask bid price on specific date """
    
    if not date[4] == '.':
        temp = date.split('-')
        date = '.'.join(temp)
    
    version = '1a'
    
    # connect kdb server
    q = qconnection.QConnection(host = 'localhost', port = 5000)
    # The connection is initialized explicitly by calling the open() method.
    q.open()    
    # connect to the KDB 
    q.async('h: hopen`:108.60.133.23:5003:ziyan:kxGuest88;')  
        
    q_head = '.hnd.h[`eq.hdb] '
    q_vari1 = 'temptable1'  # save data to variable in localhost
    q_body1 = '\\"select from nbbo where date = ' + date + ', ' + \
              'sym = `' + symbol + ', time within ' + \
              start_time + ' ' + end_time + '\\"'
    q_get1 = q_vari1 + ':h(\"' + q_head + q_body1 + '\")'
    q.async(q_get1)
    
    # save data to local csv file
    file_name = symbol + '_' + date + '_price_' + version + '.csv'
    path = data_path + symbol + '/' + file_name
    q_save = '`:' + path + ' 0:.h.tx[`csv;' + q_vari1 + ']'
    q.async(q_save)
    
    # suspend
    sleep(0.01, path)
    
    return path
    
    
def getTradePrice(symbol, date, time, data_path):
    """ get trade price on specific date """
    
    # connect kdb server
    q = connection()
        
    q_head = '.hnd.h[`eq.hdb] '
    q_vari1 = 'temptable1'  # save data to variable in localhost
    q_body1 = ('\\"select from trade '
               'where date = ' + date + ', '
               'sym = `' + symbol + '\\"')
    q_get1 = q_vari1 + ':h(\"' + q_head + q_body1 + '\")'
    q.async(q_get1)
    
    # save data to local csv file
    file_name = symbol + '_' + date + '_tradeprice_' + version + '.csv'
    q_save = '`:' + data_path + symbol + '/' + file_name + ' 0:.h.tx[`csv;' + q_vari1 + ']'
    q.async(q_save)
    
    return True



def kdb2csv(symbol, date1, path):
    """ export trade data to csv files """
    q = connection()
    # retrieve data to qVari in q
    qHead = '.hnd.h[`core.hdb] '
    # use parenthesis to construct long string
    # use 2 back slashes to keep \" 
    qBody = ('\\"(select sym, date, time, price from trade '
             'where date = ' + date1 + ', sym = `' + symbol + '\\"') 
    qVari = 'tempData'  # save data to local variable in q
    qGet  = qVari + ':h(\"' + qHead + qBody + '\")'
    q.async(qGet)

    # save data to local csv file
    fName = symbol + '_' + date1 + '_price.csv'
    qSave = '`:' + path + fName + ' 0:.h.tx[`csv;' + qVari + ']'
    q.async(qSave)


def sleep(seconds, path):
    """ sleep until target file updated. In case data overlap """
      
    i = 0
#    while(not os.path.isfile(path)):   # exist?
#        time.sleep(seconds)
#        i += 1
    
    if not os.path.isfile(path):
        while not os.path.isfile(path):   # exist?
            time.sleep(seconds)
            i += 1
            if i*seconds > 300:     # retrieving data from KDB has no response
                return False
    else:
        last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(path))
        while (datetime.datetime.now()-last_modified).seconds > 60:     # modified just now?
            time.sleep(seconds)
            i += 1
            last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(path))
            if i*seconds > 300:
                return False
    
    print '------------------------------------------------'
    print 'It took about ' + str(i*seconds) + ' seconds'
    print '    finished at ' + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') 
    print '------------------------------------------------'
    return True


def file_validate(symbol, date, file_path):
    try:
        check_row = pd.read_csv(file_path, nrows=1)
    except:
        print symbol + ' ' + date + ' does not exist'
        return False
        
    if (check_row['sym'].iloc[0] == symbol):
        return True
    else:
        print symbol + ' needs to be updated...'
        return False
        


""" 
3. main()
"""

#if not os.path.isfile(path + 'kdb_log.txt'):
    
"""
for i in range(len(earnings)):
#for i in range(3):
#for i in range(355, len(earnings)):
    symbol = earnings['Symbol'][i]
    date = earnings['Date'][i]
    time1 = earnings['Time'][i]
    file_name = symbol + '_' + date + '_price1.csv'
    file_path = path + symbol + '/' + file_name
#    if (not file_validate(symbol, date, file_path)):   # get bid ask price
#        getPrice(symbol, date, time1)
#        # to have the sleep function worked, delete all historical files!!
#        sleep(earnings['Symbol'][i], earnings['Date'][i], 0.05, file_path)

#    # download trade price
#    file_name1 = symbol + '_' + date + '_trade_price.csv'
#    file_path1 = path + symbol + '/' + file_name1
#    getTradePrice(symbol, date, time1)
    getPrice(symbol, date, time1)
    if not sleep(symbol, date, 0.01, file_path):
        print symbol + ' ' + date + ' no response'
        print '------------------------------------------'
"""







