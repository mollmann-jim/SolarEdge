#!/usr/bin/python
from se_api import Solaredge, _fmt_date
import testdata
import datetime as dt
import sqlite3

DBname = '/home/jim/tools/SolarEdge//SolarEdge.sql'
debug = False
translate = {'consumption' : 'Consumption', 'production' : 'Production' , \
             'purchased' : 'Purchased', 'feedin' : 'FeedIn', \
             'selfconsumption' : 'SelfConsumption', 'timestamp' : 'Date'}
columns = ['consumption', 'production', 'purchased', 'feedin', \
                        'selfconsumption', 'timestamp']
Columns = ['Consumption', 'Production', 'Purchased', 'FeedIn', \
                        'SelfConsumption', 'Timestamp']
def getPrev7days():
    now   = dt.datetime.now()
    end   = now.replace(hour = 23, minute = 59, second = 0, microsecond = 0) - \
        dt.timedelta(days = 1)
    start = now.replace(hour = 0, minute = 0, second =0, microsecond = 0) - \
        dt.timedelta(days = 7)
    return start, end

def getYesterday():
    now   = dt.datetime.now()
    day   = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days = 1)
    yesterday = dt.datetime.date(day)
    return str(yesterday)
    
def getThisWeek():
    now   = dt.datetime.now()
    end   = now.replace(hour = 23, minute = 59, second = 0, microsecond = 0) - \
        dt.timedelta(days = 1)
    start = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days = now.weekday())
    #print('this week', start, end)
    return start, end

def getLastWeek():
    now   = dt.datetime.now()
    end   = now.replace(hour = 23, minute = 59, second = 0, microsecond = 0) - \
        dt.timedelta(days = 1 + now.weekday())
    start = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days = 7 + now.weekday())
    #print('last week', start, end)
    return start, end

def getThisMonth():
    now   = dt.datetime.now()
    end   = dt.datetime(now.year, now.month + 1, 1) - dt.timedelta(seconds = 1)
    start = now.replace(day = 1, hour = 0, minute = 0, second = 0, microsecond = 0)
    #print('this month', start, end)
    return start, end

def getLastMonth():
    now   = dt.datetime.now()
    end   = dt.datetime(now.year, now.month, 1) - dt.timedelta(seconds = 1)
    month = now.month - 1
    if month < 1: month = 12
    start = dt.datetime(now.year, month, 1)
    #print('last month', start, end)
    return start, end

def getYear(year):
    end   = dt.datetime(year = year + 1, month = 1, day = 1) - \
        dt.timedelta(seconds = 1)
    start =  dt.datetime(year = year, month = 1, day = 1)
    #print(year, ':', start, end)
    return start, end

def fmtLine(tag, row):
    line = tag + ': (none)'
    if row['Consumption']:
        header  =  '{:>19s}'.format(tag)
        prod    = ' {:>10.2f}'.format(row['Production'] / 1000)
        used    = ' {:>11.2f}'.format(row['Consumption'] / 1000)
        percent = ' {:>8.1f}'.format(row['Production'] / row['Consumption'] * 100)
        sold    = ' {:>8.2f}'.format(row['FeedIn'] / 1000)
        bought  = ' {:>8.2f}'.format(row['Purchased'] / 1000)
        selfcon = ' {:>9.2f}'.format(row['SelfConsumption'] / 1000)
        line = header + prod + used + percent + sold + bought + selfcon
    return line

def printHeader():
    #       Average This Month 1234567890 12345678901 12345678 12345678 12345678 123456789
    #        mm/dd/yyyy        1234567890 12345678901 12345678 12345678 12345678 123456789
    print('                    Production Consumption              Sold   Bought Prod-Used')
    print('             Period        KWh         KWh   % Prod      KWh      KWh       KWh')
    print('------------------- ----------  ---------- -------- -------- -------- ---------')
    #...... Maximum This Month      63.90       86.75     73.7    37.35    51.96     39.16
    
def makeSection(c, period, title):
    start, end = period
    select_sum = 'SELECT TOTAL(production) AS production, TOTAL(consumption) AS consumption, ' +\
        'TOTAL(feedin) AS feedin, TOTAL(purchased) AS purchased, ' +\
        'TOTAL(selfconsumption) AS selfconsumption FROM energy_day WHERE ' +\
        'timestamp >= ? AND timestamp <= ?;'
    select_avg = 'SELECT AVG(production) AS production, AVG(consumption) AS consumption, ' +\
        'AVG(feedin) AS feedin, AVG(purchased) AS purchased, ' +\
        'AVG(selfconsumption) AS selfconsumption FROM energy_day WHERE ' +\
        'timestamp >= ? AND timestamp <= ?;'
    select_min = 'SELECT MIN(production) AS production, MIN(consumption) AS consumption, ' +\
        'MIN(feedin) AS feedin, MIN(purchased) AS purchased, ' +\
        'MIN(selfconsumption) AS selfconsumption FROM energy_day WHERE ' +\
        'timestamp >= ? AND timestamp <= ?;'
    select_max = 'SELECT MAX(production) AS production, MAX(consumption) AS consumption, ' +\
        'MAX(feedin) AS feedin, MAX(purchased) AS purchased, ' +\
        'MAX(selfconsumption) AS selfconsumption FROM energy_day WHERE ' +\
        'timestamp >= ? AND timestamp <= ?;'
    c.execute(select_sum, (start, end))
    result = c.fetchall()
    for record in result:
        print(fmtLine('Total ' + title, record))
    c.execute(select_avg, (start, end))
    result = c.fetchall()
    for record in result:
        print(fmtLine('Average ' + title, record))
    c.execute(select_min, (start, end))
    result = c.fetchall()
    for record in result:
        print(fmtLine('Minimum ' + title, record))
    c.execute(select_max, (start, end))
    result = c.fetchall()
    for record in result:
        print(fmtLine('Maximum ' + title, record))  
    print(' ')
    
def main():
    db = sqlite3.connect(DBname)
    db.row_factory = sqlite3.Row
    c = db.cursor()
    
    start, end = getPrev7days()

    printHeader()
    
    select = 'SELECT * FROM energy_day WHERE timestamp >= ? AND timestamp <= ? ' +\
        'ORDER BY timestamp DESC;'
    c.execute(select, (start, end))
    result = c.fetchall()
    for record in result:
        date = record['timestamp'].split(' ')[0]
        print(fmtLine(date, record))
    print(' ')

    printHeader()
    select_hr = 'SELECT TOTAL(production) AS production, TOTAL(consumption) AS consumption, ' +\
        'TOTAL(feedin) AS feedin, TOTAL(purchased) AS purchased, timestamp, ' +\
        'TOTAL(selfconsumption) AS selfconsumption FROM energy_details WHERE ' +\
        'DATE(timestamp) == ? GROUP BY STRFTIME("%H", timestamp) ORDER BY timestamp;'
    yesterday = getYesterday()
    c.execute(select_hr, (yesterday,))
    result = c.fetchall()
    for record in result:
        print(fmtLine(record['timestamp'], record))
    print(' ')

    printHeader() 
    makeSection(c, getThisWeek(),  'This Week')
    makeSection(c, getLastWeek(),  'Last Week')
    makeSection(c, getThisMonth(), 'This Month')
    makeSection(c, getLastMonth(), 'Last Month')
        
    select_min_yr = 'SELECT min(timestamp) AS min FROM energy_day;'
    c.execute(select_min_yr)
    min = c.fetchone()
    first = dt.datetime.strptime(min['min'], '%Y-%m-%d %H:%M:%S')
    select_max_yr = 'SELECT max(timestamp) AS max FROM energy_day;'
    c.execute(select_max_yr)
    max = c.fetchone()
    last = dt.datetime.strptime(max['max'], '%Y-%m-%d %H:%M:%S')
    for year in range(last.year, first.year - 1, -1):
        makeSection(c, getYear(year), '{:4d}'.format(year))

    
if __name__ == '__main__':
  main()
