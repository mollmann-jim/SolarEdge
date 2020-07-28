#!/usr/bin/python
from se_api import Solaredge, _fmt_date
import testdata
import datetime as dt
import sqlite3
from sys import path
path.append('/home/jim/tools/')
from shared import getTimeInterval

DBname = '/home/jim/tools/SolarEdge//SolarEdge.sql'
debug = False
translate = {'consumption' : 'Consumption', 'production' : 'Production' , \
             'purchased' : 'Purchased', 'feedin' : 'FeedIn', \
             'selfconsumption' : 'SelfConsumption', 'timestamp' : 'Date'}
columns = ['consumption', 'production', 'purchased', 'feedin', \
                        'selfconsumption', 'timestamp']
Columns = ['Consumption', 'Production', 'Purchased', 'FeedIn', \
                        'SelfConsumption', 'Timestamp']
def getYears(c):
    select_min_yr = 'SELECT min(timestamp) AS min FROM energy_day;'
    c.execute(select_min_yr)
    min = c.fetchone()
    first = dt.datetime.strptime(min['min'], '%Y-%m-%d %H:%M:%S')
    select_max_yr = 'SELECT max(timestamp) AS max FROM energy_day;'
    c.execute(select_max_yr)
    max = c.fetchone()
    last = dt.datetime.strptime(max['max'], '%Y-%m-%d %H:%M:%S')
    return first, last

def getYesterday():
    now   = dt.datetime.now()
    day   = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days = 1)
    yesterday = dt.datetime.date(day)
    return str(yesterday)

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
    
def makeSection(c, title, byDay = False, byMonth = False, year = None):
    start, end, name = getTimeInterval.getPeriod(title, year = year)
    #print(start, end, name)
    select_sum = 'SELECT TOTAL(production) AS production, TOTAL(consumption) AS consumption, ' \
        'TOTAL(feedin) AS feedin, TOTAL(purchased) AS purchased, ' \
        'TOTAL(selfconsumption) AS selfconsumption, date(timestamp) AS date ' \
        'FROM energy_day WHERE timestamp >= ? AND timestamp <= ? '
    select_avg = 'SELECT AVG(production) AS production, AVG(consumption) AS consumption, ' \
        'AVG(feedin) AS feedin, AVG(purchased) AS purchased, ' \
        'AVG(selfconsumption) AS selfconsumption , date(timestamp) AS date ' \
        'FROM energy_day WHERE timestamp >= ? AND timestamp <= ? '
    select_min = 'SELECT MIN(production) AS production, MIN(consumption) AS consumption, ' \
        'MIN(feedin) AS feedin, MIN(purchased) AS purchased, ' \
        'MIN(selfconsumption) AS selfconsumption , date(timestamp) AS date ' \
        'FROM energy_day WHERE timestamp >= ? AND timestamp <= ? '
    select_max = 'SELECT MAX(production) AS production, MAX(consumption) AS consumption, ' \
        'MAX(feedin) AS feedin, MAX(purchased) AS purchased, ' \
        'MAX(selfconsumption) AS selfconsumption , date(timestamp) AS date ' \
        'FROM energy_day WHERE timestamp >= ? AND timestamp <= ? ' 
    groupMonth = ' GROUP BY substr(timestamp,1, 7) ORDER BY timestamp DESC;'
    if byMonth:
        select_sum = select_sum + groupMonth
        select_avg = select_avg + groupMonth
        select_min = select_min + groupMonth
        select_max = select_max + groupMonth
    else:
        select_sum += ' ;' 
        select_avg += ' ;'
        select_min += ' ;'
        select_max += ' ;'
    c.execute(select_sum, (start, end))
    sums = c.fetchall()
    c.execute(select_avg, (start, end))
    avgs = c.fetchall()
    c.execute(select_min, (start, end))
    mins = c.fetchall()
    c.execute(select_max, (start, end))
    maxs = c.fetchall()
    titles = ['Total ' + name,' Average ' + name, 'Minimum ' + name, 'Maximum ' + name]
    titles = ['Total ', ' Average ', 'Minimum ', 'Maximum ']
    results = [sums, avgs, mins, maxs]
    for sum, avg, min, max in zip(sums, avgs, mins, maxs):
        for record, title in zip([sum, avg, min, max], titles):
            if byMonth: name = record['date'][0:7]
            print(fmtLine(title + name, record))
        if byMonth: print('')
    print('')
    
def main():
    db = sqlite3.connect(DBname)
    db.row_factory = sqlite3.Row
    c = db.cursor()
    
    start, end, name = getTimeInterval.getPeriod('Prev7days')
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

    #db.set_trace_callback(print)

    printHeader() 
    for period in ['This Week',  'Last Week', 'This Month', 'Last Month']:
        makeSection(c, period)
        
    printHeader()
    makeSection(c, 'YearByMonth', byMonth = True)
    makeSection(c, 'LastYear')
    
    printHeader()
    first, last = getYears(c)
    for year in range(last.year, first.year - 1, -1):
        makeSection(c, 'Year', year = year)

    printHeader()
    makeSection(c, 'All')
    
if __name__ == '__main__':
  main()
