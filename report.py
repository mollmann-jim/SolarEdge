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
        Prod    = row['Production'] if row['Production'] is not None else 0.0
        prod    = ' {:>10.2f}'.format(Prod / 1000)
        used    = ' {:>11.2f}'.format(row['Consumption'] / 1000)
        percent = ' {:>8.1f}'.format(Prod / row['Consumption'] * 100)
        sold    = ' {:>8.2f}'.format(row['FeedIn'] / 1000)
        bought  = ' {:>8.2f}'.format(row['Purchased'] / 1000)
        selfcon = ' {:>9.2f}'.format(row['SelfConsumption'] / 1000)
        line = header + prod + used + percent + sold + bought + selfcon
    return line

def printHeader():
    print('')
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
        select_end = groupMonth
    else:
        select_end = ' ;'
    results = []
    for select in [select_sum, select_avg, select_min, select_max]:
        select += select_end
        c.execute(select, (start, end))
        result = c.fetchall()
        results.append(result)
                               
    titles = ['Total ', ' Average ', 'Minimum ', 'Maximum ']
    for sum, avg, min, max in zip(*results):
        for record, title in zip([sum, avg, min, max], titles):
            if byMonth: name = record['date'][0:7]
            print(fmtLine(title + name, record))
        if byMonth: print('')
    print('')

def reportByHour(c):
    periods = ['Yesterday', 'Prev7days', 'Last30Days', 'All']
    prodFields = ['AvgProd', 'MaxProd']
    usedFields = ['AvgUsed', 'MaxUsed']
    dataFields = prodFields + usedFields
    starts = {}
    ends   = {}
    for period in periods:
        starts[period], ends[period], name = getTimeInterval.getPeriod(period)
    selectHour = 'SELECT count(*) AS count, hours.date as date, hours.hour as hour, ' \
        ' AVG(hours.production)  AS AvgProd,  MAX(hours.production)  AS MaxProd, ' \
        ' AVG(hours.consumption) AS AvgUsed,  MAX(hours.consumption) AS MaxUsed  ' \
        ' FROM ( ' \
        '     SELECT TOTAL(production) AS production, TOTAL(consumption) AS consumption, ' \
        '     date(timestamp) as date, STRFTIME("%H", timestamp) as hour ' \
        '     FROM energy_details ' \
        '     WHERE timestamp >= ? AND timestamp <= ? ' \
 	'     GROUP BY date, STRFTIME("%H", timestamp) ' \
	' )		 AS hours '\
	' GROUP BY hour ORDER BY hour;'
    selectCumm = 'SELECT count(*) AS count, hours.date as date, hours.hour as hour, ' \
        ' AVG(hours.production)  AS AvgProd,  MAX(hours.production)  AS MaxProd, ' \
        ' AVG(hours.consumption) AS AvgUsed,  MAX(hours.consumption) AS MaxUsed  ' \
        ' FROM ( ' \
        '     SELECT TOTAL(production) AS production, TOTAL(consumption) AS consumption, ' \
        '     date(timestamp) as date, STRFTIME("%H", timestamp) as hour ' \
        '     FROM energy_details ' \
        '     WHERE timestamp >= ? AND timestamp <= ? ' \
        '       AND STRFTIME("%H", timestamp) <= ? ' \
 	'     GROUP BY date ' \
	' )		 AS hours '\
	' GROUP BY hour ORDER BY hour;'

    hourData = {}
    for period in periods:
        hourData[period] = {}
        c.execute(selectHour, (starts[period], ends[period]))
        results = c.fetchall()
        for rec in results:
            hour = rec['hour']
            hourData[period][hour] = {}
            for field in dataFields:
                hourData[period][hour][field] = rec[field]
    
    cummData = {}
    for period in periods:
        cummData[period] = {}
        for hour in range(0,24):
             hr = '{:02d}'.format(hour)
             cummData[period][hr] = {}
             c.execute(selectCumm, (starts[period], ends[period], hr))
             results = c.fetchall()
             for rec in results:
                 for field in dataFields:
                     cummData[period][hr][field] = rec[field] / 1000.0
    #print(cummData)

    Hdr = [None] * 4
    fmt1 = '{:^' + str(12 * 2 * len(periods)) + '}'
    fmt2 = '{:^' + str( 6 * 2 * len(periods)) + '}'
    fmt2 += fmt2
    fmt3 = ''.join(['{:>11s} '.format(p) for p in periods])
    fmt3 += fmt3
    Hdr = [None] * 4
    Hdr[0] = '' 
    Hdr[2] = '    ' + fmt2.format('Average', 'Maximum')
    Hdr[3] = 'Time' + fmt3
    for type, unit, numfmt, data in zip(['Hourly ', 'Cummulative '], \
                                        [' (Wh)', ' (KWh)'], \
                                        ['{:>11.0f} ', '{:>11.3f} '], \
                                        [hourData, cummData]):
        for pu, fld in zip(['Production', 'Consumption'], \
                           [prodFields, usedFields]):
            Hdr[1] = '    ' + fmt1.format(type + pu + unit)
            for line in Hdr:
                print(line)
            for hour in range(0, 24):
                hr = '{:02d}'.format(hour)
                l = '{:02d}00'.format(hour)
                for field in fld:
                    for period in periods:
                        try:
                            val = data[period][hr][field]
                        except KeyError:
                            val = 0
                        #l += numfmt.format(data[period][hr][field])
                        l += numfmt.format(val)
                print(l)
         
def main():
    db = sqlite3.connect(DBname)
    db.row_factory = sqlite3.Row
    c = db.cursor()
    #db.set_trace_callback(print)
    
    start, end, name = getTimeInterval.getPeriod('Prev7days')
    printHeader()
    
    select = 'SELECT * FROM energy_day WHERE timestamp >= ? AND timestamp <= ? ' +\
        'ORDER BY timestamp DESC;'
    c.execute(select, (start, end))
    result = c.fetchall()
    for record in result:
        date = record['timestamp'].split(' ')[0]
        print(fmtLine(date, record))
    
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

    reportByHour(c)

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
    
if __name__ == '__main__':
  main()
