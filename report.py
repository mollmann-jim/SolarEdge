#!/usr/bin/env python3
#from se_api import Solaredge, _fmt_date
#import testdata
import datetime as dt
import sqlite3
import os
import zoneinfo

home = os.getenv('HOME')

from sys import path
path.append(home + '/tools/')
from shared import getTimeInterval

DBname = home + '/tools/SolarEdge.2//SolarEdge.sql'
debug = False

translate = {'consumption' : 'Consumption', 'production' : 'Production' , \
             'purchased' : 'Purchased', 'feedin' : 'FeedIn', \
             'selfconsumption' : 'SelfConsumption', 'timestamp' : 'Date'}
columns = ['consumption', 'production', 'purchased', 'feedin', \
                        'selfconsumption', 'timestamp']
Columns = ['Consumption', 'Production', 'Purchased', 'FeedIn', \
                        'SelfConsumption', 'Timestamp']

def adapt_datetime(DT):
    return DT.isoformat()

def convert_datetime(val):
    return dt.datetime.fromisoformat(val.decode('utf-8'))

def getYears(c):
    select_min_yr = 'SELECT min(timestamp) AS min FROM power_energy_day;'
    c.execute(select_min_yr)
    minYear = c.fetchone()
    #first = dt.datetime.strptime(minYear['min'], '%Y-%m-%d %H:%M:%S')
    first = dt.datetime.fromisoformat(minYear['min'])
    select_max_yr = 'SELECT max(timestamp) AS max FROM power_energy_day;'
    c.execute(select_max_yr)
    maxYear = c.fetchone()
    #last = dt.datetime.strptime(maxYear['max'], '%Y-%m-%d %H:%M:%S')
    last = dt.datetime.fromisoformat(maxYear['max'])
    return first, last

def getYesterday():
    now   = dt.datetime.now()
    day   = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days = 1)
    yesterday = dt.datetime.date(day)
    return str(yesterday)

def fmtLine(tag, row):
    line = tag + ': (none)'
    if row['Consumption'] is not None:
        #print(dict(row))
        header  = f'{tag:>19s}'
        Prod    = row['Production'] if row['Production'] is not None else 0.0
        prod    = f' {Prod / 1000:>10.2f}'
        used    = f' {(row['Consumption'] / 1000):>11.2f}'
        Percent = Prod / row['Consumption'] * 100 if row['Consumption'] else 0.0
        percent = f' {Percent:>8.1f}'
        sold    = f' {row['FeedIn'] / 1000:>8.2f}'
        bought  = f' {row['Purchased'] / 1000:>8.2f}'
        Selfcon = row['SelfConsumption'] if row['SelfConsumption'] is not None else 0.0
        selfcon = f' {(Selfcon / 1000):>9.2f}'
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
def printHeaderTZ():
    print('')
    print('                          Production Consumption              Sold   Bought Prod-Used')
    print('                   Period        KWh         KWh   % Prod      KWh      KWh       KWh')
    print('------------------------- ----------  ---------- -------- -------- -------- ---------')
    
def makeSection(c, title, byDay = False, byMonth = False, year = None):
    start, end, name = getTimeInterval.getPeriod(title, year = year)
    #print(start, end, name)
    
    select_sum = 'SELECT ' \
        'TOTAL(inverter_day.energy)                AS production, '        \
        'TOTAL(power_energy_day.consumptionenergy) AS consumption, '       \
        'TOTAL(power_energy_day.exportenergy)      AS feedin, '            \
        'TOTAL(power_energy_day.importenergy)      AS purchased, '         \
        'TOTAL(power_energy_day.consumptionenergy - '                      \
        '      power_energy_day.importenergy) '                            \
        '                                          AS selfconsumption,   ' \
        'inverter_day.timestamp                    AS date '               \
        'FROM power_energy_day JOIN inverter_day '                         \
        '  ON inverter_day.timestamp = power_energy_day.timestamp '        \
        'WHERE inverter_day.timestamp >= ? AND inverter_day.timestamp <= ? '
    select_avg = 'SELECT ' \
        'AVG(inverter_day.energy)                AS production, '          \
        'AVG(power_energy_day.consumptionenergy) AS consumption, '         \
        'AVG(power_energy_day.exportenergy)      AS feedin, '              \
        'AVG(power_energy_day.importenergy)      AS purchased, '           \
        'AVG(power_energy_day.consumptionenergy - '                        \
        '      power_energy_day.importenergy) '                            \
        '                                          AS selfconsumption,   ' \
        'inverter_day.timestamp                    AS date '               \
        'FROM power_energy_day JOIN inverter_day '                         \
        '  ON inverter_day.timestamp = power_energy_day.timestamp '        \
        'WHERE inverter_day.timestamp >= ? AND inverter_day.timestamp <= ? '
    select_min = 'SELECT ' \
        'MIN(inverter_day.energy)                AS production, '          \
        'MIN(power_energy_day.consumptionenergy) AS consumption, '         \
        'MIN(power_energy_day.exportenergy)      AS feedin, '              \
        'MIN(power_energy_day.importenergy)      AS purchased, '           \
        'MIN(power_energy_day.consumptionenergy - '                        \
        '      power_energy_day.importenergy) '                            \
        '                                          AS selfconsumption,   ' \
        'inverter_day.timestamp                    AS date '               \
        'FROM power_energy_day JOIN inverter_day '                         \
        '  ON inverter_day.timestamp = power_energy_day.timestamp '        \
        'WHERE inverter_day.timestamp >= ? AND inverter_day.timestamp <= ? '
    select_max = 'SELECT ' \
        'MAX(inverter_day.energy)                AS production, '          \
        'MAX(power_energy_day.consumptionenergy) AS consumption, '         \
        'MAX(power_energy_day.exportenergy)      AS feedin, '              \
        'MAX(power_energy_day.importenergy)      AS purchased, '           \
        'MAX(power_energy_day.consumptionenergy - '                        \
        '      power_energy_day.importenergy) '                            \
        '                                          AS selfconsumption,   ' \
        'inverter_day.timestamp                    AS date '               \
        'FROM power_energy_day JOIN inverter_day '                         \
        '  ON inverter_day.timestamp = power_energy_day.timestamp '        \
        'WHERE inverter_day.timestamp >= ? AND inverter_day.timestamp <= ? '

    groupMonth = ' GROUP BY substr(inverter_day.timestamp,1, 7) '          \
        'ORDER BY inverter_day.timestamp DESC;'
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
    for Sum, Avg, Min, Max in zip(*results):
        for Record, Title in zip([Sum, Avg, Min, Max], titles):
            if byMonth:
                name = str(Record['date'])[0:7]
            print(fmtLine(Title + name, Record))
        if byMonth:
            print('')
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
        ' AVG(hours.production)  AS AvgProd,  MAX(hours.production)  AS MaxProd, '    \
        ' AVG(hours.consumption) AS AvgUsed,  MAX(hours.consumption) AS MaxUsed  '    \
        ' FROM ( '                                                                    \
        ' SELECT '                                                                    \
        'TOTAL(inverter_details.energy)                AS production, '               \
        'TOTAL(power_energy_details.consumptionenergy) AS consumption, '              \
        '     date(inverter_details.timestamp) as date, '                             \
        ' STRFTIME("%H",inverter_details. timestamp) as hour '                        \
        'FROM power_energy_details JOIN inverter_details '                            \
        '  ON inverter_details.timestamp = power_energy_details.timestamp '           \
        'WHERE inverter_details.timestamp >= ? AND inverter_details.timestamp <= ? '  \
        'GROUP BY date, STRFTIME("%H", inverter_details.timestamp) '                  \
        ' )		 AS hours '                                                   \
	' GROUP BY hour ORDER BY hour;'
    selectCumm = 'SELECT count(*) AS count, hours.date as date, hours.hour as hour, ' \
        ' AVG(hours.production)  AS AvgProd,  MAX(hours.production)  AS MaxProd, '    \
        ' AVG(hours.consumption) AS AvgUsed,  MAX(hours.consumption) AS MaxUsed  '    \
        ' FROM ( '                                                                    \
        ' SELECT '                                                                    \
        'TOTAL(inverter_details.energy)                AS production, '               \
        'TOTAL(power_energy_details.consumptionenergy) AS consumption, '              \
        '     date(inverter_details.timestamp) as date, '                             \
        ' STRFTIME("%H",inverter_details. timestamp) as hour '                        \
        'FROM power_energy_details JOIN inverter_details '                            \
        '  ON inverter_details.timestamp = power_energy_details.timestamp '           \
        'WHERE inverter_details.timestamp >= ? AND inverter_details.timestamp <= ? '  \
        '  AND STRFTIME("%H", inverter_details.timestamp) <= ? ' \
        'GROUP BY date '                                                              \
        ' )		 AS hours '                                                   \
	' GROUP BY hour ORDER BY hour;'
    hourData = {}
    for period in periods:
        hourData[period] = {}
        print
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
            hr = f'{hour:02d}'
            cummData[period][hr] = {}
            c.execute(selectCumm, (starts[period], ends[period], hr))
            results = c.fetchall()
            for rec in results:
                for field in dataFields:
                    cummData[period][hr][field] = rec[field] / 1000.0
    #print(cummData)

    Hdr = [None] * 4
    fmt1 = '{:^' + str(10 * 2 * len(periods)) + '}'
    fmt2 = '{:^' + str( 5 * 2 * len(periods)) + '}'
    fmt2 += fmt2
    fmt3 = ''.join([f'{p:>10s} ' for p in periods])
    fmt3 += fmt3

    Hdr = [None] * 4
    Hdr[0] = ''
    Hdr[2] = '    ' + fmt2.format('Average', 'Maximum')
    Hdr[3] = 'Time' + fmt3
    for hourCum, unit, numfmt, data in zip(['Hourly ', 'Cummulative '], \
                                        [' (Wh)', ' (KWh)'], \
                                        ['{:>10.0f} ', '{:>10.3f} '], \
                                        [hourData, cummData]):
        for pu, fld in zip(['Production', 'Consumption'], \
                           [prodFields, usedFields]):
            Hdr[1] = '    ' + fmt1.format(hourCum + pu + unit)
            for line in Hdr:
                print(line)
            for hour in range(0, 24):
                hr = f'{hour:02d}'
                l  = f'{hour:02d}00'
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
    sqlite3.register_adapter(dt.datetime, adapt_datetime)
    sqlite3.register_converter("DATETIME", convert_datetime)
    db = sqlite3.connect(DBname, detect_types=sqlite3.PARSE_DECLTYPES)
    #db.set_trace_callback(print)
    db.row_factory = sqlite3.Row
    c = db.cursor()
    #db.set_trace_callback(print)

    for period in ['Prev7days', 'Prev7daysLastYear']:
        start, end, name = getTimeInterval.getPeriod(period)
        printHeader()

        select = 'SELECT inverter_day.timestamp, '                     \
            'inverter_day.energy    AS Production, '                   \
            'power_energy_day.consumptionenergy AS Consumption, '      \
            'power_energy_day.exportenergy      AS FeedIn, '           \
            'power_energy_day.importenergy      AS Purchased, '        \
            'power_energy_day.consumptionenergy - '                    \
            'power_energy_day.importenergy '                           \
            '                                   AS SelfConsumption '   \
            'FROM power_energy_day JOIN inverter_day '                 \
            ' ON inverter_day.timestamp = power_energy_day.timestamp ' \
            'WHERE inverter_day.timestamp >= ? AND inverter_day.timestamp <= ? ' \
            'ORDER BY inverter_day.timestamp DESC;'

        c.execute(select, (start, end))
        result = c.fetchall()
        for record in result:
            date = str(record['timestamp']).split(' ', maxsplit = 1)[0]
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

    printHeaderTZ()
    select_hr = 'SELECT ' \
        'TOTAL(inverter_details.energy)                AS production, '        \
        'TOTAL(power_energy_details.consumptionenergy) AS consumption, '       \
        'TOTAL(power_energy_details.exportenergy)      AS feedin, '            \
        'TOTAL(power_energy_details.importenergy)      AS purchased, '         \
        'TOTAL(power_energy_details.consumptionenergy - '                      \
        '      power_energy_details.importenergy) '                            \
        '                                          AS selfconsumption,   '     \
        'inverter_details.timestamp  '                                         \
        'FROM power_energy_details JOIN inverter_details '                     \
        '  ON inverter_details.timestamp = power_energy_details.timestamp '    \
        'WHERE DATE(inverter_details.timestamp) == ? ' \
        'GROUP BY STRFTIME("%H", inverter_details.timestamp) ' \
        'ORDER BY inverter_details.timestamp;'

    yesterday = getYesterday()
    c.execute(select_hr, (yesterday,))
    result = c.fetchall()
    for record in result:
        print(fmtLine(record['timestamp'].isoformat(sep = ' '), record))

if __name__ == '__main__':
    main()
