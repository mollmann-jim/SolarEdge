#!/usr/bin/env python3
from se_api import Solaredge, _fmt_date
#import testdata
import datetime as dt
import sqlite3
import os

home = os.getenv('HOME')

try:
    import solar_auth
    USERNAME = solar_auth.USERNAME
    PASSWORD = solar_auth.PASSWORD
    siteid   = solar_auth.SITEID
    TOKEN    = solar_auth.TOKEN
except:
    pass

DBname = home + '/tools/SolarEdge//SolarEdge.sql'
debug = False

def adapt_datetime(dt):
    return dt.isoformat(sep=' ')

def convert_datetime(val):
    return dt.datetime.fromisoformat(val).replace('T', ' ')

def getTimeRange():
    now = dt.datetime.now()
    yesterday = now.replace(hour =0, minute =0, second =0, microsecond = 0) - \
        dt.timedelta(days=7)
    #
    # initial data load
    #
    if False:
        yesterday = dt.datetime(2020, 4, 20)
        n = 6
        yesterday = yesterday + dt.timedelta(days= n * 7)
        now = yesterday + dt.timedelta(days= 7)
        print(yesterday, '->', now)
    now = _fmt_date(now, '%Y-%m-%d %H:%M:%S')
    yesterday = _fmt_date(yesterday, '%Y-%m-%d %H:%M:%S')

    
    return (yesterday, now)

def dumpData(data, meters):
    for day in data:
        s = day
        row = []
        row.append(day)
        for meter in meters:
            if data[day][meter] is None: s += '\tNone'
            else: s += '\t{:7.1f}'.format(data[day][meter])
            row.append(data[day][meter])
        print(s)
    print('============================================')

class DB:
    def __init__(self):
        #self.table = table
        self.columns = ['consumption', 'production', 'purchased', 'feedin', \
                        'selfconsumption']
        self.translate = {'consumption' : 'Consumption', 'production' : 'Production' , \
                          'purchased' : 'Purchased', 'feedin' : 'FeedIn', \
                          'selfconsumption' : 'SelfConsumption'}
        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' timestamp       DATETIME DEFAULT CURRENT_TIMESTAMP PRIMARY KEY,\n' +\
            ' consumption     REAL,\n' +\
            ' production      REAL,\n' +\
            ' purchased       REAL,\n' +\
            ' feedin          REAL,\n' +\
            ' selfconsumption REAL \n' +\
            ' );'
        sqlite3.register_adapter(dt.datetime, adapt_datetime)
        sqlite3.register_converter("DATETIME", convert_datetime)
        self.db = sqlite3.connect(DBname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = sqlite3.Row
        self.c = self.db.cursor()
        if debug:
            #drop = 'DROP TABLE IF EXISTS ' + self.table
            #self.c.execute(drop)
            pass
        self.c.execute(create)
        self.db.commit()
        
    def getOldData(self, date):
        select = 'SELECT consumption, production, purchased, feedin, selfconsumption FROM ' +\
            self.table + ' WHERE timestamp = ?'
        self.c.execute(select, (date, ))
        record = self.c.fetchone()
        if record is None: return None
        Row = dict(record)
        row = {}
        for key in Row.keys():
            row[self.translate[key]] = Row[key]
        return row

    def updateData(self, date, row):
        insert = 'INSERT OR REPLACE INTO ' + self.table + ' (timestamp, consumption, ' +\
            'production, purchased, feedin, selfconsumption) ' +\
            'VALUES( ?, ?, ?, ?, ?, ?);'
        values = [date, row['Consumption'], row['Production'], row['Purchased'], row['FeedIn'], \
                  row['SelfConsumption']]
        self.c.execute(insert, values)
        self.db.commit()
                
class PowerDetails(DB):
    def __init__(self, s):
        self.s = s
        self.table = 'power_details'
        DB.__init__(self)
        

    def UpdateData(self):
        (begin, finish) = getTimeRange()
        power = self.s.get_power_details(siteid, begin, finish)
        #power = testdata.power_details
        #print(power)
        self.UpdateDB(power)
       
    def UpdateDB(self, power):
        meters = []
        data = {}
        times = set(())
        for row in power['powerDetails']['meters']:
            meter = row['type']
            meters.append(meter)
            times.add(len(row['values']))
            
        if len(set(times)) == 1:
            print(len(set(times)), 'timestamps counts', set(times), 'found as expected')
        else:
            print("Not all meters have the same number of timestamps:", set(times), self.table)

        for row in power['powerDetails']['meters']:
            meter = row['type']
            for item in row['values']:
                if item['date'] not in data:
                    data[item['date']] = {}
                    for m in meters:
                        data[item['date']][m] = None
                if 'value' in item:
                    data[item['date']][meter] = item['value']
        #dumpData(data, meters)
        for day in data:
            oldInfo = self.getOldData(day)
            #oldInfo may be 'None'
            # do we want to not use the latest data???
            if oldInfo:
                for k in data[day].keys():
                    if data[day][k] != oldInfo.get(k, None):
                        print('Differ:', self.table, day, k, \
                              oldInfo.get(k, None), ' -> ', data[day][k], data[day])
            self.updateData(day, data[day])
        
class Energy(DB):
    def __init__(self):
        DB.__init__(self)

    def UpdateDB(self, energy):
        meters = []
        data = {}
        times = set(())
        for row in energy['energyDetails']['meters']:
            meter = row['type']
            meters.append(meter)
            times.add(len(row['values']))
            
        if len(set(times)) == 1:
            print(len(set(times)), 'timestamps counts', set(times), 'found as expected')
        else:
            print("Not all meters have the same number of timestamps:", set(times))

        for row in energy['energyDetails']['meters']:
            meter = row['type']
            for item in row['values']:
                if item['date'] not in data:
                    data[item['date']] = {}
                    for m in meters:
                        data[item['date']][m] = None
                if 'value' in item:
                    data[item['date']][meter] = item['value']
        #dumpData(data, meters)
        for day in data:
            oldInfo = self.getOldData(day)
            #oldInfo may be 'None'
            # do we want to not use the latest data???
            if oldInfo:
                for k in data[day].keys():
                    if data[day][k] != oldInfo.get(k, None):
                        print('Differ:', self.table, day, k, \
                              oldInfo.get(k, None), ' -> ', data[day][k], data[day])

            self.updateData(day, data[day])
                           
        
class EnergyDay(Energy):
    def __init__(self, s):
        self.s = s
        self.table = 'energy_day'
        self.timeunit = 'DAY'
        Energy.__init__(self)
        
    def UpdateData(self):
        (begin, finish) = getTimeRange()
        energy = self.s.get_energy_details(siteid, begin, finish, time_unit = self.timeunit)
        #energy = testdata.energy_details_day
        #print(energy)
        self.UpdateDB(energy)
        
                  
class EnergyDetails(Energy):
    def __init__(self, s):
        self.s = s
        self.table = 'energy_details'
        self.timeunit = 'QUARTER_OF_AN_HOUR'
        Energy.__init__(self)
        
    def UpdateData(self):
        (begin, finish) = getTimeRange()
        energy = self.s.get_energy_details(siteid, begin, finish, time_unit = self.timeunit)
        #energy = testdata.energy_details
        self.UpdateDB(energy)
        
def main():
    s = Solaredge(TOKEN)
    p = PowerDetails(s)
    eDay = EnergyDay(s)
    eDet = EnergyDetails(s)
    eDay.UpdateData()
    eDet.UpdateData()
    p.UpdateData()

    

    
if __name__ == '__main__':
  main()
