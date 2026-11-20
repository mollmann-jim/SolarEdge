#!/usr/bin/env python3
#import testdata
import datetime as dt
import sqlite3
import os
from pathlib import Path
import json
import pprint as pp

from solaredge import MonitoringClient, OAuth2, OAuthToken

home = os.getenv('HOME')

try:
    import solar_auth
    USERNAME = solar_auth.USERNAME
    SITEID   = solar_auth.SITEID
    ID       = solar_auth.CLIENT_ID
    SECRET   = solar_auth.CLIENT_SECRET
except:
    pass

DBname = home + '/tools/SolarEdge.2//SolarEdge.sql'
debug = False

def adapt_datetime(DT):
    return DT.isoformat(sep=' ')

def convert_datetime(val):
    return dt.datetime.fromisoformat(val).replace('T', ' ')

def getTimeRange(interval = 'QUARTER_HOUR'):
    now = dt.datetime.now(dt.timezone.utc)
    if interval == 'QUARTER_HOUR':
        yesterday = now - dt.timedelta(hours = 12)
    elif interval == 'HOUR':
        yesterday = now - dt.timedelta(hours = 24)
    else:
        yesterday = now.replace(hour = 0, minute = 0, second = 0, \
                                microsecond = 0) - dt.timedelta(days = 30)
    yesterday = now - dt.timedelta(hours = 1)

    #
    # initial data load
    #
    '''
    if False:
        yesterday = dt.datetime(2020, 4, 20)
        n = 6
        yesterday = yesterday + dt.timedelta(days= n * 7)
        now = yesterday + dt.timedelta(days= 7)
        print(yesterday, '->', now)
    '''
    #now = _fmt_date(now, '%Y-%m-%d %H:%M:%S')
    #yesterday = _fmt_date(yesterday, '%Y-%m-%d %H:%M:%S')
    return (yesterday, now)

def dumpData(data, kinds):
    for day in data:
        s = day
        for kind in kinds:
            s += '\t' + kind
            if data[day][kind] is None:
                s += '\tNone'
            else:
                s += f'\t{data[day][kind]:7.1f}'
        print(s)
    print('============================================')

class DB:
    def __init__(self, table):
        self.table = table
        self.columns = ['exportenergy',      'exportpower',
                        'importenergy',      'importpower',
                        'consumptionenergy', 'consumptionpower',
                        'productionenergy',  'productionpower']
        self.translate = {'exportenergy'     : 'exportEnergy',
                          'exportpower'      : 'exportPower',
                          'importenergy'     : 'importEnergy',
                          'importpower'      : 'importPower',
                          'consumptionenergy': 'consumptionEnergy',
                          'consumptionpower' : 'consumptionPower',
                          'productionenergy' : 'productionEnergy',
                          'productionpower'  : 'productionPower'}
        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' timestamp         DATETIME DEFAULT CURRENT_TIMESTAMP PRIMARY KEY,\n' +\
            ' exportenergy      REAL,\n' +\
            ' exportpower       REAL,\n' +\
            ' importenergy      REAL,\n' +\
            ' importpower       REAL,\n' +\
            ' consumptionenergy REAL,\n' +\
            ' consumptionpower  REAL,\n' +\
            ' productionenergy  REAL,\n' +\
            ' productionpower   REAL \n' +\
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
        select = 'SELECT exportenergy, exportpower, importenergy, '  +\
            'importpower, consumptionenergy, consumptionpower,'      +\
            'productionenergy, productionpower FROM '                +\
            self.table + ' WHERE timestamp = ?'
        self.c.execute(select, (date, ))
        record = self.c.fetchone()
        if record is None:
            return None
        Row = dict(record)
        row = {}
        for key in Row.keys():
            row[self.translate[key]] = Row[key]
        return row

    def updateData(self, date, row):
        insert = 'INSERT OR REPLACE INTO ' + self.table + ' (timestamp, '     +\
            'exportenergy, exportpower, importenergy, importpower, '          +\
            'consumptionenergy,  consumptionpower, productionenergy, '        +\
            'productionpower) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?);'
        print(date, row)
        values = [date, row['exportEnergy'], row['exportPower'],
                  row['importEnergy'],       row['importPower'], 
                  row['consumptionEnergy'],  row['consumptionPower'],
                  row['productionEnergy'],   row['productionPower']]
                  
        self.c.execute(insert, values)
        self.db.commit()

class PowerEnergy(DB):
    def __init__(self, table):
        self.table = table
        DB.__init__(self, self.table)

    def UpdateData(self):
        (begin, finish) = getTimeRange(interval = 'QUARTER_HOUR')
        with MonitoringClient(oauth=self.s.auth) as client:
            power = client.get_meters_telemetry(self.s.siteid, begin,
                                                finish, 'QUARTER_HOUR')
        pp.pprint(power)
        self.UpdateDB(power)

    def UpdateDB(self, power):
        meters = []
        kinds  = []
        data = {}
        times = set(())
        for meter in power['meters']:
            meters.append(meter)
            for kind in power['meters'][meter]:
                kinds.append(kind)
                times.add(len(power['meters'][meter][kind]['values']))
        print('meters:', meters)
        print('kinds: ', kinds)

        if len(set(times)) == 1:
            print(len(set(times)), 'timestamps counts', set(times), 'found as expected')
        else:
            print("Not all meters have the same number of timestamps:", set(times), self.table)

        for meter in power['meters']:
            for kind in power['meters'][meter]:
                for item in power['meters'][meter][kind]['values']:
                    if item['timestamp'] not in data:
                        data[item['timestamp']] = {}
                        for col in self.columns:
                            Col = self.translate[col]
                            data[item['timestamp']][Col] = None
                    data[item['timestamp']][kind] = item['value']
        print('data:')
        #pp.pprint(data)
        dumpData(data, kinds)
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


class PowerEnergyDetails(PowerEnergy):
    def __init__(self, s):
        self.s = s
        self.table = 'power_energy_details'
        PowerEnergy.__init__(self, self.table)

    def UpdateData(self):
        (begin, finish) = getTimeRange(interval = 'QUARTER_HOUR')
        #power = self.s.get_power_details(siteid, begin, finish)
        with MonitoringClient(oauth=self.s.auth) as client:
            power = client.get_meters_telemetry(self.s.siteid, begin,
                                                finish, 'QUARTER_HOUR'
                                                )
        #pp.pprint(power)
        #print(power)
        self.UpdateDB(power)

class PowerEnergyDay(PowerEnergy):
    def __init__(self, s):
        self.s = s
        self.table = 'power_energy_day'
        self.timeunit = 'DAY'
        PowerEnergy.__init__(self, self.table)

    def UpdateData(self):
        (begin, finish) = getTimeRange(interval = self.timeunit)
        #energy = self.s.get_energy_details(siteid, begin, finish, time_unit = self.timeunit)
        with MonitoringClient(oauth=self.s.auth) as client:
            energy = client.get_meters_telemetry(self.s.siteid, begin,
                                                 finish, self.timeunit,
                                                 '606324254')
        #print(energy)
        #pp.pprint(energy)
        self.UpdateDB(energy)
        
class Inverter():
    def __init__(self, s):
        self.s         = s
        self.table     = 'inverter_details'
        self.timeunit  = 'QUARTER_HOUR'
        self.columns   = ['power',           'energy',
                          'consumedacpower', 'consumedacenergy',
                          'voltage',         'current',
                          'frequency']
        self.translate = {'power'            : 'power',
                          'energy'           : 'energy',
                          'consumedacpower'  : 'consumedAcPower',
                          'consumedacenergy' : 'consumedAcEnergy',
                          'voltage'          : 'voltage',
                          'current'          : 'current',
                          'frequency'        : 'frequency'}
        self.DBInit(self.table)

    def DBInit(self, table):
        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' timestamp         DATETIME DEFAULT CURRENT_TIMESTAMP PRIMARY KEY,\n' +\
            ' power                  REAL,\n' +\
            ' energy                 REAL,\n' +\
            ' consumedacpower        REAL,\n' +\
            ' consumedacenergy       REAL,\n' +\
            ' voltage                REAL,\n' +\
            ' current                REAL,\n' +\
            ' frequency              REAL \n' +\
            ' );'
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
        select = 'SELECT power, energy, consumedacpower, consumedacenergy, ' +\
            'voltage, current, frequency FROM ' + self.table + ' '           +\
            'WHERE timestamp = ?;'
        self.c.execute(select, (date, ))
        record = self.c.fetchone()
        if record is None:
            return None
        Row = dict(record)
        row = {}
        for key in Row.keys():
            row[self.translate[key]] = Row[key]
        return row

    def updateData(self, date, row):
        insert = 'INSERT OR REPLACE INTO ' + self.table + ' (timestamp, '   +\
            'power, energy, consumedacpower, consumedacenergy, voltage, '   +\
            'current, frequency) VALUES( ?, ?, ?, ?, ?, ?, ?, ?);'
        print(date, row)
        values = [date, row['power'],     row['energy'],
                  row['consumedAcPower'], row['consumedAcEnergy'], 
                  row['voltage'],         row['current'],
                  row['frequency']]
        
    def UpdateData(self):
        (begin, finish) = getTimeRange(interval = self.timeunit)
        #energy = self.s.get_energy_details(siteid, begin, finish, time_unit = self.timeunit)
        with MonitoringClient(oauth=self.s.auth) as client:
            inverter = client.get_inverters_telemetry(self.s.siteid, begin,
                                                 finish, self.timeunit)
        #print(energy)
        #pp.pprint(energy)
        self.UpdateDB(inverter)
        
    def UpdateDB(self, power):
        inverters = []
        kinds     = []
        data      = {}
        times     = set(())
        for inverter in power['inverters']:
            inverters.append(inverter)
            for kind in power['inverters'][inverter]:
                kinds.append(kind)
                times.add(len(power['inverters'][inverter][kind]['values']))
        print('inverters:', inverters)
        print('kinds: ', kinds)

        if len(set(times)) == 1:
            print(len(set(times)), 'timestamps counts', set(times), 'found as expected')
        else:
            print("Not all inverters have the same number of timestamps:", set(times), self.table)

        for inverter in power['inverters']:
            for kind in power['inverters'][inverter]:
                for item in power['inverters'][inverter][kind]['values']:
                    if item['timestamp'] not in data:
                        data[item['timestamp']] = {}
                        for col in self.columns:
                            Col = self.translate[col]
                            data[item['timestamp']][Col] = None
                    data[item['timestamp']][kind] = item['value']
        print('data:')
        #pp.pprint(data)
        dumpData(data, kinds)
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

        
class Solaredge():
    def __init__(self, siteid, client_id, client_secret):
        self.siteid        = siteid
        self.client_id     = client_id
        self.client_secret = client_secret
        self.TOKEN_FILE    = Path("solaredge-token.json")
        self.auth          = OAuth2(
            OAuthToken.from_dict(json.loads(self.TOKEN_FILE.read_text())),
            client_id=self.client_id,
            client_secret=self.client_secret,
            on_token_update=self.save,
        )
        #print(self.siteid, self.client_id, self.client_secret, self.TOKEN_FILE)
        #print(self.auth)

    def save(self, token: OAuthToken) -> None:
        self.TOKEN_FILE.write_text(json.dumps(token.to_dict()))
        print('Solaredge:save: saved token', dt.datetime.now(dt.timezone.utc))
        
def main():
    s = Solaredge(SITEID, ID, SECRET)
    p = PowerEnergyDetails(s)
    d = PowerEnergyDay(s)
    i = Inverter(s)

    #### p.UpdateData()
    #### d.UpdateData()
    i.UpdateData()

    '''
    s = Solaredge(TOKEN)
    p = PowerDetails(s)
    eDay = EnergyDay(s)
    eDet = EnergyDetails(s)
    eDay.UpdateData()
    eDet.UpdateData()
    p.UpdateData()
    '''




if __name__ == '__main__':
    main()
