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

def getTimeRange():
    now = dt.datetime.now(dt.timezone.utc)
    yesterday = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days=7)
    yesterday = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0) - \
        dt.timedelta(days=1)
    yesterday = now.replace(hour = 17, minute = 0, second = 0, microsecond = 0)

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

def dumpData(data, meters):
    for day in data:
        s = day
        row = []
        row.append(day)
        for meter in meters:
            if data[day][meter] is None:
                s += '\tNone'
            else:
                s += f'\t{data[day][meter]:7.1f}'
            row.append(data[day][meter])
        print(s)
    print('============================================')


class DB:
    def __init__(self, table):
        self.table = table
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
        if record is None:
            return None
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
        DB.__init__(self, self.table)


    def UpdateData(self):
        (begin, finish) = getTimeRange()
        #power = self.s.get_power_details(siteid, begin, finish)
        with MonitoringClient(oauth=self.s.auth) as client:
            '''
            power = client.get_meters_telemetry(self.s.siteid, begin,
                                                finish, 'QUARTER_HOUR')
            '''
            power = client.get_meters_telemetry(self.s.siteid, begin,
                                                finish, 'QUARTER_HOUR')
        pp.pprint(power)
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
        dumpData(data, meters)
        z = data / 0
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

'''
class Energy(DB):
    def __init__(self, table):
        self.table = table
        DB.__init__(self, self.table)

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

'''
'''
class EnergyDay(Energy):
    def __init__(self, s):
        self.s = s
        self.table = 'energy_day'
        self.timeunit = 'DAY'
        Energy.__init__(self, self.table)

    def UpdateData(self):
        (begin, finish) = getTimeRange()
        energy = self.s.get_energy_details(siteid, begin, finish, time_unit = self.timeunit)
        #energy = testdata.energy_details_day
        #print(energy)
        self.UpdateDB(energy)
'''
'''
class EnergyDetails(Energy):
    def __init__(self, s):
        self.s = s
        self.table = 'energy_details'
        self.timeunit = 'QUARTER_OF_AN_HOUR'
        Energy.__init__(self, self.table)

    def UpdateData(self):
        (begin, finish) = getTimeRange()
        energy = self.s.get_energy_details(siteid, begin, finish, time_unit = self.timeunit)
        #energy = testdata.energy_details
        self.UpdateDB(energy)
'''
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
    p = PowerDetails(s)

    p.UpdateData()

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
