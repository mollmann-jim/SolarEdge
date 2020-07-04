#!/usr/bin/python
from se_api import Solaredge, _fmt_date
#import testdata
import datetime as dt
import sqlite3
import requests
from requests.auth import HTTPBasicAuth
from dateutil.tz import tz

try:
    import solar_auth
    USERNAME = solar_auth.USERNAME
    PASSWORD = solar_auth.PASSWORD
    siteid   = solar_auth.SITEID
except:
    pass

URL = 'https://monitoring.solaredge.com/solaredge-apigw/api/sites/' + siteid + '/layout/logical'
DBname = '/home/jim/tools/SolarEdge//SolarEdge.sql'
debug = False


class DB:
    def __init__(self):
        self.db = sqlite3.connect(DBname)
        self.db.row_factory = sqlite3.Row
        self.c = self.db.cursor()
        
class PanelData(DB):
    def __init__(self):
        self.table = 'paneldata'
        DB.__init__(self)
        self.notPanels = ['114844043', '114496154', '1234567']
        if debug:
            #self.c.execute('DROP TABLE IF EXISTS ' + self.table)
            pass
        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' id              INTEGER PRIMARY KEY,\n' +\
            ' timestamp       DATETIME DEFAULT CURRENT_TIMESTAMP,\n' +\
            ' module          INTEGER,\n' +\
            ' energy          REAL,\n' +\
            ' energyw         REAL \n' +\
            ' )'
        self.c.execute(create)
        self.db.commit()

    def Insert(self, layout):
        insert = 'INSERT INTO ' + self.table + ' (timestamp, module, energy, energyw)' +\
            ' VALUES(?, ?, ?, ?)'
        now = dt.datetime.now()
        timestamp = now
        for module in layout['reportersData']:
            if module in self.notPanels:
                #print(module, 'not a panel', layout['reportersData'][module]['moduleEnergy'])
                pass
            else:
                #print(module, ' is a panel', layout['reportersData'][module]['moduleEnergy'])
                self.c.execute(insert, (timestamp, \
                                        module, \
                                        layout['reportersData'][module]['energy'], \
                                        layout['reportersData'][module]['unscaledEnergy']))
        self.db.commit()
        
class PanelInfo(DB):
    def __init__(self):
        self.table = 'panelinfo'
        DB.__init__(self)
        self.notPanels = ['114844043', '114496154']
        if debug:
            #self.c.execute('DROP TABLE IF EXISTS ' + self.table)
            pass
        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' module          INTEGER PRIMARY KEY,\n' +\
            ' timestamp       DATETIME DEFAULT CURRENT_TIMESTAMP,\n' +\
            ' timestampfirst  DATETIME DEFAULT NULL,\n' +\
            ' serialnumber    TEXT,\n' +\
            ' name            TEXT,\n' +\
            ' manufacturer    TEXT,\n' +\
            ' model           TEXT \n' +\
            ' )'
        self.c.execute(create)
        self.db.commit()
        
    def Insert(self, layout):
        insert = 'INSERT OR REPLACE INTO ' + self.table +\
            ' (module, timestamp, serialnumber, name, manufacturer, model)' +\
            ' VALUES(?, ?, ?, ?, ?, ?)'
        update = 'UPDATE ' + self.table + ' SET timestampfirst = COALESCE(timestampfirst, ?) ' +\
            ' WHERE module IS ?;'
        update = 'UPDATE ' + self.table + ' SET timestampfirst = ? WHERE module = ? ' +\
            ' AND timestampfirst is NULL;'
        for module in layout['reportersInfo']:
            if module in self.notPanels:
                #print(module, 'not a panel', layout['reportersInfo'][module])
                pass
            else:
                timestamp = dt.datetime.fromtimestamp(layout['reportersInfo'][module]['lastMeasurement'] / 1000 \
                                                      , tz=tz.gettz('UTC'))
                values = (module, \
                          timestamp, \
                          layout['reportersInfo'][module]['serialNumber'], \
                          layout['reportersInfo'][module]['name'], \
                          layout['reportersInfo'][module]['manufacturer'], \
                          layout['reportersInfo'][module]['model'] )
                self.c.execute(insert, values)
                self.c.execute(update, (timestamp, module))
        self.db.commit()

class PanelMeasurement(DB):
    def __init__(self):
        self.table = 'panelmeasurement'
        DB.__init__(self)
        self.notPanels = ['114844043', '114496154']
        if debug:
            #self.c.execute('DROP TABLE IF EXISTS ' + self.table)
            pass
        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' id              INTEGER PRIMARY KEY,\n' +\
            ' timestamp       DATETIME DEFAULT CURRENT_TIMESTAMP,\n' +\
            ' module          INTEGER,\n' +\
            ' current         REAL,\n' +\
            ' optvoltage      REAL,\n' +\
            ' power           REAL,\n' +\
            ' voltage         REAL \n' +\
            ' )'
        self.c.execute(create)
        self.db.commit()
         
    def Insert(self, layout):
        insert = 'INSERT INTO ' + self.table +\
            ' (timestamp, module, current, optvoltage, power, voltage)' +\
            ' VALUES(?, ?, ?, ?, ?, ?)'
        for module in layout['reportersInfo']:
            if module in self.notPanels:
                #print(module, 'not a panel', layout['reportersInfo'][module])
                pass
            else:
                timestamp = dt.datetime.fromtimestamp(layout['reportersInfo'][module]['lastMeasurement'] / 1000)
                values = (timestamp, \
                          module, \
                          layout['reportersInfo'][module]['localizedMeasurements']['Current [A]'], \
                          layout['reportersInfo'][module]['localizedMeasurements']['Optimizer Voltage [V]'], \
                          layout['reportersInfo'][module]['localizedMeasurements']['Power [W]'], \
                          layout['reportersInfo'][module]['localizedMeasurements']['Voltage [V]'] )
                self.c.execute(insert, values)
        self.db.commit()
        
def main():
    
    if debug:
        layout = testdata.layout
    else:
        r = requests.get(URL, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        r.raise_for_status()
        layout = r.json()
        #print(layout)

    pd = PanelData()
    pd.Insert(layout)
    pi = PanelInfo()
    pi.Insert(layout)
    pm = PanelMeasurement()
    pm.Insert(layout)

if __name__ == '__main__':
  main()
