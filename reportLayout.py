#!/usr/bin/python

import datetime as dt
import sqlite3
from sys import path
path.append('/home/jim/tools/')
from shared import getTimeInterval
from dateutil.tz import tz

DBname = '/home/jim/tools/SolarEdge//SolarEdge.sql'

debug = False

class DB:
    def __init__(self):
        sqlite3.register_adapter(dt.datetime, adapt_datetime)
        sqlite3.register_converter("DATETIME", convert_datetime)
        self.db = sqlite3.connect(DBname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = sqlite3.Row
        self.c = self.db.cursor()
        #self.db.set_trace_callback(print)
        
    def getYears(self):
        select_min_yr = 'SELECT min(timestamp) AS min FROM ' + self.table + ' ;'
        self.c.execute(select_min_yr)
        min = self.c.fetchone()
        first = dt.datetime.fromisoformat(min['min'])
        select_max_yr = 'SELECT max(timestamp) AS max FROM ' + self.table + ' ;'
        self.c.execute(select_max_yr)
        max = self.c.fetchone()
        last = dt.datetime.fromisoformat(max['max'])                          
        return first, last
    
class PanelInfo(DB):
    def __init__(self):
        self.table = 'panelinfo'
        DB.__init__(self)

    def getModule2Name(self):
        select = 'SELECT module, name FROM ' + self.table + ' ;'
        self.c.execute(select)
        result = self.c.fetchall()
        table = {}
        for rec in result:
            table[rec['module']] = rec['name']
        return table
    
class PanelData(DB):
    def __init__(self):
        self.table = 'paneldata'
        DB.__init__(self)
        self.minPanelCount = 0  # ignore records from testing
        self.minCount      = 0  # ignore records from testing

    def getProduction(self, start, end, minPanelCount = None, minCount = None):
        if not minPanelCount: minPanelCount = self.minPanelCount
        if not minCount: minCount = self.minCount
        minPanelCount= str(minPanelCount)
        minCount = str(minCount)
        selectPanel = 'SELECT AVG(energyw) AS energy, module, ' \
            ' count(*) AS count, substr(timestamp, 12 ,2) AS hour ' \
            ' FROM ' + self.table + \
            ' WHERE timestamp >= ? AND timestamp <= ? ' \
            ' GROUP BY module, hour HAVING count(*) >' + minPanelCount + ' ' \
            ' ORDER BY module, hour ;'
        selectAll = 'SELECT AVG(energyw) AS energy, ' \
            ' count(*) AS count, substr(timestamp, 12 ,2) AS hour ' \
            ' FROM ' + self.table + \
            ' WHERE timestamp >= ? AND timestamp <= ? ' \
            ' GROUP BY hour HAVING count(*) >' + minCount + ' ' \
            ' ORDER BY hour ;'
        self.c.execute(selectAll, (start, end))
        AllPanels = self.c.fetchall()
        allPanels = {}
        panels = {}
        
        for rec in AllPanels:
            allPanels[rec['hour']] = rec['energy']
            panels[rec['hour']] = {}
        self.c.execute(selectPanel, (start, end))
        Panels = self.c.fetchall()
        
        for rec in Panels:
            panels[rec['hour']][rec['module']] = rec['energy']
            #print('Panel', rec['hour'], rec['module'], panels[rec['hour']][rec['module']])
        return allPanels, panels

    def showProduction(self, allPanels, panels, module2name, title, start, end):
        fmtHR = '{:>7.1f} {:>6.1f}%    '
        fmtHd = '{:>7s} {:>7s}    '
        fmtNm = '{:^14s} '
        hdr1 = fmtNm.format(' ')
        hdr2 = fmtNm.format('Panel')
        for hr in allPanels.keys():
            hhmm = hr + '00'
            hdr1 += fmtHd.format('Energy', 'Delta')
            hdr2 += fmtHd.format(hhmm, hhmm)
        hdr0 = title + ' : ' + str(start) + ' - ' + str(end)
        fmtH0 = '{:^' + str(len(hdr1)) + 's}'
        hdr0 = fmtH0.format(hdr0)
        print(hdr0, '\n')
        print(hdr1)
        print(hdr2)
            
        for module in module2name.keys():
            line = fmtNm.format(module2name[module])
            #print(module, module2name[module])
            for hr in allPanels.keys():
                #print('hr', hr, panels[hr].keys())
                try:
                    panel = panels[hr][module]
                except KeyError:
                    panel = 0.0
                        
                if allPanels[hr] == 0.0:
                    normalized = 0.0
                else:
                    normalized = panel / allPanels[hr] * 100.0 - 100.0
                line += fmtHR.format(panel, normalized)
            print(line)
        print('')
        
def adapt_datetime(dt):
    return dt.isoformat(sep=' ')

def convert_datetime(val):
    return dt.datetime.fromisoformat(val).replace('T', ' ')

def main():
    pi = PanelInfo()
    module2Name = pi.getModule2Name()
    for k in module2Name.keys():
        #print(k, ':', module2Name[k])
        pass

    pd = PanelData()
    start, end, name = getTimeInterval.getPeriod('Prev7days')
    allPanels, panels = pd.getProduction(start, end)
    pd.showProduction(allPanels, panels, module2Name, 'Previous 7 Days', start, end)

    first, last = pd.getYears()
    for year in range(last.year, first.year - 1, -1):
        start, end, name = getTimeInterval.getPeriod('Year', year = year)
        allPanels, panels = pd.getProduction(start, end, minPanelCount = 9, minCount = 99)
        pd.showProduction(allPanels, panels, module2Name, 'Year ' + str(year), start, end)
        
    start, end, name = getTimeInterval.getPeriod('All')
    allPanels, panels = pd.getProduction(start, end, minPanelCount = 9, minCount = 99)
    pd.showProduction(allPanels, panels, module2Name, 'All Data', start, end)

    
if __name__ == '__main__':
  main()
