#!/usr/bin/python

import java, csv, sys, datetime, os, re

from hec.script import MessageBox
from hec.heclib.dss import HecDss
from hec.heclib.util import HecTime
from hec.io import TimeSeriesContainer

from optparse import OptionParser

sys.path.append("./simplejson-2.5.2")
import simplejson as json

try:
    try:
        print 'Jython version: ', sys.version

        DSS_FILE_PATH = '/home/uwcc-admin/hechms_hourly/2008_2_Events/2008_2_Events_input.dss'

        myDss = HecDss.open(DSS_FILE_PATH)

        flow = myDss.get('//HANWELLA/FLOW//1HOUR/RUN:RUN 1/', 1)

        if flow.numberValues == 0:
            MessageBox.showError('No Data', 'Error')
        else:
            print flow.values[:1], flow.times[:1]
            print flow.values[-1], flow.times[-1]

            csvList = []

            for i in range(0, flow.numberValues):
                # print int(flow.times[i])
                time = HecTime()
                time.set(int(flow.times[i]))

                d = [time.year(), '%d' % (time.month(),), '%d' % (time.day(),)]
                t = ['%d' % (time.hour(),), '%d' % (time.minute(),), '%d' % (time.second(),)]
                if (int(t[0]) > 23):
                    t[0] = '23'
                    dtStr = '-'.join(str(x) for x in d) + ' ' + ':'.join(str(x) for x in t)
                    dt = datetime.datetime.strptime(dtStr, '%Y-%m-%d %H:%M:%S')
                    dt = dt + datetime.timedelta(hours=1)
                else:
                    dtStr = '-'.join(str(x) for x in d) + ' ' + ':'.join(str(x) for x in t)
                    dt = datetime.datetime.strptime(dtStr, '%Y-%m-%d %H:%M:%S')

                csvList.append([dt.strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % flow.values[i]])

            print csvList[:3], "...", csvList[-3:]

    except Exception, e:
        MessageBox.showError(' '.join(e.args), "Python Error")
    except java.lang.Exception, e:
        MessageBox.showError(e.getMessage(), "Error")
finally:
    myDss.done()
    print
    '\nCompleted converting.'
