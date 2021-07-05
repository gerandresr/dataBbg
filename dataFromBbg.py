# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 15:52:21 2021

@author: gerar
"""
'''
from __future__ import print_function
from __future__ import absolute_import
'''
from optparse import OptionParser

import blpapi
from datetime import date
import pandas as pd

def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()

    return options


def simpleData(secty, fld):
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)
    
    # Create a Session
    session = blpapi.Session(sessionOptions)
    
    # Start a Session
    if not session.start():
        raise Exception("Failed to start session.")
    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            raise Exception("Failed to open //blp/refdata")
    
        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")
    
        # Create and fill the request for the historical data
        request = refDataService.createRequest("ReferenceDataRequest")
        for i in secty:
            request.getElement("securities").appendValue(i)
        for i in fld:
            request.getElement("fields").appendValue(i)

        # Send the request
        cid = session.sendRequest(request)

        # Process received events
        count = 0
        secus = []
        field = []
        value = []
        while(True):
            ev = session.nextEvent(500)
            #global msg
            for msg in ev:
                if cid in msg.correlationIds():
                    subc = msg.getElement("securityData")

                    while count < subc.numValues():
                        ssc = subc.getValue(count)
                        # dates and fields
                        for i in fld:
                            secus.append(ssc.getElement('security').getValue())
                            field.append(i)
                            value.append(subc.getValue(count)\
                                             .getElement("fieldData")\
                                             .getElement(i)\
                                             .getValue())
                        count += 1

            if ev.eventType() == blpapi.Event.RESPONSE:
                session.stop()
                return pd.DataFrame(list(zip(secus, field, value)),
                                    columns =['security',
                                              'field',
                                              'value'])
    finally:
        session.stop()


def historicalData(secty, fld, start, end):
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)
    
    # Create a Session
    session = blpapi.Session(sessionOptions)
    
    # Start a Session
    if not session.start():
        raise Exception("Failed to start session.")
    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            raise Exception("Failed to open //blp/refdata")
    
        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")
    
        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        for i in secty:
            request.getElement("securities").appendValue(i)
        for i in fld:
            request.getElement("fields").appendValue(i)
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "DAILY")
        request.set("startDate", start)
        request.set("endDate", end)

        # Send the request
        cid = session.sendRequest(request)

        # Process received events
        finish = False
        secus = []
        dates = []
        field = []
        value = []
        while(True):
            count = 0
            ev = session.nextEvent(500)
            for msg in ev:
                if cid in msg.correlationIds():
                    subc = msg.getElement("securityData")\
                              .getElement("fieldData")

                    secu = msg.getElement("securityData")\
                              .getElement("security").getValue()

                    while count < subc.numValues():
                        ssc = subc.getValue(count)
                        # dates and fields
                        for i in fld:
                            secus.append(secu)
                            field.append(i)
                            value.append(ssc.getElement(i).getValue())
                            dates.append(ssc.getElement('date').getValue())
                        count += 1

            if ev.eventType() == blpapi.Event.RESPONSE:
                finish = True

            if (ev.eventType() != blpapi.Event.RESPONSE) and (finish):
                session.stop()
                return pd.DataFrame(list(zip(dates, secus, field, value)),
                                    columns =['date', 'security',
                                              'field', 'value'])
    finally:
        session.stop()


def BBG(secty, fld, start=None, end=None):
    if not isinstance(secty, list):
        secty = [secty]
    if not isinstance(fld, list):
        fld = [fld]
    if isinstance(start, date):
        start = start.strftime('%Y%m%d')
    if isinstance(end, date):
        end = end.strftime('%Y%m%d')

    if start is not None and end is None:
        end = date.today().strftime('%Y%m%d')
    
   # get data
    if start is None or end is None:
        return simpleData(secty, fld)
    else:
        return historicalData(secty, fld, start, end)


if __name__ == "__main__":
        '''
    #value = BBG(secty=['CLP Curncy', 'MXN Curncy'], fld=["PX_LAST","PREV_CLOSE_VALUE_REALTIME"])

    df = BBG(secty=['MXN Curncy', 'EUR Curncy'], fld="PX_LAST", start=date(2021, 4, 21))

    df = BBG(secty='CLP Curncy', fld="PX_LAST",
             start=date(2021, 4, 21), end=date(2021, 4, 21))
    '''