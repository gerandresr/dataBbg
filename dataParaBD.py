# -*- coding: utf-8 -*-
"""
Created on Wed Aug 11 21:12:06 2021

@author: gerar
"""

'''
obtengo datos de distintas fuentes y cargo a base de datos por definir

ruta BD amazon:
https://aws.amazon.com/es/free/?sc_icontent=awssm-evergreen-free_tier&sc_iplace=2up&trk=ha_awssm-evergreen-free_tier&sc_ichannel=ha&sc_icampaign=evergreen-free_tier&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all
'''


import os
FOLDER = 'D:/python/'
os.chdir(FOLDER)

from datetime import date
import chileanCalendar as ccl
import dateFormulas as dtf
import dataFromBbg as bbg
import json

# llamo calendarios para ocupar formulas
cal = ccl.CLTradingCalendar()
bday_cl = ccl.CustomBusinessDay(calendar=cal)

'''
print(mtd(cal, bday_cl, fecha, mo=i))
print(ytd(cal, bday_cl, fecha, yr=i))
'''

hoy = date.today()
desde = date(2020, 12, 31)
df = bbg.BBG(secty='CLP Curncy', fld="PX_LAST", start=desde, end=hoy)

toBD = df.to_json()


ccy = ['MXN Curncy', 'EUR Curncy', 'GBP Curncy', 'AUD Curncy',
       'MXNCLP Curncy', 'EURCLP Curncy', 'GBPCLP Curncy', 'AUDCLP Curncy',]
df = bbg.BBG(secty=ccy, fld="PX_LAST")
