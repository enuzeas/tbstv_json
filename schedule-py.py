
#-*- coding: utf-8 -*-

import requests
import xmltodict,json
import pandas as pd
import datetime,time
import schedule

def make_resp():
    channel = 'CH_B'
    today = datetime.date.today().strftime("%Y%m%d")
    url = "http://192.168.20.32:8080/SOAP/services/CisServicePort"
    querystring = {"WSDL":""}
    payload = u"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:ser="http://service.cxf.cis.com/">
                    <soapenv:Header/>
                    <soapenv:Body>
                    <ser:CIS_IF_101>
                    <!--Optional:-->
                    <chan_cd>{}</chan_cd>         <!--Optional:-->
                    <trff_ymd>{}</trff_ymd>         <!--Optional:-->
                    <trff_clf>0</trff_clf>         <!--Optional:-->
                    <trff_no>1</trff_no>
                    </ser:CIS_IF_101>
                    </soapenv:Body>
                    </soapenv:Envelope>""".format(channel,today)
    headers = {
        'Content-Type': "text/xml; charset=utf-8",
        'cache-control': "no-cache",
        'Postman-Token': "86c563e4-ffda-4607-bc04-458e76c8df61"

            }
    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    o = xmltodict.parse(response.text)  #XML to Dictionary
    #json_data = json.dumps(o, indent=4, ensure_ascii=False).encode('utf-8')
    result0=o['soap:Envelope']['soap:Body']['ns2:CIS_IF_101Response']['return']#filtering useless netsted tag from soap xml
    dfobj = pd.DataFrame(result0) #make list to DataFrame

#    dfobj.to_excel('/home/enuzeas/schedule-py/json/tbstv_schedule_{}.xls'.format(today)) #excel exporting
#    dfobj.to_json('/home/enuzeas/schedule-py/json/tbstv_schedule_{}.json'.format(today),orient='index') #export Dataframe to json file to path
   # dfobj.to_json('/home/enuzeas/schedule-py/json/db.json',orient='index')	
    dfobj_live=dfobj[dfobj['mtrl_info']=='(Live)'] #select rows which mtrl_info is (Live)
#    dfobj_live.to_json('/home/enuzeas/schedule-py/jsserver/db.json',orient='index')

    today = datetime.date.today().strftime("%Y%m%d")
    tomorrow = str(int(today) +1) 
    dfobj.loc[dfobj['brd_hm'].astype('int64') < 2359, 'day_changer'] = today
    dfobj.loc[dfobj['brd_hm'].astype('int64') >= 2359, 'day_changer'] = tomorrow 

    dfobj['datetime']=pd.to_datetime(dfobj['day_changer']+dfobj['view_hm'])
    dfobj['time']=dfobj['day_changer']+dfobj['view_hm']
    dfobj['trff_seq']=dfobj['trff_seq'].astype(int)
    dfobj['trff_time'] = pd.to_datetime(dfobj['trff_time'],format= '%H%M%S%f' ).dt.time
    dfobj['trff_ymd'] = pd.to_datetime(dfobj['trff_ymd'],format= '%Y%m%d')
#dfobj['trff_time'] = pd.to_datetime(dfobj['trff_time'],format= '%H%M%S%f').dt.time
    dfobj['trff_run'] = pd.to_datetime(dfobj['trff_run'],format= '%H%M%S%f' ).dt.time
    dfobj =dfobj[dfobj['mtrl_clf'].str.contains("ZP")]
    dfobj.sort_values("trff_seq", axis = 0, ascending = True, inplace = True, na_position ='last') 
#dfobj.drop_duplicates(subset ="datetime",keep = False, inplace = True)  # drop duplicate row 
    dfobj['idx'] = dfobj.reset_index().index
    dfobj.set_index('idx',inplace=True)
    dfobj[['datetime','trff_ymd','trff_time','trff_run','brd_typ','pgm_id','pgm_nm']].to_json('/home/enuzeas/schedule-py/json/db.json',orient='index')
   
    
    dfobj[['datetime','chan_cd','trff_ymd','trff_time','trff_run','brd_typ','pgm_id','pgm_nm','mtrl_nm']].to_csv('/home/enuzeas/odbc_conn/csv_/tbstv_schedule_{}.csv'.format(today),index=False)
make_resp()

