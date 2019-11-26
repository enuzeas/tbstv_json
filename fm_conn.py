#cibis  db_open
import pyodbc
import pandas as pd 
import datetime

today = datetime.date.today().strftime("%Y%m%d")

server = 'tcp:1.1.1.153'
database = 'Cibis'
username = 'sa'
password = 'ryxhdqkdthd09@!'
driver = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.4.so.2.1"
cnxn = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)
cursor = cnxn.cursor()

#cursor.execute("SELECT @@version;")
#rows = cursor.fetchone()

#for row in rows:
#    print(row)

#cursor.execute(eFM_qry)
#rows = cursor.fetchall()

#for row in rows:
#    print (row)
FM_qry = """SELECT scplaylst.chan_cd,   
                 scplaylst.trff_ymd,   
                 scplaylst.trff_seq,   
                 scplaylst.trff_time,   
                 scplaylst.trff_run,   
                 scplaylst.studio,   
                 scplaylst.pgm_id,   
                 scplaylst.scl_clf,   
                 scplaylst.prfx_no,   
                 scplaylst.rec_clf,   
                 scplaylst.trff_typ,   
                 scplaylst.cue_clf,   
                 scplaylst.pd_nm,   
                 scplaylst.mc_nm,   
                 scplaylst.mtrl_id,   
                 scplaylst.mtrl_nm,   
                 scplaylst.mtrl_info,   
                 scplaylst.house_no,   
                 scplaylst.mtrl_clf,   
                 scplaylst.arc_loc,   
                 scplaylst.bin_no,   
                 scplaylst.trff_info1,   
                 scplaylst.trff_info2,   
                 scplaylst.trff_info3,   
                 scplaylst.trff_info4,   
                 scplaylst.trff_info5,   
                 scplaylst.regr,   
                 scplaylst.reg_dt,   
                 scplaylst.modr,   
                 scplaylst.mod_dt,   
                 scpgmmst.pgm_nm,   
                 daso_list.mtrl_clf,   
                 daso_list.sub_clf,   
                 daso_list.mtrl_seq,   
                 daso_list.mtrl_nm,   
                 daso_list.editor,   
                 daso_list.edittime,   
                 daso_list.house_no,   
                 daso_list.arc_loc,   
                 daso_list.duration,   
                 'N' check_yn,   
                 case when SCPlayLst.scl_clf <> '3' then SCPlayLst.pgm_id else '' end pgm_id1,   
                 case when SCPlayLst.scl_clf = '3' then SCPlayLst.pgm_id else '' end pgm_id2  
            FROM {oj scplaylst LEFT OUTER JOIN scpgmmst ON scplaylst.pgm_id = scpgmmst.pgm_id LEFT OUTER JOIN daso_list ON scplaylst.chan_cd = daso_list.chan_cd AND scplaylst.trff_ymd = daso_list.trff_ymd AND scplaylst.pgm_id = daso_list.pgm_id}  
           WHERE ( scplaylst.chan_cd = 'CH_A' ) AND  
                 ( scplaylst.trff_ymd = %s )    """%today
    
#def copia (argumento):
#    df=pd.DataFrame(argumento)
#    return df    

tableResult = pd.read_sql(FM_qry, cnxn)
#copia(tableResult)
df=pd.DataFrame(tableResult)

PATH = "/home/enuzeas/schedule-py/json/"
PATH2 = "/home/enuzeas/odbc_conn/csv_/"

df['durations'] = pd.to_datetime(df['duration'],format= '%H%M%S' ).dt.time
#df.to_csv(PATH+'tbsfm_schedule_{}.csv'.format(today),index=False)
#df.to_json(PATH+'db_fm_{}.json'.format(today),orient='table')
#df.to_json(PATH+'db_fm.json',orient='table')

df['datetime'] = pd.to_datetime(df['trff_ymd']+df['trff_time'])
df['trff_seq']=df['trff_seq'].astype(int)
df['prfx_no']=df['prfx_no'].astype(int)
df['trff_run'] = pd.to_datetime(df['trff_run'],format= '%H%M%S%f' ).dt.time
df['trff_time'] = pd.to_datetime(df['trff_time'],format= '%H%M%S' ).dt.time
df['trff_ymd'] = pd.to_datetime(df['trff_ymd'],format= '%Y%m%d' ).dt.date

#df.to_csv(PATH2+'tbsfm_schedule_{}.csv'.format(today),index=False)

df = df[df['pgm_id'].str.contains("PG")]
df = df[~df['pgm_id'].str.contains("R000000097")]
df = df[~df['pgm_id'].str.contains("R3")]
df = df[~df['pgm_id'].str.contains("PG2060581A")]

df.sort_values("trff_seq", axis = 0, ascending = True, inplace = True, na_position ='last') 
df.drop_duplicates(subset ="pgm_nm",keep = False, inplace = True)  # drop duplicate row 

df['idx'] = df.reset_index().index  ## index reset
df.set_index('idx',inplace=True)    ## idx as a index inplaced
#df[['datetime','trff_ymd','trff_time','trff_run','pgm_id','pgm_nm','mc_nm','pd_nm']]
df[['datetime','trff_ymd','trff_time','trff_run','trff_typ','pgm_id','pgm_nm','mc_nm','pd_nm']].to_json(PATH+'db_fm.json', orient='index')
##FM생방 스케줄###

df.to_csv(PATH+'tbsfm_schedule_{}.csv'.format(today),index=False)
#df.set_index('datetime',inplace=True)
df[['datetime','chan_cd','trff_ymd','trff_time','trff_run','trff_typ','pgm_id','pgm_nm','mc_nm','pd_nm']].to_csv(PATH2+'tbsfm_schedule_{}.csv'.format(today),index=False)
