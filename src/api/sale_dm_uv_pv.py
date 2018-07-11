# -*- coding=utf-8 -*-
################################################################################
## 프로그램명 : 일별 UV/LV/PV 집계
##------------------------------------------------------------------------------
## 비      고 : <DM_UV_PV.DM_COM_TP 값 유형>
##               UV: User View
##               LV: Login View
##               PV: Page View
##               API URL : https://da-api.test.com/api/v1/usage_stat
##               [ test 서버의 API 를 호출해서 DB에 INSERT )
################################################################################

import sys
from time import strftime, gmtime, localtime

import cx_Oracle
from datetime import date, timedelta, datetime

sys.path.insert(0, '../icrypto')
import icrypto

import os
os.environ["NLS_LANG"] = ".KO16MSWIN949"

import requests
import json

params = {'page':2, 'per_page':100}

# Replace with the correct URL
url = "https://da-api.test.com/api/v1/usage_stat?date="
#url = "http://mercury.inpark.kr/api/v1/usage_stat?date="

def printf (format,*args):
  sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);

def job_log(v_stat_cd):
  try:
    pparam = (v_stat_cd, p_job_id, p_source_cnt, p_target_cnt, p_update_cnt, p_etl_start_date, p_etl_end_date, p_rework_cd, p_sql_code, p_message, p_out_msg)
    outcursor.callproc("DMS.SP_JOB_LOG", pparam)
  except cx_Oracle.DatabaseError, exception:
    printf('Failed to call JOB Procedure(s_stat_code=[%s])\n', v_stat_cd)
    printException(exception)
    exit(1)

#########################################
####  Common Variable Definition    #####
#########################################
# program info
p_job_id = 'SALE_DM_UV_PV'
v_dm_com_tp = ''
v_user_etl_start_date = ''
v_user_etl_end_date = ''
v_workdate = ''

# input db connection info
inusername = ''
inpassword = ''
indatabaseName = ""

# output db connection info
outusername = icrypto.input_id(1)
outpassword = icrypto.input_pwd(1)
outdatabaseName = "TESTDB"

# procedure call variable
v_stat_cd_s  = '1'   #LOG 프로시져 시작 코드
v_stat_cd_ok = '2'   #LOG 프로시져 종료 코드
v_stat_cd_f  = '3'   #LOG 프로시져 오류 코드
p_source_cnt = 0
p_target_cnt = 0
p_update_cnt = 0
p_etl_start_date = ''
p_etl_end_date = ''
p_rework_cd = ''
p_sql_code = ''
p_message = ''
p_out_msg = ''

###### GET CURRENT TIME
today = date.today()
current_time = strftime("%Y%m%d%H%M%S", localtime())

#########################################
####  DM_COM_TP  Definition         ####
#########################################
argc = len(sys.argv)
if(argc>=1):
    if(argc==1):
        printf("")
    elif(argc==2):
        v_user_etl_start_date = sys.argv[1]
        #v_user_etl_end_date = sys.argv[2]
    else:
        printf('[Error]Argument count is wrong. \n')
        #printException (exception)
        exit(1)
else:
    printf('[Error]Argument is not enough.(ex: pgm_nm arg1 arg2 arg3) \n')
    #printException (exception)
    exit(1)

##################################################
#### 사업부별 고유한 정보값 세팅(DB, JOB명등) ####
##################################################
#inconn, outconn 동일한 작업이므로 outconn만 사용

######### CONNECT TARGER(OUT) DB ##########
try:
  outconn = cx_Oracle.connect (outusername,outpassword,outdatabaseName)
  outcursor = outconn.cursor()
except cx_Oracle.DatabaseError, exception:
  printf ('Failed to connect(out) to %s\n',outdatabaseName)
  printException (exception)
  exit(1)

########## GET ETL DATE   ##########
try:
  #### DEFAULT PROCESS ####
  if(v_user_etl_start_date==""):
      job_date_sql = """ 
                        SELECT TO_CHAR(SYSDATE-1, 'YYYYMMDD') AS ETL_START_DATE
                           , TO_CHAR(SYSDATE -1, 'YYYYMMDDHH24')  AS ETL_END_DATE
                        FROM JOB_MASTER A
                       WHERE A.JOB_ID = :p_job_id 
                     """

      outcursor.execute(job_date_sql, (p_job_id,))
      job_date = outcursor.fetchall()

      ## ETL 기준일 세팅
      p_etl_start_date = job_date[0][0]
      p_etl_end_date = job_date[0][1]

  #### 사용자 지정일 기준으로 처리 ####
  else:
      ## ETL 기준일 세팅
      p_etl_start_date = v_user_etl_start_date
      p_etl_end_date = v_user_etl_end_date
except cx_Oracle.DatabaseError, exception:
  printf ('Failed to select JOB_DATE \n')
  outcursor.close()
  outconn.close()
  printException (exception)
  exit (1)

print("####### work_date" + p_etl_start_date)

printf("--[step1]ok~!! (%s) ---\n", current_time)
printf("p_job_id              = [%s]\n", p_job_id)
printf("v_user_etl_start_date = [%s]\n", v_user_etl_start_date)
printf("v_user_etl_end_date   = [%s]\n", v_user_etl_end_date)
printf("p_etl_start_date      = [%s]\n", p_etl_start_date)
printf("p_etl_end_date        = [%s]\n", p_etl_end_date)
printf("v_workdate             = [%s]\n", v_workdate)

########## JOB_LOG Procedure(Start)  ##########
job_log(v_stat_cd_s)

########## CONNECT SOURCE(IN) DB ##########

work_url = url + p_etl_start_date

print("WORK_URL" + " " + work_url)

# UV-PV API CALL[GET]
myResponse = requests.get(work_url, params=params)

# OK : 200 return
if(myResponse.ok):
   jData = json.loads(myResponse.content)

   # -1 을 넣은 이유 -> service_code 가 'inter' 인 것을 걸러 내기 위해(맨 마지막 부분)

   for i in range(0, len(jData['data']['usage_stat'])-1):
       dm_code = jData['data']['usage_stat'][i]['service_code']
       uv = str(jData['data']['usage_stat'][i]['unique_visit'])
       pv = str(jData['data']['usage_stat'][i]['page_view'])

       #if pv == '-1':
       #   pv = 0       
       #   print("## PV=[%s]\n", pv)

       lv = str(jData['data']['usage_stat'][i]['logged_in_user_visit'])

       if dm_code == 'shop':
          dm_com_tp = '02'
       elif dm_code == 'book':
          dm_com_tp = '03'
       elif dm_code == 'ticket':
          dm_com_tp = '04'
       elif dm_code == 'tour':
	  exit(1)
          # tour 는 mercury.inpark.kr/.... v2 URL 을 사용할 것
          #dm_com_tp = '05'
       else:
          print("ELSE=" + dm_code)


#print("END!!")
#exit(1)

       print("rev_dt     " + p_etl_start_date)
       print("dm_com_tp  " + dm_com_tp)
       print("dm_code " +  " " + dm_code)
       print("uv      " +  " " + uv)
       print("pv      " +  " " + pv)
       print("lv      " +  " " + lv)
       print(i)

       insert_sql = '''
               INSERT INTO DMS.DM_UV_PV (REV_DT, DM_COM_TP, UV, LV, PV)
                    VALUES(SUBSTR(:p_etl_start_date,1,8), :dm_com_tp, DECODE(:uv, '-1','0', :uv), DECODE(:lv, '-1','0', :lv), DECODE(:pv, '-1','0', :pv))
       '''
       update_sql = '''
               UPDATE DMS.DM_UV_PV 
                  SET UV = DECODE(:uv, '-1','0', :uv),
                      LV = DECODE(:lv, '-1','0', :lv),
                      PV = DECODE(:pv, '-1','0', :pv)
                WHERE REV_DT = SUBSTR(:p_etl_start_date, 1,8)
                  AND DM_COM_TP = :dm_com_tp
       '''

       try:
           outcursor.prepare(insert_sql)
           outcursor.execute(insert_sql, {'p_etl_start_date': p_etl_start_date, 'dm_com_tp' : dm_com_tp, 'uv' : uv, 'lv' : lv, 'pv' : pv})
           p_source_cnt = outcursor.rowcount
           p_target_cnt = outcursor.rowcount
           p_update_cnt = outcursor.rowcount
           outconn.commit()

       except cx_Oracle.DatabaseError, exception:
           error, = exception.args

           if (error.code == 1):
               #printf("ORA-00001 ERROR ==> UPDATE start")
               #printException(exception)
               try:
                   ########## UPDATE PROCESS  ########
                   outcursor.prepare(update_sql)
                   outcursor.execute(update_sql, {'p_etl_start_date': p_etl_start_date, 'dm_com_tp' : dm_com_tp, 'uv' : uv, 'lv' : lv, 'pv' : pv})
                   p_source_cnt = outcursor.rowcount
                   p_target_cnt = outcursor.rowcount
                   p_update_cnt = outcursor.rowcount
                   outconn.commit()
                   printf("")  #SQL MERGE 문으로 처리하기때문에 SQL UPDATE문이 없고 DUPLICATION 오류가 나올 수 없으므로 주석처리함.
               except cx_Oracle.DatabaseError, exception:
                   job_log(v_stat_cd_f)
                   incursor.close()
                   inconn.close()
                   printException(exception)
                   raise exception
               else:
                   # 성공 건수
                   p_update_cnt += 1
           else:
               #############################
               # Work Table(JOB_LOG) Procedure Write ( ERROR )
               #############################
               printf('Failed to INSERT\n')
               #outcursor.callproc("DMS.SP_JOB_LOG", fparam)
               job_log(v_stat_cd_f)
               incursor.close()
               inconn.close()
               printException(exception)
               raise exception

else:
   myResponse.raise_for_status()

########## JOB_LOG Procedure(END)  ##########
job_log(v_stat_cd_ok)

outcursor.close ()
outconn.commit()
outconn.close ()

printf(" <<<ok>>> p_update_cnt =[%s]\n",p_update_cnt)
printf("end time =[%s]", strftime("%Y/%m %d %H:%M:%S", localtime()))

exit (0)
