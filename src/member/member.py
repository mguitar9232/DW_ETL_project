#-*- coding=utf-8 -*-
################################################################################
################################################################################

import sys
from time import strftime, gmtime, localtime

import cx_Oracle
from datetime import date, timedelta, datetime

sys.path.insert(0, '../icrypto')
import icrypto

import os
os.environ["NLS_LANG"] = ".KO16MSWIN949"

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
p_job_id = 'TEST000'
#v_dm_com_tp = ''
v_user_etl_start_date = ''
v_user_etl_end_date = ''
v_sysdate = ''

# input db connection info
inusername = icrypto.input_id(8)
inpassword = icrypto.input_pwd(8)
indatabaseName = "TESTDB01"

# output db connection info
outusername = icrypto.input_id(1)
outpassword = icrypto.input_pwd(1)
outdatabaseName = "TESTDB02"

# procedure call variable
v_stat_cd_s  = '1'   #LOG 프로시져 시작 코드
v_stat_cd_ok = '2'   #LOG 프로시져 종료 코드
v_stat_cd_f  = '3'   #LOG 프로시져 오류 코드
#p_job_id = ''
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
#current_time = strftime("%Y%m%d%H%M%S", gmtime())
#yesterday = strftime("%Y%m%d", datetime.now() - timedelta(days=1))
#printf("TODAY=[%s]:::YESTERDAY=[%s]:::TIME=[%s]\n", today, yesterday, current_time)

#########################################
####  DM_COM_TP  Definition         ####
#########################################
argc = len(sys.argv)
if(argc>=1):
    if (argc==1):
        #v_dm_com_tp = '03'
        printf('ok \n')
    elif(argc==3):
        v_user_etl_start_date = sys.argv[1]
        v_user_etl_end_date = sys.argv[2]
    else:
        printf('[Error]Argument count is wrong. \n')
        #printException (exception)
        exit(1)
else:
    printf('[Error]Argument is not enough.(ex: pgm_nm arg1 arg2) \n')
    #printException (exception)
    exit(1)

########## CONNECT TARGER(OUT) DB ##########
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
                     SELECT TO_CHAR(NVL(TO_DATE(A.LAST_UPD_DATE,'YYYYMMDD') + INTERVAL '1' DAY, SYSDATE), 'YYYYMMDD') AS ETL_START_DATE
                          , TO_CHAR(SYSDATE -1, 'YYYYMMDD') AS ETL_END_DATE
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

########## GET DB sysdate 추출 ##########
try:
  sysdate_sql = """ SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS')  AS V_SYSDATE
                   FROM DUAL """

  outcursor.execute(sysdate_sql)
  fetch_sysdate = outcursor.fetchall()
  v_sysdate = fetch_sysdate[0][0]

except cx_Oracle.DatabaseError, exception:
    printf('Failed to select SYSDATE \n')
    outcursor.close()
    outconn.close()
    printException(exception)
    exit(1)

printf("--[step1]ok~!! (%s) ---\n", current_time)
printf("p_job_id              = [%s]\n", p_job_id)
printf("v_user_etl_start_date = [%s]\n", v_user_etl_start_date)
printf("v_user_etl_end_date   = [%s]\n", v_user_etl_end_date)
printf("p_etl_start_date      = [%s]\n", p_etl_start_date)
printf("p_etl_end_date        = [%s]\n", p_etl_end_date)
printf("---ing ..........       \n")

########## JOB_LOG Procedure(Start)  ##########
job_log(v_stat_cd_s)

########## CONNECT SOURCE(IN) DB ##########
try:
  inconn = cx_Oracle.connect (inusername,inpassword,indatabaseName)
  incursor = inconn.cursor()
except cx_Oracle.DatabaseError, exception:
  printf ('Failed to connect(in) to %s\n',indatabaseName)
  outcursor.close()
  outconn.close()
  printException (exception)
  exit (1)

########## MAIN SELECT ##########
in_sql = """
SELECT MEM_NO
      ,SEX
      ,substr(HOUSE_ZIP_ADDR,1,5) as HOUSE_ZIP_ADDR
      ,GRD_GRANT_DT
  FROM TEST000
 WHERE MOD_DTS >=TO_DATE(:p_etl_start_date,'YYYYMMDDHH24MISS') AND MOD_DTS <=TO_DATE(:p_etl_end_date,'YYYYMMDDHH24MISS') 
        """

try:
  #incursor.execute (in_sql)
  incursor.execute(in_sql, (p_etl_start_date, p_etl_end_date))   #1일 1회 실행

except cx_Oracle.DatabaseError, exception:
  printf ('Failed to select from TABLE\n')
  outcursor.close()
  outconn.close()
  incursor.close()
  inconn.close()
  printException (exception)
  exit (1)

########## FETCH(result_set)  ########
result_set = incursor.fetchall()
p_source_cnt = p_source_cnt + incursor.rowcount
#printf("-<fetch cnt = %d>---\n", incursor.rowcount)

########## MAIN PROCESS  ########
for rows in result_set:
    p_target_cnt += 1
    data_params = {  "1": rows[ 0],  "2": rows[ 1],  "3": rows[ 2],  "4": rows[ 3], "v_sysdate": v_sysdate }

    merge_sql = '''
           MERGE INTO MEMBER T
                USING (
                        SELECT :1	as 	MEM_NO
                              , :2	as 	SEX
                              , :3	as 	HOUSE_ZIP_ADDR
                              , :4	as 	GRD_GRANT_DT
                          FROM DUAL
                      ) S
                ON (    T.MEM_NO = S.MEM_NO
                    )
                WHEN MATCHED THEN
                     UPDATE
                        SET T.SEX = S.SEX
                            ,T.HOUSE_ZIP_ADDR = S.HOUSE_ZIP_ADDR
                            ,T.HOUSE_ZIPCD = S.HOUSE_ZIPCD
                            ,T.GRD_GRANT_DT = S.GRD_GRANT_DT
                WHEN NOT MATCHED THEN
                     INSERT(T.MEM_NO
                            ,T.SEX
                            ,T.HOUSE_ZIP_ADDR
                            ,T.GRD_GRANT_DT
                            )
                     VALUES(S.MEM_NO
                            ,S.SEX
                            ,S.HOUSE_ZIP_ADDR
                            ,S.GRD_GRANT_DT
                            )   
                '''

    try:
        ########## MERGE PROCESS  ########
        outcursor.prepare(merge_sql)
        #outcursor.execute(merge_sql, data_params )
        outcursor.execute(merge_sql, data_params )
    except cx_Oracle.DatabaseError, exception:
        error, = exception.args

        ## 존재하는 데이터는 UPDATE 처리
        if (error.code == 1):
            try:
                ########## UPDATE PROCESS  ########
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
            printf('Failed to INSERT(not duplication) or MERGE \n')
            job_log(v_stat_cd_f)
            incursor.close()
            inconn.close()
            printException(exception)
            raise exception
    else:
        #성공 건수
        p_update_cnt += 1

######## 제외대상 DELETE 처리(loop밖에서 한번만 수행)   ########
printf(">>>> step2 (%s)\n", strftime("%Y/%m %d %H:%M:%S", localtime()))
delete_sql_d1 = '''
                DELETE FROM TEST000
                WHERE DM_MOD_DTS IS NULL OR DM_MOD_DTS <> TO_DATE(:v_sysdate,'YYYYMMDDHH24MISS')
            '''
try:
    outcursor.prepare(delete_sql_d1)
    outcursor.execute(delete_sql_d1, {'v_sysdate': v_sysdate})

except cx_Oracle.DatabaseError, exception:
    job_log(v_stat_cd_f)
    incursor.close()
    inconn.close()
    printException(exception)
    raise exception

########## JOB_LOG Procedure(END)  ##########
job_log(v_stat_cd_ok)

incursor.close ()
inconn.close ()

outcursor.close ()
outconn.commit()
outconn.close ()

printf(">>>>[OK-END] p_update_cnt =[%s] (%s)\n", p_update_cnt, strftime("%Y/%m %d %H:%M:%S", localtime()))


exit (0)
