#coding=utf-8
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
# program exec info
p_job_id = 'MST_CALENDAR'  # 최종 p_job_id는 사업부명 결합함([예] 쇼핑:MST_DM_DISPLAY_CLASS_SHOP / 도서:MST_DM_DISPLAY_CLASS_BOOK
v_dm_com_tp = ''
v_user_etl_start_date = ''
v_user_etl_end_date = ''

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
###p_job_id = 'MST_DM_DISPLAY_CLASS_SHOP'
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
if(argc>=2):
    if(argc==2):
        v_dm_com_tp = sys.argv[1]
    elif(argc==4):
        v_dm_com_tp = sys.argv[1]
        v_user_etl_start_date = sys.argv[2]
        v_user_etl_end_date = sys.argv[3]
    else:
        printf('[Error]Argument count is wrong. \n')
        #printException (exception)
        exit(1)
else:
    printf('[Error]Argument is not enough.(ex: pgm_nm arg1 arg2 arg3) \n')
    #printException (exception)
    exit(1)

if(v_dm_com_tp=="03"):
    # input db connection info(BOOK)
    inusername = icrypto.input_id(3)
    inpassword = icrypto.input_pwd(3)
    indatabaseName = "BOOKDB"
    p_job_id = p_job_id + "_BOOK"
else:
    printf('[Error]v_dm_com_tp is wrong.\n')
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
    if (v_user_etl_start_date == ""):
        job_date_sql = """
                      SELECT TO_CHAR(NVL(MIN(TO_DATE(A.LAST_UPD_DATE,'YYYYMMDDHH24MISS') + INTERVAL '1' SECOND), SYSDATE), 'YYYYMMDDHH24MISS') AS ETL_START_DATE
                           , TO_CHAR(SYSDATE, 'YYYYMMDDHH24')||'0000'  AS ETL_END_DATE
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

printf("--[step1]ok~!! (%s) ---\n", current_time)
printf("v_dm_com_tp           = [%s]\n", v_dm_com_tp)
printf("p_job_id              = [%s]\n", p_job_id)
printf("v_user_etl_start_date = [%s]\n", v_user_etl_start_date)
printf("v_user_etl_end_date   = [%s]\n", v_user_etl_end_date)
printf("p_etl_start_date      = [%s]\n", p_etl_start_date)
printf("p_etl_end_date        = [%s]\n", p_etl_end_date)

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
in_sql = ""
if(v_dm_com_tp=='03'):
    in_sql = """
            SELECT A.WORK_DT     AS WORK_DT
                 , B.YOY_WORK_DT AS YOY_WORK_DT
              FROM WORK_CALENDAR A
                 , (
                     SELECT B.CODE1   AS WORK_DT
                          , B.DIV_RT  AS YOY_WORK_DT
                       FROM BSN_DIV_RATE B
                      WHERE B.TYPE_CD = 'YOY'
                        AND B.CODE1 BETWEEN TO_CHAR(TO_DATE(SUBSTR(:p_etl_start_date,1,8),'YYYYMMDD')-365,'YYYYMMDD')  
                                        AND TO_CHAR(TO_DATE(SUBSTR(:p_etl_end_date,  1,8),'YYYYMMDD')+365,'YYYYMMDD')
                   ) B
             WHERE A.WORK_DT = B.WORK_DT(+)
               AND A.WORK_DT BETWEEN TO_CHAR(TO_DATE(SUBSTR(:p_etl_start_date,1,8),'YYYYMMDD')-365,'YYYYMMDD')  
                                 AND TO_CHAR(TO_DATE(SUBSTR(:p_etl_end_date,  1,8),'YYYYMMDD')+365,'YYYYMMDD')
           """


else:
    printf('[Error]v_dm_com_tp is wrong.\n')
    outcursor.close()
    outconn.close()
    exit(1)

########## EXECUTE  ########
try:
  #incursor.execute (in_sql)
  #incursor.execute(in_sql, (p_etl_start_date, p_etl_end_date))
  incursor.execute(in_sql, (p_etl_start_date, p_etl_end_date))

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
    data_params = { "1": rows[0], "2": rows[1]}

    merge_sql = '''
                MERGE INTO CALENDAR T
                USING (
                        SELECT :1   AS WORK_DT   
                             , :2   AS YOY_WORK_DT    
                          FROM DUAL
                      ) S
                ON (    T.WORK_DT   = S.WORK_DT)
                WHEN MATCHED THEN
                     UPDATE
                        SET T.YOY_WORK_DT = S.YOY_WORK_DT         
                WHEN NOT MATCHED THEN
                     INSERT(T.WORK_DT  
                          , T.YOY_WORK_DT    
                          , T.REG_DTS )
                     VALUES(S.WORK_DT  
                          , S.YOY_WORK_DT
                          , SYSDATE  ) 
                '''

    try:
        ########## MERGE PROCESS  ########
        outcursor.prepare(merge_sql)
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
    #finally:
      #############################
      # Work Table(JOB_LOG) Procedure Write ( END )
      #############################

########## JOB_LOG Procedure(END)  ##########
job_log(v_stat_cd_ok)

incursor.close ()
inconn.close ()

outcursor.close ()
outconn.commit()
outconn.close ()

exit (0)
