#-*- coding:utf-8 -*-

import sys
from datetime import date
from time import strftime, localtime

import cx_Oracle

from PYTHON_ETL_SRC import icrypto


def printf (format,*args):
  sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);

def job_log(v_stat_cd):
  try:
    pparam = (v_stat_cd, p_job_id, p_source_cnt, p_target_cnt, p_update_cnt, p_etl_start_date, p_etl_end_date, p_rework_cd, p_sql_code, p_message, p_out_msg)
    #outcursor.callproc("DMS.SP_JOB_LOG", pparam)
    printf("## CURSOR CALL PROCEDURE ##\n");
  except cx_Oracle.DatabaseError, exception:
    printf('Failed to call JOB Procedure(s_stat_code=[%s])\n', v_stat_cd)
    printException(exception)
    exit(1)

#########################################
####  Common Variable Definition    #####
#########################################
# program exec info
p_job_id = 'MST_TEST_PRODUCT'
v_dm_com_tp = ''
v_user_etl_start_date = ''
v_user_etl_end_date = ''
in_sql = ''

# input db connection info
inusername = ''
inpassword = ''
indatabaseName = ""

# output db connection info
outusername = icrypto.input_id(1)
outpassword = icrypto.input_pwd(1)
outdatabaseName = "BIDB"

# procedure call variable
v_stat_cd_s  = '1'   #LOG 프로시져 시작 코드
v_stat_cd_ok = '2'   #LOG 프로시져 종료 코드
v_stat_cd_f  = '3'   #LOG 프로시져 오류 코드
###p_job_id = 'MST_DM_PRODUCT_SHOP'
p_source_cnt = 0
p_target_cnt = 0
p_update_cnt = 0
p_etl_start_date = ''
p_etl_end_date = ''
p_rework_cd = ''
p_sql_code = ''
p_message = ''
p_out_msg = ''

# User Variable Definition
ing_cnt = 0

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

##################################################
#### 업무별 고유한 정보값 세팅(DB, JOB명등) ####
##################################################
if(v_dm_com_tp=="02"):
    inusername = icrypto.input_id(2)
    inpassword = icrypto.input_pwd(2)
    indatabaseName = "TESTDB01"
    p_job_id = p_job_id + "_SHOP"
elif(v_dm_com_tp=="03"):
    # input db connection info(BOOK)
    inusername = icrypto.input_id(3)
    inpassword = icrypto.input_pwd(3)
    indatabaseName = "TESTDB02"
    p_job_id = p_job_id + "_BOOK"
elif(v_dm_com_tp=="04"):
    p_job_id = p_job_id + "_ENT"
elif(v_dm_com_tp == "05"):
    p_job_id = p_job_id + "_TOUR"
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
        job_date_sql = """SELECT TO_CHAR(NVL(TO_DATE(A.LAST_UPD_DATE,'YYYYMMDDHH24MISS') + INTERVAL '1' SECOND, SYSDATE), 'YYYYMMDDHH24MISS') AS ETL_START_DATE
                            , TO_CHAR(SYSDATE, 'YYYYMMDDHH24')||'0000'  AS ETL_END_DATE
                        FROM JOB_MASTER A
                       WHERE A.JOB_ID = :p_job_id"""

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
#job_log(v_stat_cd_s)
printf("## JOB LOG ##\n");

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
if (v_dm_com_tp=="02" or v_dm_com_tp=="03"):
    in_sql = """
           SELECT :v_dm_com_tp        AS DM_COM_TP
             , PT.PRD_NO              AS PRD_NO
             , PT.PARENT_PRD_NO       AS PARENT_PRD_NO
             , PT.MAIN_PRD_NO         AS MAIN_PRD_NO
             , PT.SHOP_NO             AS SHOP_NO
             , TO_CHAR(PT.REG_DTS,'YYYYMMDDHH24MISS')  AS REG_DTS
             , TO_CHAR(PT.MOD_DTS,'YYYYMMDDHH24MISS')  AS MOD_DTS
          FROM (
                SELECT DISTINCT PARENT_PRD_NO
                  FROM (
                         SELECT /*+USE_NL(M D)
                                   INDEX(M IDX_ORDERCLM_02) 
                                   INDEX(D ORDERCLMDTL_PK) */
                                D.PARENT_PRD_NO
                           FROM ORDERCLM    M
                              , ORDERCLMDTL D
                          WHERE D.ORDCLM_NO = M.ORDCLM_NO
                            AND M.ORDCLM_TP IN ('1')
                            AND M.ORDCLM_DTS BETWEEN TO_DATE(:p_etl_start_date,'YYYYMMDDHH24MISS') AND TO_DATE(:p_etl_end_date,'YYYYMMDDHH24MISS')
                         UNION ALL
                         SELECT P.PARENT_PRD_NO
                           FROM PRODUCT P
                          WHERE P.MOD_DTS BETWEEN TO_DATE(:p_etl_start_date,'YYYYMMDDHH24MISS') AND TO_DATE(:p_etl_end_date,'YYYYMMDDHH24MISS')
                            AND P.PARENT_PRD_NO = P.PRD_NO
                            AND EXISTS (SELECT 1  FROM ORDERCLMDTL D
                                                 WHERE D.PARENT_PRD_NO = P.PARENT_PRD_NO
                                                   AND ROWNUM =1 )
                       )
               ) Z
             , PRODUCT         PT
             , PRODUCT_DETAIL  PD
         WHERE PT.PRD_NO = Z.PARENT_PRD_NO
           AND PD.PRD_NO = PT.PRD_NO
           """
else:
    printf('[Error]v_dm_com_tp is wrong(2).\n')
    outcursor.close()
    outconn.close()
    exit(1)

try:
  #incursor.execute (in_sql)
  #incursor.execute(in_sql, (p_etl_start_date, p_etl_end_date))
  #incursor.execute(in_sql, (v_dm_com_tp, p_etl_start_date, p_etl_end_date))
  printf("## IN_SQL ##\n");

except cx_Oracle.DatabaseError, exception:
  printf ('Failed to select from TABLE\n')
  outcursor.close()
  outconn.close()
  incursor.close()
  inconn.close()
  printException (exception)
  exit (1)

########## JOB_LOG Procedure(END)  ##########
#job_log(v_stat_cd_ok)
printf("## JOB LOG ##\n");

incursor.close ()
inconn.close ()

outcursor.close ()
outconn.commit()
outconn.close ()

exit (0)
