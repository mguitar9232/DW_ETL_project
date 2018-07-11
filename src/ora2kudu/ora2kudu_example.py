#-*- coding=utf-8 -*-
################################################################################
################################################################################

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from datetime import datetime
import datetime

from datetime import date
from time import strftime, localtime


import cx_Oracle

sys.path.insert(0, '../icrypto')
import icrypto

import os
os.environ["NLS_LANG"] = ".KO16MSWIN949"

############### KUDU import
import kudu
from kudu.client import Partitioning

table_name ='TEST0000'
###########################

os.putenv('NLS_LANG','AMERICAN_AMERICA.KO16MSWIN949')

def printf (format,*args):
  sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);

def job_log(v_stat_cd):
  try:
    pparam = (v_stat_cd, p_job_id, p_source_cnt, p_target_cnt, p_update_cnt, p_etl_start_data, p_etl_end_data, p_rework_cd, p_sql_code, p_message, p_out_msg)
    printf("## CURSOR CALL PROCEDURE ##\n");
  except cx_Oracle.DatabaseError, exception:
    printf('Failed to call JOB Procedure(s_stat_code=[%s])\n', v_stat_cd)
    printException(exception)
    exit(1)

#########################################
####  Common Variable Definition    #####
#########################################
# program exec info
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
outdatabaseName = "XXXX"

# procedure call variable
v_stat_cd_s  = '1'   #LOG 프로시져 시작 코드
v_stat_cd_ok = '2'   #LOG 프로시져 종료 코드
v_stat_cd_f  = '3'   #LOG 프로시져 오류 코드
p_job_cnt = 10
p_source_cnt = 0
p_target_cnt = 0
p_update_cnt = 0
p_etl_start_data = ''
p_etl_end_data = ''
p_rework_cd = ''
p_sql_code = ''
p_message = ''
p_out_msg = ''
p_regionnm ='0000'

cnt = 0

mem_no = ''

# User Variable Definition
ing_cnt = 0

###### GET CURRENT TIME
today = date.today()
current_time = strftime("%Y%m%d%H%M%S", localtime())

########## KUDU CONNECTION & TABLE CREATE ##############################################################

client = kudu.connect(host="192.168.0.1", port=7051)

# Define a schema for a new table
builder = kudu.schema_builder()

builder.add_column('DT').type(kudu.string).nullable(False)
builder.add_column('FILE_TYPE').type(kudu.string).nullable(False)
builder.add_column('SEQ').type(kudu.int64).nullable(False)
builder.add_column('V1').type(kudu.string).nullable(True)
builder.add_column('V2').type(kudu.string).nullable(True)
builder.add_column('V3').type(kudu.string).nullable(True)
builder.add_column('MOD_DTS', type_=kudu.unixtime_micros, nullable=False, compression='lz4')

builder.set_primary_keys(['DT','FILE_TYPE','SEQ'])

schema = builder.build()

# Define partitioning schema
partitioning = Partitioning().add_hash_partitions(column_names=['DT'], num_buckets=3)

exists = False
drop = False

if client.table_exists(table_name):
    print("TABLE EXIST")

    exists = True

    if drop:
      cre = client.delete_table(table_name)
      print(cre)
      exists = False

if not exists:
    # Create new table
    #client.create_table('python-example', schema, partitioning)
    client.create_table(table_name, schema, partitioning)

table = client.table(table_name)

# Create a new session so that we can apply write operations
session = client.new_session()

###################################################################################################


########## CONNECT TARGER(OUT) DB ##########
try:
  outconn = cx_Oracle.connect (outusername,outpassword,outdatabaseName)
  outcursor = outconn.cursor()
  printf ('CONNECT SUCCESS\n')
except cx_Oracle.DatabaseError, exception:
  printf ('Failed to connect(out) to %s\n',outdatabaseName)
  printException (exception)
  exit(1)

try:
  job_data_sql = """
SELECT DT ,
  FILE_TYPE,
  SEQ      ,
  NVL(V1, '-'),
  NVL(V2, '-'),
  NVL(V3, '-'),
  to_char(MOD_DTS, 'YYYY-MM-DD HH24:Mi:SS')
 FROM shop_moif_add_upload
WHERE dt  between to_char(sysdate-1,'YYYYMMDD') and to_char(sysdate,'YYYYMMDD') 
  and file_type in ('01','02')
"""

  printf('STEP 1=[%s]\n',   job_data_sql)

  outcursor.execute(job_data_sql)
  printf('STEP 2\n')
  job_data = outcursor.fetchall()
  print(outcursor.fetchall())

  for row in job_data:

    han = row[4].decode('cp949').encode('utf-8')
    han2 = row[5].decode('cp949').encode('utf-8')

    dt = datetime.datetime.strptime(row[33], "%Y-%m-%d %H:%M:%S")


    op = table.new_insert({'DT'       : row[0], 
    #op = table.new_upsert({ 'DT'       : row[0], 
                            'FILE_TYPE': row[1], 
                            'SEQ'      : row[2], 
                            'V1'       : row[3], 
                            'V2'       : han,
                            'V3'       : han2,
                            'MOD_DTS'  : dt })

    session.apply(op)

    printf("##[%d]\n",cnt)
    cnt +=1

    try:
      session.flush()
    except kudu.KuduBadStatus as e:
      print(session.get_pending_errors())
      printf('JOB_DATA      = [%s][%s][%s][%s][%s][%s][%s]\n',
	   row[0],row[1],row[2],row[3],han,han2,dt)


except cx_Oracle.DatabaseError, exception:
  printf ('Failed to select JOB_DATA \n')
  outcursor.close()
  outconn.close()
  printException (exception)
  exit (1)


printf("--[step1]ok~!! (%s) ---\n", current_time)
printf("---ing=END NUMBER=[%d] ..........       \n", cnt)

