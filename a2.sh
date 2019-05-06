#!/bin/sh
FILELIST=$1
LOGFILE="`date +%F`"RUN.log
#LOCAL_DIR=/u03/project/XSCM/linda
LOCAL_DIR=/u03/project/XSCM/linda
HOST_INFO=/u03/project/XSCM/linda/hostinfo

# Datastage Server相關資料
PROJECT=XSCM
JOBNAME=xscm_job

# DataStage Env
. `cat /.dshome`/dsenv > /dev/null 2>&1

# DS Functions
JobStatus()
{
status=`$DSHOME/bin/dsjob -jobinfo $1 $2 2>&1 | grep "Job Status" | sed "s/.*(\(.*\))/\1/"`
echo $status
}

ResetJob()
{
echo "JOBNAME="$JOBNAME >> $LOGFILE
job_status=`JobStatus $PROJECT $JOBNAME`

if [ $job_status -eq 3 ]
then
    echo "Resetting Job" >> $LOGFILE
    $DSHOME/bin/dsjob -run -mode RESET $PROJECT $JOBNAME > /dev/null 2>&1
    job_status=$?
    if [ ! $job_status -eq 0 ]
    then
        echo "FATAL ERROR: Reset Job failed" >> $LOGFILE
    fi
fi
}

RunJob()
{
$DSHOME/bin/dsjob -run -warn 0 -mode NORMAL -jobstatus $PROJECT $JOBNAME > /dev/null 2>&1
job_status=$?

if [ $job_status -ne 1 -a $job_status -ne 2 ]
then
    echo "Job Failed" >> $LOGFILE
else
    echo "Job Successful" >> $LOGFILE
fi
}

CheckFile()
{
S_FILE=$1
S_DIR=$2
S_HOST=$3
CHK_TYPE=$4
echo $1 >&2
echo $2 >&2
echo $3 >&2
echo $4 >&2
CHK_FILE=$S_DIR/$S_FILE
echo CHKFILE=$CHK_FILE >&2


if [[ $CHK_TYPE == "all_dir" ]] 
then
  CHK_FILE=$S_DIR/complete.txt
  CHKFTP $CHK_FILE $S_HOST >&2
elif [[ $CHK_TYPE == "ctl_file" ]]
then
  CHK_FILE=$S_DIR/../ctl/$S_FILE.ctl
  echo chkfile change to $CHK_FILE >&2
  CHKFTP $CHK_FILE $S_HOST >&2
elif [[ $CHK_TYPE == "chg_name" ]]
then
  GETFILELIST $S_DIR $S_HOST >&2
  FNDTGT $S_DIR $S_HOST $CHK_TYPE >&2
elif [[ $CHK_TYPE == "move_all" ]]
then
  GETFILELIST $S_DIR $S_HOST >&2
  FNDTGT $S_DIR $S_HOST $CHK_TYPE >&2
else
   echo "`date`  Parameter error!" >&2
   echo 1
fi

# TEST Flag Result
# If the flag file size is GT 0, means the file is arrived
#
FILESIZE=`stat -c%s $LOCAL_DIR/flag`
echo FILESIZE=$FILESIZE >&2

if [[ $FILESIZE -eq 0 ]]
then
   echo 1
else
   if [[ $CHK_TYPE == "all_dir" ]] 
   then
      echo GETLIST >&2
      GETFILELIST $S_DIR $S_HOST
   fi 
   echo 0
fi
}

CHKFTP()
{
CHK_FILE=$1
S_HOST=$2

## Get Host Info
INFO=`cat $HOST_INFO/$S_HOST`
H_ACT=$(echo $INFO | awk '{print $1}')
H_PWD=$(echo $INFO | awk '{print $2}')
echo HOST act=$H_ACT , HOST pwd=$H_PWD >&2

rm $LOCAL_DIR/flag
touch $LOCAL_DIR/flag


ftp -i -n -v $S_HOST <<EOF > /dev/null 2>&1
quote USER $H_ACT
quote PASS $H_PWD
prompt
ls $CHK_FILE $LOCAL_DIR/flag 
quit
EOF
}

GETFILELIST()
{
S_DIR=$1
S_HOST=$2

## Get Host Info
INFO=`cat $HOST_INFO/$S_HOST`
H_ACT=$(echo $INFO | awk '{print $1}')
H_PWD=$(echo $INFO | awk '{print $2}')
echo HOST act=$H_ACT , HOST pwd=$H_PWD >&2

rm $LOCAL_DIR/flag
touch $LOCAL_DIR/flag

ftp -i -n $S_HOST <<EOF > /dev/null 2>&1
quote USER $H_ACT
quote PASS $H_PWD
prompt
ls $S_DIR $LOCAL_DIR/flag 
quit
EOF
}

FNDTGT()
{
S_DIR=$1
S_HOST=$2
CHK_TYPE=$3

grep -v "<DIR>" $LOCAL_DIR/flag|grep -v prs|grep -v fin|grep -v stg > renamelist 

# Get Host Info

INFO=`cat $HOST_INFO/$S_HOST`
H_ACT=$(echo $INFO | awk '{print $1}')
H_PWD=$(echo $INFO | awk '{print $2}')
echo HOST act=$H_ACT , HOST pwd=$H_PWD >&2


#Check file is avaliable for rename (check file if complete
while IFS='' read -r list
do
   ftp -i -n  $S_HOST  <<EOF > ftp.log
   quote USER ditest
   quote PASS ditest
   rename $S_DIR/$F_NAME $S_DIR/stg$F_NAME
   quit
EOF

   rec="250 RNTO command successful"
   if fgrep -e "$rec" ftp.log
   then
      if [[ $CHK_TYPE == "chg_name" ]]
      then
         echo "stg$F_NAME" >> $LOCAL_DIR/flag
         echo "FILENAME=stg$F_NAME" >&2
      elif [[ $CHK_TYPE == "move_all" ]]
      then
         echo "$F_NAME" >> $LOCAL_DIR/flag
         echo "FILENAME=$F_NAME" >&2
      fi
   fi
done < renamelist
}

###############################
#      Shell Start Here       #
###############################

echo start
while read -r line
do
   echo line=$line

   S_FILE=$(echo $line | awk '{print $1}')
   S_DIR=$(echo $line | awk '{print $2}')
   S_HOST=$(echo $line | awk '{print $3}')
   S_CHK=$(echo $line | awk '{print $4}')

   T_FILE=$(echo $line | awk '{print $5}')
   T_DIR=$(echo $line | awk '{print $6}')
   T_HOST=$(echo $line | awk '{print $7}')
   T_TYPE=$(echo $line | awk '{print $8}')

   status=`CheckFile $S_FILE $S_DIR $S_HOST $S_CHK`
   echo status=$status >&2

   ##Prepare Running List
   if [[ $status -eq 0 ]]
   then
        echo "Running List: chktype=$S_CHK" >&2 
	if [[ $S_CHK == "all_dir" ]] 
	then
      	  grep -v "<DIR>" $LOCAL_DIR/flag > namelist 
      	 
          while IFS='' read -r list 
          do
	     F_NAME=$(echo $list |awk '{print $4}') 
             echo "$F_NAME  $S_DIR  $S_HOST  $F_NAME  $T_DIR  $T_HOST  $T_TYPE  " >> runlist 
      	  done < namelist
	elif [[ $S_CHK == "chg_name" || $S_CHK == "move_all" ]] 
	then
          while IFS='' read -r list
          do
             F_NAME=$(echo $list |awk '{print $1}')
             echo "$F_NAME  $S_DIR  $S_HOST  $F_NAME  $T_DIR  $T_HOST  $T_TYPE  " >> runlist
          done < $LOCAL_DIR/flag
        else
       	  echo "$S_FILE  $S_DIR  $S_HOST  $T_FILE  $T_DIR  $T_HOST  $T_TYPE  " >> runlist 
        fi
   else
      echo "Not RunJob $S_FILE"
   fi
done < $FILELIST    

#Passing List to DSJob
#ResetJob
#RunJob
