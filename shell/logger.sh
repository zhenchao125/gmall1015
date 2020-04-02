#!/bin/bash
log_home=/opt/gmall1015
case $1 in
start)
  for host in hadoop102 hadoop103 hadoop104 ; do
      echo "====== $host 启动日志服务器 ======"
      ssh $host "source /etc/profile ; nohup java -jar $log_home/gmall-logger-0.0.1-SNAPSHOT.jar  1>$log_home/server.log 2>$log_home/error.log & "
  done
  ;;
stop)
  for host in hadoop102 hadoop103 hadoop104 ; do
      echo "====== $host 停止日志服务器 ======"
      ssh $host "source /etc/profile ; ps -ef | grep gmall-logger | grep -v grep | awk '{print \$2}' | xargs kill -9"
  done
  ;;
*)
  echo "脚本使用方法: "
  echo "    start 启动日志服务器 "
  echo "    stop  停止日志服务器 "

esac