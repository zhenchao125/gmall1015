#!/bin/bash
es_home=/opt/module/elasticsearch-6.3.1
kibana_home=/opt/module/kibana-6.3.1
case $1 in
start)
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在 $host 启动 es"
        ssh $host "source /etc/profile ; nohup $es_home/bin/elasticsearch 1>$es_home/es.log 2>$es_home/error.log &"
    done

    echo "在 hadoop102 启动 kibana "
    nohup $kibana_home/bin/kibana 1>$kibana_home/k.log 2>error.log &

;;

stop)
    echo "在 hadoop102 停止 kibana "
    ps -ef | grep kibana | grep -v grep | awk '{print $2}' | xargs kill -9

    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在 $host 停止 es"
        ssh $host "source /etc/profile ; jps | grep Elasticsearch | awk '{print \$1}' | xargs kill -9"
    done
;;

*)
    echo "脚本使用方法: "
    echo "    start 启动es和kibana "
    echo "    stop  停止es和kibana "

;;
esac