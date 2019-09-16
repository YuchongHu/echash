#!/bin/sh

ps -ef|grep "memcached -d"|cut -c 9-15|xargs kill -9

cat config.txt |grep -Ev "^$|[#;]"| while read line
do
  	ip=${line%% *}
	tmp=${line#* }
	port=${tmp%% *}
	id=${tmp##* }
	memcached -d -m 60 -u root -l $ip -p $port
done

