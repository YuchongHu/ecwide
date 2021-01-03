#!/bin/sh

ps -ef|grep "memcached -d -m 100 -u root -l 127.0.0.1"|cut -c 9-15|xargs kill -9

ps -ef|grep "./requestor"|cut -c 9-15|xargs kill -9

#for task
memcached -d -m 100 -u root -l 127.0.0.1 -p 21000
memcached -d -m 100 -u root -l 127.0.0.1 -p 21001
memcached -d -m 100 -u root -l 127.0.0.1 -p 21002
memcached -d -m 100 -u root -l 127.0.0.1 -p 21003
memcached -d -m 100 -u root -l 127.0.0.1 -p 21004
memcached -d -m 100 -u root -l 127.0.0.1 -p 21005
memcached -d -m 100 -u root -l 127.0.0.1 -p 21006
memcached -d -m 100 -u root -l 127.0.0.1 -p 21007
memcached -d -m 100 -u root -l 127.0.0.1 -p 21008
memcached -d -m 100 -u root -l 127.0.0.1 -p 21009
memcached -d -m 100 -u root -l 127.0.0.1 -p 21010