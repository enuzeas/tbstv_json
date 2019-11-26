#! /bin/bash

sudo pm2 start --name jsonserver npm -- start 
sleep 3
sudo /home/enuzeas/vpnclient/vpnclient/vpnclient start 
sleep 3
sudo dhclient vpn_se
sleep 3
sudo ip route add 192.168.20.0/24 via 192.168.88.1
sudo ip route add 1.1.1.0/24 via 192.168.88.1
cd  /home/enuzeas/schedule-py/json

sudo pm2 start http-server -a

cd /opt/splunkforwarder/bin
sudo ./splunk start
