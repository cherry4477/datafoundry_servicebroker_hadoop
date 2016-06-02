#!/usr/bin/env bash
echo "modify /etc/hosts"
cat ./config/hosts  >> /etc/hosts

echo "kinit"
date
kinit ocdp/h-4lcf6qdz@ASIAINFO.COM <<!!
asiainfo
!!

echo "start main"
date
./datafoundry_servicebroker_hadoop
