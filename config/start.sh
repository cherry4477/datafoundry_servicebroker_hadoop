#!/usr/bin/env bash
echo "modify /etc/hosts"
cat ./config/hosts  >> /etc/hosts

echo "parametre1="$1

echo "kinit"
date
if [ "$1" = "jd" ];then
	cp ./config/krb5.conf.jd /etc/krb5.conf
	kinit -p ocdp-OCDPforLDP@ASIAINFO.COM -kt ./config/ocdp-go-broker.keytab
else
	kinit ocdp/h-4lcf6qdz@ASIAINFO.COM <<!!
	asiainfo
!!
fi


echo "start main"
date
./datafoundry_servicebroker_hadoop
