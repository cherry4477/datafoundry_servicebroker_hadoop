FROM golang:1.6.2

ENV TIME_ZONE=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TIME_ZONE /etc/localtime && echo $TIME_ZONE > /etc/timezone

WORKDIR /usr/local/go/src/github.com/asiainfoLDP/datafoundry_servicebroker_hadoop

ADD ./config/krb5.conf /etc/

RUN apt-get update   && \
    apt-get install -y --no-install-recommends  krb5-user krb5-config libldap2-dev

ADD ./config/hosts /etc/hosts

ADD ./config/start.sh /start.sh

ADD . /usr/local/go/src/github.com/asiainfoLDP/datafoundry_servicebroker_hadoop

ENV GODEBUG=cgocheck=0

#RUN go get github.com/tools/godep \
#    && godep go build 

RUN go build

ENV BROKERPORT 8000
EXPOSE 8000

RUN chmod u+x /start.sh

CMD ["/start.sh"]
