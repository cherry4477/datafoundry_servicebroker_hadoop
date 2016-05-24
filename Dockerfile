FROM golang:1.5.2

ENV BROKERPORT 8000
EXPOSE 8000

ENV TIME_ZONE=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TIME_ZONE /etc/localtime && echo $TIME_ZONE > /etc/timezone

#ENV GOPATH=/xxxxx/
COPY . /usr/local/go/src/github.com/asiainfoLDP/datafoundry_servicebroker_hadoop

WORKDIR /usr/local/go/src/github.com/asiainfoLDP/datafoundry_servicebroker_hadoop

ADD ./config/krb5.conf /etc/

RUN apt-get update  

RUN apt-get install -y --no-install-recommends  krb5-user krb5-config





ADD ./config/hosts /etc/hosts

COPY ./config/start.sh /start.sh

COPY ./libldap2-dev_2.4.31-1+nmu2ubuntu8.2_amd64.deb /var/lib/dpkg/info/

COPY ./libldap2-dev:amd64.md5sums /var/lib/dpkg/info/

RUN go get github.com/tools/godep \
    && godep go build 

CMD ["/start.sh"]