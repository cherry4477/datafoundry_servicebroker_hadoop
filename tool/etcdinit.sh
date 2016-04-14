#运行etcd
#HostIP=`docker-machine ip default`
export ETCDCTL_ENDPOINT=http://etcdsystem.servicebroker.dataos.io:2379
export ETCDCTL_USERNAME=asiainfoLDP:6ED9BA74-75FD-4D1B-8916-842CB936AC1A


#建立初始化数据
##!!注意，初始化的时候，所有键值必须小写，这样程序才认识
##初始化用户名和密码部分
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker/zookeeper
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/username asiainfoLDP
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/password 2016asia

##初始化catalog
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker/zookeeper/catalog

###创建服务
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D #服务id

###创建服务级的配置
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/name "Zookeeper"
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/description "A experimental zookeeper"
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/bindable true
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/planupdatable false
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/tags 'zookeeper'
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/metadata '{"displayName":"Zookeeper","imageUrl":"https://d33na3ni6eqf5j.cloudfront.net/app_resources/18492/thumbs_112/img9069612145282015279.png","longDescription":"ZooKeeper is a centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services","providerDisplayName":"ocdp","documentationUrl":"https://zookeeper.apache.org/doc/trunk/","supportUrl":"https://cwiki.apache.org/confluence/display/ZOOKEEPER/FAQ"}'

###创建套餐目录
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/plan
###创建套餐1
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/plan/F39AC4BF-C237-484F-AC9D-FB0A80223F85
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/plan/F39AC4BF-C237-484F-AC9D-FB0A80223F85/name "Experimental"
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/plan/F39AC4BF-C237-484F-AC9D-FB0A80223F85/description "share a zookeeper instance"
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/plan/F39AC4BF-C237-484F-AC9D-FB0A80223F85/metadata '{"bullets":["1 MB of Disk","10 connections"],"displayName":"Experimental and Free" }'
etcdctl -u $ETCDCTL_USERNAME set /servicebroker/zookeeper/catalog/B35EF534-595F-4252-B6FE-EA4F347BCE3D/plan/F39AC4BF-C237-484F-AC9D-FB0A80223F85/free true



##初始化instance
etcdctl -u $ETCDCTL_USERNAME mkdir /servicebroker/zookeeper/instance

