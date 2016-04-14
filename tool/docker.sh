docker build -t mongodb_aws .

docker run -d -p 8000:8000 \
	-e "ETCDENDPOINT=http://192.168.99.100:2379"  \
	-e "BROKERPORT=8000"	 \
	-e "MONGOURL=54.222.155.67:27017"	 \
	-e "MONGOADMINUSER=asiainfoLDP"  \
	-e "MONGOADMINPASSWORD=6ED9BA74-75FD-4D1B-8916-842CB936AC1A"  \
	-e "AWS_ACCESS_KEY_ID=XXX"
	-e "AWS_SECRET_ACCESS_KEY=XXX"
	--name mongodb_aws mongodb_aws


docker run --name postgres -e POSTGRES_PASSWORD=C1BFACD6-E500-4257-B1BA-E7D369999C0F -p 5432:5432 -e POSTGRES_USER=asiainfoLDP -d postgres


docker run mysql MYSQL_ROOT_PASSWORD=cec82702-0ab3-449e-a80f-f815ce28e0cf

docker run --name mysqladmin -d --link mysql:db -p 8080:80 phpmyadmin/phpmyadmin

docker run --name phppgadmin -d --link postgres:postgresql -p 9090:80 maxexcloo/phppgadmin