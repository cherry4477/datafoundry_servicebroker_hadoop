package handler

import (
	"fmt"
	"github.com/pivotal-cf/brokerapi"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

var zookeeperUrl string
var zookeeperAdminUser string
var zookeeperAdminPassword string
var aclScheme string = "digest"
var gsbrootpath string = "/servicebroker"

type zookeeperHandler struct{}

func (handler *zookeeperHandler) DoProvision(instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, ServiceInfo, error) {
	//初始化zookeeper的链接串
	servers := strings.Split(zookeeperUrl, ",")
	sessionTimeout := time.Second * 2

	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	defer conn.Close()

	adminAuth := zookeeperAdminUser + ":" + zookeeperAdminPassword
	conn.AddAuth(aclScheme, []byte(adminAuth))

	newusername := getguid()
	newpassword := getguid()
	path := gsbrootpath + "/" + instanceID
	flags := int32(0)

	existsbroot, _, err := conn.Exists(gsbrootpath)
	if err != nil {
		fmt.Println("get", gsbrootpath, err, existsbroot)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	if existsbroot == false {
		// anyone can operate the node /servicebroker
		aclsb := zk.WorldACL(zk.PermAll)
		_, err = conn.Create(gsbrootpath, []byte(instanceID), flags, aclsb)
		if err != nil {
			fmt.Println("create", gsbrootpath, err)
			return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
		}
	}

	aclnew := zk.DigestACL(zk.PermAll, newusername, newpassword)
	//创建一个名为instanceID的path，并随机的创建用户名和密码，这个用户名是该path的管理员
	_, err = conn.Create(path, []byte(instanceID), flags, aclnew)
	if err != nil {
		fmt.Println("create:", path, err)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	//为dashbord赋值 todo dashboard应该提供一个界面才对
	DashboardURL := newusername + ":" + newpassword + "@" + strings.Split(zookeeperUrl, ":")[0] + " instance=" + instanceID
	fmt.Println("DashboardURL:", DashboardURL)

	//赋值隐藏属性
	myServiceInfo := ServiceInfo{
		Service_name:   "Zookeeper",
		Url:            zookeeperUrl,
		Admin_user:     zookeeperAdminUser,
		Admin_password: zookeeperAdminPassword,
		Database:       path,
		User:           newusername,
		Password:       newpassword,
	}

	provsiondetail := brokerapi.ProvisionedServiceSpec{DashboardURL: DashboardURL, IsAsync: false}

	return provsiondetail, myServiceInfo, nil
}

func (handler *zookeeperHandler) DoLastOperation(myServiceInfo *ServiceInfo) (brokerapi.LastOperation, error) {
	//因为是同步模式，协议里面并没有说怎么处理啊，统一反馈成功吧！
	return brokerapi.LastOperation{
		State:       brokerapi.Succeeded,
		Description: "It's a sync method!",
	}, nil
}

func (handler *zookeeperHandler) DoDeprovision(myServiceInfo *ServiceInfo, asyncAllowed bool) (brokerapi.IsAsync, error) {
	//初始化mongodb的链接串
	zkUrl := myServiceInfo.Url
	servers := strings.Split(zkUrl, ",")
	sessionTimeout := time.Second * 2

	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}
	defer conn.Close()

	adminAuth := myServiceInfo.User + ":" + myServiceInfo.Password
	err = conn.AddAuth(aclScheme, []byte(adminAuth))
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	path := myServiceInfo.Database

	err = conn.Delete(path, 0) //todo version?
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	//非异步，无错误的返回
	return brokerapi.IsAsync(false), nil
}

func (handler *zookeeperHandler) DoBind(myServiceInfo *ServiceInfo, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, Credentials, error) {
	//初始化mongodb的两个变量
	//mongodburl := myServiceInfo.Url
	zkUrl := myServiceInfo.Url
	//share 模式只能是该数据库
	path := myServiceInfo.Database
	//share 模式，只是这个数据库的读写
	var zkrole int32 = zk.PermAll
	//完成变量赋值以后，开始准备创建用户
	//初始化mongodb的链接串
	servers := strings.Split(zkUrl, ",")
	sessionTimeout := time.Second * 2
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}
	defer conn.Close()

	auth := myServiceInfo.User + ":" + myServiceInfo.Password
	err = conn.AddAuth(aclScheme, []byte(auth))
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}

	acls := []zk.ACL{}
	oldacl, statget, err := conn.GetACL(path)
	if err != nil {
		fmt.Println(err)
		return brokerapi.Binding{}, Credentials{}, err
	}
	fmt.Println("GetACL:", oldacl, statget)
	acls = append(acls, oldacl...)

	//去创建一个用户，权限为RoleReadWrite
	newusername := getguid()
	newpassword := getguid()
	newacl := zk.DigestACL(zkrole, newusername, newpassword)

	acls = append(acls, newacl...)
	fmt.Println("acls:", acls)

	statset, err := conn.SetACL(path, acls, statget.Aversion)
	if err != nil { //version)
		return brokerapi.Binding{}, Credentials{}, err
	}
	fmt.Println(*statset)

	mycredentials := Credentials{
		Uri:      "zookeeper://" + newusername + ":" + newpassword + "@" + zkUrl + path,
		Hostname: strings.Split(zkUrl, ":")[0],
		Port:     strings.Split(zkUrl, ":")[1],
		Username: newusername,
		Password: newpassword,
		Name:     path,
	}

	myBinding := brokerapi.Binding{Credentials: mycredentials}

	return myBinding, mycredentials, nil
}

func (handler *zookeeperHandler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {
	//初始化mongodb的两个变量
	//mongodburl := myServiceInfo.Url
	zkUrl := zookeeperUrl
	path := myServiceInfo.Database
	//初始化mongodb的链接串
	servers := strings.Split(zkUrl, ",")
	sessionTimeout := time.Second * 2
	conn, _, err := zk.Connect(servers, sessionTimeout) //连接数据库
	if err != nil {
		return err
	}
	defer conn.Close()

	auth := myServiceInfo.User + ":" + myServiceInfo.Password
	err = conn.AddAuth(aclScheme, []byte(auth))
	if err != nil {
		return err
	}

	acls := []zk.ACL{}
	oldacl, statget, err := conn.GetACL(path)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("Unbind GetACL:", oldacl, statget)

	for _, v := range oldacl {
		sp := strings.Split(v.ID, ":")
		//ACL ID format:   user:base64(sha1(user:passwd))
		if sp[0] == mycredentials.Username {
			continue
		}
		acls = append(acls, v)
	}

	statset, err := conn.SetACL(path, acls, statget.Aversion)
	if err != nil { //version)
		return err
	}
	fmt.Println(statset)

	return nil
}

func init() {
	register("Zookeeper_Experimental", &zookeeperHandler{})
	zookeeperUrl = getenv("ZOOKEEPERURL")                     //共享实例的mongodb地址
	zookeeperAdminUser = getenv("ZOOKEEPERADMINUSER")         //共享实例和独立实例的管理员用户名
	zookeeperAdminPassword = getenv("ZOOKEEPERADMINPASSWORD") //共享实例和独立实例的管理员密码
}
