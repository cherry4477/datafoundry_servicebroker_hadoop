package handler

import (
	"fmt"
	hdfs "github.com/vladimirvivien/gowfs"
	"github.com/mqu/openldap"
	"github.com/pivotal-cf/brokerapi"
	"github.com/xmwilldo/ranger"
	"os"
	"strings"
	"time"
)

var (
	hdfsUrl  string
	hdfsUser string

	ldapUrl      string
	ldapUser     string
	ldapPassword string

	rangerEndpoint string
	rangerUser     string
	rangerPassword string
	policyRepoType string
	policyRepoName string
)

type Hdfs_sharedHandler struct{}

func (handler *Hdfs_sharedHandler) DoProvision(instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, ServiceInfo, error) {
	fmt.Println("DoProvision......")
	config := newHdfsConfig()

	fs, err := hdfs.NewFileSystem(*config)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	dname := instanceID

	_, err = createDirectory(fs, dname, 0700)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Printf("Create directory /servicebroker/%s done......\n", dname)

	newAccount := "wm_" + getRandom()
	policyName := getRandom()

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		rollbackDeleteDirectory(fs, dname)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		rollbackDeleteDirectory(fs, dname)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	err = addAccount(ldap, newAccount, "broker")
	if err != nil {
		rollbackDeleteDirectory(fs, dname)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Printf("Create account %s done......\n", newAccount)

	info := newHdfsPolicyInfo(policyRepoType, policyRepoName, policyName, "/servicebroker/"+dname)

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, newAccount)
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm, "read", "write", "execute")
	ranger.AddPermissionToPolicy(&info, perm)

	var policyId int
	for i := 0; i < 10; i++ {
		fmt.Println("try create policy......")
		policyId, err = ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, info)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}

	if err != nil {
		rollbackDeleteAccount(newAccount)
		rollbackDeleteDirectory(fs, dname)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	fmt.Printf("Create policy %s done......\n", policyName)

	//policyIdStr := strconv.Itoa(policyId)

	DashboardURL := "http://"

	myServiceInfo := ServiceInfo{
		Url:        hdfsUrl,
		Admin_user: "ocdp",
		//Admin_password: "", //把创建好的policy的id通过这个参数传递
		Database:   dname,
		User:       newAccount,
		Password:   "",
		PolicyInfo: info,
		Policy_id:  policyId,
	}

	provisiondetail := brokerapi.ProvisionedServiceSpec{DashboardURL: DashboardURL, IsAsync: false}

	return provisiondetail, myServiceInfo, nil
}

func (handler *Hdfs_sharedHandler) DoLastOperation(myServiceInfo *ServiceInfo) (brokerapi.LastOperation, error) {
	fmt.Println("DoLastOperation......")

	return brokerapi.LastOperation{
		State:       brokerapi.Succeeded,
		Description: "It's a sync method!",
	}, nil
}

func (handler *Hdfs_sharedHandler) DoDeprovision(myServiceInfo *ServiceInfo, asyncAllowed bool) (brokerapi.IsAsync, error) {
	fmt.Println("DoDeprovision......")

	config := newHdfsConfig()
	fs, err := hdfs.NewFileSystem(*config)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	_, err = deleteDirectory(fs, myServiceInfo.Database, true)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("Delete directory /servicebroker/%s done......\n", myServiceInfo.Database)

	//policyId, err := strconv.Atoi(myServiceInfo.Admin_password)
	//if err != nil {
	//	return brokerapi.IsAsync(false), err
	//}

	_, err = ranger.DeletePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("Delete policy %s done......\n", myServiceInfo.PolicyInfo.PolicyName)

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo)
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo)
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}

	err = deleteAccount(ldap, myServiceInfo.User)
	if err != nil {
		rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo)
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("Delete account %s done......\n", myServiceInfo.User)

	return brokerapi.IsAsync(false), nil
}

func (handler *Hdfs_sharedHandler) DoBind(myServiceInfo *ServiceInfo, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, Credentials, error) {
	fmt.Println("DoBind......")
	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}
	newAccount := "wm_" + getRandom()
	err = addAccount(ldap, newAccount, "broker")
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}
	myServiceInfo.Bind_user = newAccount
	fmt.Printf("Create account %s done......\n", newAccount)

	//policyName := getRandom()

	//info := myServiceInfo.PolicyInfo

	ranger.AddUserToPermission(&myServiceInfo.PolicyInfo.PermMapList[0], newAccount)
	//myServiceInfo.PolicyInfo = info

	//perm := ranger.InitPermission()
	//ranger.AddUserToPermission(&perm, newAccount)
	//ranger.AddGroupToPermission(&perm, "broker")
	//ranger.AddPermToPermission(&perm, "read", "write", "execute")
	//ranger.AddPermissionToPolicy(&info, perm)

	//policyId, err := strconv.Atoi(myServiceInfo.Admin_password)
	//if err != nil {
	//	return brokerapi.Binding{}, Credentials{}, err
	//}

	for i := 0; i < 10; i++ {
		fmt.Println("try update policy......")
		_, err = ranger.UpdatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.PolicyInfo, myServiceInfo.Policy_id)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}

	if err != nil {
		rollbackDeleteAccount(newAccount)
		return brokerapi.Binding{}, Credentials{}, err
	}

	fmt.Printf("Update policy %s done......\n", myServiceInfo.PolicyInfo.PolicyName)

	//policyIdStr := strconv.Itoa(policyId)

	mycredentials := Credentials{
		Uri:      "",
		Hostname: strings.Split(myServiceInfo.Url, ":")[0],
		Port:     strings.Split(myServiceInfo.Url, ":")[1],
		Username: newAccount,
		Password: "", //通过这个参数来传递policyId
		Name:     myServiceInfo.Database,
		//Policy_id: myServiceInfo.Policy_id,
	}

	myBinding := brokerapi.Binding{Credentials: mycredentials}

	return myBinding, mycredentials, nil
}

func (handler *Hdfs_sharedHandler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {
	fmt.Println("DoUnbind......")

	//policyId, err := strconv.Atoi(myServiceInfo.Admin_password)
	//if err != nil {
	//	return err
	//}

	info := myServiceInfo.PolicyInfo
	userList := info.PermMapList[0].UserList
	for k, v := range userList {
		if v == myServiceInfo.Bind_user {
			kk := k + 1
			userList = append(userList[:k], userList[kk:]...)
		}
	}

	_, err := ranger.UpdatePolicy(rangerEndpoint, rangerUser, rangerPassword, info, myServiceInfo.Policy_id)
	if err != nil {
		return err
	}
	fmt.Printf("Delete policy %s done......\n", myServiceInfo.PolicyInfo.PolicyName)

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo)
		return err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo)
		return err
	}

	err = deleteAccount(ldap, myServiceInfo.User)
	if err != nil {
		rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo)
		return err
	}
	fmt.Printf("Delete account %s done......\n", myServiceInfo.User)

	return nil
}

func init() {
	register("hdfs_shared", &Hdfs_sharedHandler{})
	hdfsUrl = getenv("HDFSURL")
	hdfsUser = getenv("HDFSUSER")
	ldapUrl = getenv("LDAPURL")
	ldapUser = getenv("LDAPUSER")
	ldapPassword = getenv("LDAPPASSWORD")
	rangerEndpoint = getenv("RANGERENDPOINT")
	rangerUser = getenv("RANGERUSER")
	rangerPassword = getenv("RANGERPASSWORD")
	policyRepoType = getenv("POLICYREPOTYPE")
	policyRepoName = getenv("POLICYREPONAME")
}

func newHdfsConfig() *hdfs.Configuration {
	config := hdfs.NewConfiguration()
	config.Addr = hdfsUrl
	config.User = hdfsUser
	config.BasePath = "/servicebroker"
	config.MaxIdleConnsPerHost = 64

	return config
}

func createDirectory(fs *hdfs.FileSystem, name string, fileMode os.FileMode) (bool, error) {
	path := hdfs.Path{}
	path.Name = name

	isCreated, err := fs.MkDirs(path, fileMode)
	if err != nil {
		return isCreated, err
	}

	return isCreated, nil
}

func deleteAccount(ldap *openldap.Ldap, user string) error {
	err := ldap.Delete("uid=" + user + ",ou=People,dc=asiainfo,dc=com")
	if err != nil {
		return err
	}
	return nil
}

func deleteDirectory(fs *hdfs.FileSystem, name string, recursive bool) (bool, error) {
	path := hdfs.Path{}
	path.Name = name

	isDelete, err := fs.Delete(path, recursive)
	if err != nil {
		return isDelete, err
	}
	return isDelete, nil
}

func addAccount(ldap *openldap.Ldap, user, group string) error {
	attrs := make(map[string][]string)
	obj := make([]string, 0)
	uid := make([]string, 0)
	groupby := make([]string, 0)

	obj = append(obj, "account")
	uid = append(uid, user)
	groupby = append(groupby, "cn="+group+",ou=Group,dc=asiainfo,dc=com")

	attrs["objectclass"] = obj
	attrs["uid"] = uid
	attrs["memberOf"] = groupby

	err := ldap.Add("uid="+user+",ou=People,dc=asiainfo,dc=com", attrs)
	if err != nil {
		return err
	}

	return nil
}

func newHdfsPolicyInfo(repoType, repoName, policyName, resourceName string) ranger.HdfsPolicyInfo {

	info := ranger.NewHdfsPolicyInfo()
	info.RepositoryType = repoType
	info.RepositoryName = repoName
	info.PolicyName = policyName
	info.ResourceName = resourceName

	return info
}

func rollbackDeleteDirectory(fs *hdfs.FileSystem, dname string) {
	fmt.Println("Error occurred ! Rollback delete directory......")
	var err error
	for i := 0; i < 10; i++ {
		_, err = deleteDirectory(fs, dname, true)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}
	if err == nil {
		fmt.Printf("Rollback delete directory /servicebroker/%s done......\n", dname)
	} else {
		fmt.Println("Rollback failed......")
	}
}

func rollbackCreateDirectory(fs *hdfs.FileSystem, dname string) {
	fmt.Println("Error occurred ! Rollback create directory......")
	var err error
	for i := 0; i < 10; i++ {
		_, err = createDirectory(fs, dname, 0700)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}
	if err == nil {
		fmt.Printf("Rollback create directory /servicebroker/%s done......\n", dname)
	} else {
		fmt.Println("Rollback failed......")
	}
}

func rollbackDeleteAccount(user string) {
	fmt.Println("Error occurred ! Rollback delete account......")
	var err error
	for i := 0; i < 10; i++ {
		ldap, err := openldap.Initialize(ldapUrl)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
		err = ldap.Bind(ldapUser, ldapPassword)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
		err = deleteAccount(ldap, user)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}
	if err == nil {
		fmt.Printf("Rollback delete account /%s done......\n", user)
	} else {
		fmt.Println("Rollback failed......")
	}
}

func rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword string, myServiceInfo *ServiceInfo) {
	fmt.Println("Error occurred ! Rollback create policy......")
	var err error
	for i := 0; i < 10; i++ {
		fmt.Println("try create policy......")
		policyId, err := ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.PolicyInfo)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			myServiceInfo.Policy_id = policyId
			break
		}
	}
	if err == nil {
		fmt.Printf("Rollback create policy /%s done......\n", myServiceInfo.PolicyInfo.PolicyName)
	} else {
		fmt.Println("Rollback failed......")
	}
}
