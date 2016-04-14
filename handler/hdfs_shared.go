package handler

import (
	hdfs "github.com/gowfs"
	"github.com/mqu/openldap"
	"github.com/pivotal-cf/brokerapi"
	"github.com/xmwilldo/ranger"
	"os"
	"fmt"
	"time"
	"strconv"
	"strings"
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

	dname := getRandom()

	_, err = createDirectory(fs, dname, 0700)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Println("create directory done......")

	newusername := "xm"+getRandom()
	//newpassword := getRandom()
	policyName := getRandom()

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	err = addAccount(ldap, newusername, "broker")
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Println("add account done......")

	info := newHdfsPolicyInfo(policyRepoType, policyRepoName, policyName, "/servicebroker/"+dname)

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, newusername)
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm,  "read", "write", "execute")
	ranger.AddPermissionToPolicy(&info, perm)

	var policyId int
	for i := 0; i < 10; i++ {
		policyId, err = ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, info)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}

	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	fmt.Println("create policy done......")

	policyIdStr := strconv.Itoa(policyId)

	DashboardURL := "http://"

	myServiceInfo := ServiceInfo{
		Url:            hdfsUrl,
		Admin_user:     "ocdp",
		Admin_password: policyIdStr,  //把创建好的policy的id通过这个参数传递
		Database:       dname,
		User:           newusername,
		Password:       "",
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
	fmt.Println("delete directory done......")

	policyId, err := strconv.Atoi(myServiceInfo.Admin_password)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	_, err = ranger.DeletePolicy(rangerEndpoint, rangerUser, rangerPassword, policyId)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}
	fmt.Println("delete policy done......")

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	err = deleteAccount(ldap, myServiceInfo.User)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}
	fmt.Println("delete account done......")

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
	newAccount := "xm"+getRandom()
	err = addAccount(ldap, newAccount, "broker")
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}
	fmt.Println("add account done......")

	policyName := getRandom()

	info := newHdfsPolicyInfo(policyRepoType, policyRepoName, policyName, "/servicebroker/"+myServiceInfo.Database)

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, newAccount)
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm,  "read", "write", "execute")
	ranger.AddPermissionToPolicy(&info, perm)

	var policyId int
	for i := 0; i < 10; i++ {
		policyId, err = ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, info)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}

	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}

	fmt.Println("create policy done......")

	policyIdStr := strconv.Itoa(policyId)

	mycredentials := Credentials{
		Uri:      "",
		Hostname: strings.Split(myServiceInfo.Url, ":")[0],
		Port:     strings.Split(myServiceInfo.Url, ":")[1],
		Username: newAccount,
		Password: policyIdStr,  //通过这个参数来传递policyId
		Name:     myServiceInfo.Database,
	}

	myBinding := brokerapi.Binding{Credentials: mycredentials}

	return myBinding, mycredentials, nil
}

func (handler *Hdfs_sharedHandler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {
	fmt.Println("DoUnbind......")

	policyId, err := strconv.Atoi(myServiceInfo.Admin_password)
	if err != nil {
		return err
	}

	_, err = ranger.DeletePolicy(rangerEndpoint, rangerUser, rangerPassword, policyId)
	if err != nil {
		return err
	}
	fmt.Println("delete policy done......")

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		return err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		return err
	}

	err = deleteAccount(ldap, myServiceInfo.User)
	if err != nil {
		return err
	}
	fmt.Println("delete account done......")

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
	err := ldap.Delete("uid="+user+",ou=People,dc=asiainfo,dc=com")
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
