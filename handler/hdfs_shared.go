package handler

import (
	hdfs "github.com/gowfs"
	"github.com/mqu/openldap"
	"github.com/pivotal-cf/brokerapi"
	"github.com/xmwilldo/ranger"
	"os"
	"fmt"
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

	dname := getRandom()

	_, err = createDirectory(fs, dname, 0700)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

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

	info := newHdfsPolicyInfo(policyRepoType, policyRepoName, policyName, "/servicebroker/"+dname)

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, newusername)
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm,  "read", "write", "execute")
	ranger.AddPermissionToPolicy(&info, perm)

	for i := 0; i < 10; i++ {
		_, err = ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, info)
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

	//info := newHdfsPolicyInfo(policyRepoType, policyRepoName, policyName, "/servicebroker/"+dname)
	//
	//perm := ranger.InitPermission()
	//ranger.AddUserToPermission(&perm, newusername)
	//ranger.AddGroupToPermission(&perm, "broker")
	//ranger.AddPermToPermission(&perm,  "read", "write", "execute")
	//ranger.AddPermissionToPolicy(&info, perm)

	//_, err = ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, info)
	//if err != nil {
	//	return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	//}

	DashboardURL := "http://"

	myServiceInfo := ServiceInfo{
		Url:            hdfsUrl,
		Admin_user:     "ocdp",
		Admin_password: "",
		Database:       dname,
		User:           newusername,
		Password:       "",
	}

	provisiondetail := brokerapi.ProvisionedServiceSpec{DashboardURL: DashboardURL, IsAsync: false}

	return provisiondetail, myServiceInfo, nil
}

func (handler *Hdfs_sharedHandler) DoLastOperation(myServiceInfo *ServiceInfo) (brokerapi.LastOperation, error) {

	return brokerapi.LastOperation{
		State:       brokerapi.Succeeded,
		Description: "It's a sync method!",
	}, nil
}

func (handler *Hdfs_sharedHandler) DoDeprovision(myServiceInfo *ServiceInfo, asyncAllowed bool) (brokerapi.IsAsync, error) {
	config := hdfs.NewConfiguration()
	config.Addr = hdfsUrl
	config.User = hdfsUser
	config.MaxIdleConnsPerHost = 64

	fs, err := hdfs.NewFileSystem(*config)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	path := hdfs.Path{}
	path.Name = myServiceInfo.Database

	_, err = fs.Delete(path, true)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	return brokerapi.IsAsync(false), nil
}

func (handler *Hdfs_sharedHandler) DoBind(myServiceInfo *ServiceInfo, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, Credentials, error) {
	mycredentials := Credentials{
		Uri:      "",
		Hostname: "",
		Port:     "",
		Username: "",
		Password: "",
		Name:     myServiceInfo.Database,
	}

	myBinding := brokerapi.Binding{Credentials: mycredentials}

	return myBinding, mycredentials, nil
}

func (handler *Hdfs_sharedHandler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {

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

func createDirectory(fs *hdfs.FileSystem, dpath string, fileMode os.FileMode) (bool, error) {
	path := hdfs.Path{}
	path.Name = dpath

	isCreated, err := fs.MkDirs(path, fileMode)
	if err != nil {
		return isCreated, err
	}

	return isCreated, nil
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
