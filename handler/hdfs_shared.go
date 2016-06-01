package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mqu/openldap"
	"github.com/pivotal-cf/brokerapi"
	hdfs "github.com/xmwilldo/gowfs"
	"github.com/xmwilldo/ranger"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
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
)

type Hdfs_sharedHandler struct{}

func (handler *Hdfs_sharedHandler) DoProvision(instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, ServiceInfo, error) {
	fmt.Println("DoProvision......")
	//fmt.Println(details.ServiceID, details.PlanID)

	err := initCookie()
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	princpalName := "wm" + getRandom()
	err = createPrincpal(princpalName, "asiainfo")
	if err != nil {
		fmt.Println("create princpal err!")
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Printf("create princpal %s@ASIAINFO.COM done......\n", princpalName)

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

	newAccount := princpalName
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

	info := newHdfsPolicyInfo("ocdp_hadoop", policyName, "/servicebroker/"+dname)

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, newAccount)
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm, "read", "write", "execute")
	ranger.AddPermissionToHdfsPolicy(&info, perm)

	var policyId int
	for i := 0; i < 10; i++ {
		fmt.Println("try create policy......")
		policyId, err = ranger.CreateHdfsPolicy(rangerEndpoint, rangerUser, rangerPassword, info)
		if err != nil {
			time.Sleep(time.Second * 3)
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
		Url: hdfsUrl,
		//Admin_user: princpalName,
		//Admin_password: "asiainfo",
		Database:       dname,
		User:           newAccount,
		Password:       "asiainfo",
		HdfsPolicyInfo: info,
		Policy_id:      policyId,
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

	err := initCookie()
	if err != nil {
		return brokerapi.IsAsync(false), err
	}

	err = deletePrincpal(myServiceInfo.User)
	if err != nil {
		fmt.Println("delete princpal err!")
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("delete princpal %s@ASIAINFO.COM done......\n", myServiceInfo.User)

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

	info := ranger.HdfsPolicyInfo{}
	fmt.Println(myServiceInfo.Policy_id)
	resp, err := ranger.GetPolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}
	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rollbackCreateDirectory(fs, myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}

		return brokerapi.IsAsync(false), errors.New(string(respbody))
	} else {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rollbackCreateDirectory(fs, myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}
		err = json.Unmarshal(respbody, &info)
		if err != nil {
			rollbackCreateDirectory(fs, myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}
	}

	var userList []string
	for _, perm := range info.PermMapList {
		for _, user := range perm.UserList {
			userList = append(userList, user)
		}
	}
	fmt.Println(userList)

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}

	for _, user := range userList {
		err = deleteAccount(ldap, user)
		if err != nil {
			rollbackCreateDirectory(fs, myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}
	}

	fmt.Printf("Delete account %a done......\n", userList)

	_, err = ranger.DeletePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateAccount(myServiceInfo.User)
		rollbackCreateDirectory(fs, myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("Delete policy %s done......\n", myServiceInfo.HdfsPolicyInfo.PolicyName)

	return brokerapi.IsAsync(false), nil
}

func (handler *Hdfs_sharedHandler) DoBind(myServiceInfo *ServiceInfo, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, Credentials, error) {
	fmt.Println("DoBind......")

	princpalName := "wm" + getRandom()
	password := getRandom()
	random := []rune(password)
	password = string(random[0:8])
	err := createPrincpal(princpalName, password)
	if err != nil {
		fmt.Println("create princpal err!")
		return brokerapi.Binding{}, Credentials{}, err
	}
	fmt.Printf("create princpal %s@ASIAINFO.COM done......\n", princpalName)

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}
	newAccount := princpalName
	err = addAccount(ldap, newAccount, "broker")
	if err != nil {
		return brokerapi.Binding{}, Credentials{}, err
	}
	//myServiceInfo.Bind_user = newAccount
	fmt.Printf("Create account %s done......\n", newAccount)

	//policyName := getRandom()

	info := ranger.HdfsPolicyInfo{}

	resp, err := ranger.GetPolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)

	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return brokerapi.Binding{}, Credentials{}, err
		}

		return brokerapi.Binding{}, Credentials{}, errors.New(string(respbody))
	} else {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return brokerapi.Binding{}, Credentials{}, err
		}
		err = json.Unmarshal(respbody, &info)
		fmt.Println(info)
		if err != nil {
			return brokerapi.Binding{}, Credentials{}, err
		}
	}

	ranger.AddUserToPermission(&info.PermMapList[0], newAccount)

	for i := 0; i < 10; i++ {
		fmt.Println("try update policy......")
		_, err = ranger.UpdateHdfsPolicy(rangerEndpoint, rangerUser, rangerPassword, info, myServiceInfo.Policy_id)
		if err != nil {
			time.Sleep(time.Second * 3)
			continue
		} else {
			break
		}
	}

	if err != nil {
		rollbackDeleteAccount(newAccount)
		return brokerapi.Binding{}, Credentials{}, err
	}

	fmt.Printf("Update policy %s done......\n", myServiceInfo.HdfsPolicyInfo.PolicyName)

	mycredentials := Credentials{
		Uri:      myServiceInfo.Url,
		Hostname: strings.Split(myServiceInfo.Url, ":")[0],
		Port:     strings.Split(myServiceInfo.Url, ":")[1],
		Username: newAccount,
		Password: password,
		Name:     myServiceInfo.Database,
	}

	myBinding := brokerapi.Binding{Credentials: mycredentials}

	return myBinding, mycredentials, nil
}

func (handler *Hdfs_sharedHandler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {
	fmt.Println("DoUnbind......")

	err := deletePrincpal(mycredentials.Username)
	if err != nil {
		fmt.Println("delete princpal err!")
		return err
	}
	fmt.Printf("delete princpal %s@ASIAINFO.COM done......\n", mycredentials.Username)

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		return err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		return err
	}

	err = deleteAccount(ldap, mycredentials.Username)
	if err != nil {
		return err
	}
	fmt.Printf("Delete account %s done......\n", mycredentials.Username)

	info := ranger.HdfsPolicyInfo{}

	resp, err := ranger.GetPolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateAccount(mycredentials.Username)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rollbackCreateAccount(mycredentials.Username)
			return err
		}

		return errors.New(string(respbody))
	} else {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rollbackCreateAccount(mycredentials.Username)
			return err
		}
		err = json.Unmarshal(respbody, &info)
		if err != nil {
			rollbackCreateAccount(mycredentials.Username)
			return err
		}
	}

	fmt.Println(info.PermMapList)

	ranger.RemoveUserFromHdfsPermission(&info, mycredentials.Username)

	fmt.Println(info.PermMapList)

	_, err = ranger.UpdateHdfsPolicy(rangerEndpoint, rangerUser, rangerPassword, info, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateAccount(mycredentials.Username)
		return err
	}
	fmt.Printf("Delete user in policy %s done......\n", myServiceInfo.HdfsPolicyInfo.PolicyName)

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

func newHdfsPolicyInfo(repoName, policyName, resourceName string) ranger.HdfsPolicyInfo {

	info := ranger.NewHdfsPolicyInfo()
	info.RepositoryType = "hdfs"
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

func rollbackCreateAccount(user string) {
	fmt.Println("Error occurred ! Rollback create account......")
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
		err = addAccount(ldap, user, "broker")
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			break
		}
	}
	if err == nil {
		fmt.Printf("Rollback create account /%s done......\n", user)
	} else {
		fmt.Println("Rollback failed......")
	}
}

func initCookie() error {
	isExist := isExistfile("/tmp/cookiejar.txt")
	if isExist {
		err := os.Remove("/tmp/cookiejar.txt")
		if err != nil {
			return err
		}
	}

	in := bytes.NewBuffer(nil)
	cmd := exec.Command("sh")
	cmd.Stdin = in
	in.WriteString("curl -i -v --negotiate -u : -b /tmp/cookiejar.txt -c /tmp/cookiejar.txt http://10.1.130.127:50070/webhdfs/v1/?op=liststatus\n")
	in.WriteString("exit\n")
	if err := cmd.Run(); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func isExistfile(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func createPrincpal(name, password string) error {
	in := bytes.NewBuffer(nil)
	cmd := exec.Command("sh")
	cmd.Stdin = in

	go func() {
		createStr := "kadmin -p 'admin/admin' -w 'admin' -q 'addprinc -pw " + password + " " + name + "'\n"
		in.WriteString(createStr)
		in.WriteString("exit\n")
	}()
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func deletePrincpal(name string) error {
	in := bytes.NewBuffer(nil)
	cmd := exec.Command("sh")
	cmd.Stdin = in

	go func() {
		//"kadmin -p 'admin/admin' -w 'admin' -q 'delprinc -force "+ name +"'"
		deleteStr := "kadmin -p 'admin/admin' -w 'admin' -q 'delprinc -force " + name + "'\n"
		in.WriteString(deleteStr)
		in.WriteString("exit\n")
	}()
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

//func rollbackCreatePolicy(rangerEndpoint, rangerUser, rangerPassword string, myServiceInfo *ServiceInfo) {
//	fmt.Println("Error occurred ! Rollback create policy......")
//	var err error
//	for i := 0; i < 10; i++ {
//		fmt.Println("try create policy......")
//		policyId, err := ranger.CreatePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.PolicyInfo)
//		if err != nil {
//			time.Sleep(time.Second * 3)
//			continue
//		} else {
//			myServiceInfo.Policy_id = policyId
//			break
//		}
//	}
//	if err == nil {
//		fmt.Printf("Rollback create policy /%s done......\n", myServiceInfo.PolicyInfo.PolicyName)
//	} else {
//		fmt.Println("Rollback failed......")
//	}
//}
