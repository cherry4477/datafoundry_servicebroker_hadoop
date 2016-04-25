package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mqu/openldap"
	"github.com/pivotal-cf/brokerapi"
	"github.com/sdming/goh"
	"github.com/xmwilldo/ranger"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var (
	hbaseUrl string
	//hdfsUser string

	//ldapUrl      string
	//ldapUser     string
	//ldapPassword string
	//
	//rangerEndpoint string
	//rangerUser     string
	//rangerPassword string
	//policyRepoType string
	//policyRepoName string
)

type Hbase_sharedHandler struct{}

func (handler *Hbase_sharedHandler) DoProvision(instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, ServiceInfo, error) {
	fmt.Println("DoProvision......")

	tableName := getRandom()
	err := createHbaseTable(tableName)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Printf("Create Hbase table %s done......\n", tableName)

	newAccount := "wm_" + getRandom()
	policyName := getRandom()

	ldap, err := openldap.Initialize(ldapUrl)
	if err != nil {
		rollbackDeleteHbaseTable(tableName)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		rollbackDeleteHbaseTable(tableName)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	err = addAccount(ldap, newAccount, "broker")
	if err != nil {
		rollbackDeleteHbaseTable(tableName)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}
	fmt.Printf("Create account %s done......\n", newAccount)

	info := newHbasePolicyInfo("ocdp_hbase", policyName, tableName)

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, newAccount)
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm, "read", "write", "create", "admin")
	ranger.AddPermissionToHbasePolicy(&info, perm)

	var policyId int
	for i := 0; i < 10; i++ {
		fmt.Println("try create policy......")
		policyId, err = ranger.CreateHbasePolicy(rangerEndpoint, rangerUser, rangerPassword, info)
		if err != nil {
			time.Sleep(time.Second * 3)
			continue
		} else {
			break
		}
	}

	if err != nil {
		rollbackDeleteAccount(newAccount)
		rollbackDeleteHbaseTable(tableName)
		return brokerapi.ProvisionedServiceSpec{}, ServiceInfo{}, err
	}

	fmt.Printf("Create policy %s done......\n", policyName)

	DashboardURL := "http://"

	myServiceInfo := ServiceInfo{
		Url:             hbaseUrl,
		Admin_user:      "ocdp",
		Database:        tableName,
		User:            newAccount,
		Password:        "",
		HbasePolicyInfo: info,
		Policy_id:       policyId,
	}

	provisiondetail := brokerapi.ProvisionedServiceSpec{DashboardURL: DashboardURL, IsAsync: false}

	return provisiondetail, myServiceInfo, nil
}

func (handler *Hbase_sharedHandler) DoLastOperation(myServiceInfo *ServiceInfo) (brokerapi.LastOperation, error) {
	fmt.Println("DoLastOperation......")

	return brokerapi.LastOperation{
		State:       brokerapi.Succeeded,
		Description: "It's a sync method!",
	}, nil
}

func (handler *Hbase_sharedHandler) DoDeprovision(myServiceInfo *ServiceInfo, asyncAllowed bool) (brokerapi.IsAsync, error) {
	fmt.Println("DoDeprovision......")

	err := deleteHbaseTable(myServiceInfo.Database)
	if err != nil {
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("Delete Hbase table %s done......\n", myServiceInfo.Database)

	info := ranger.HbasePolicyInfo{}
	fmt.Println(myServiceInfo.Policy_id)
	resp, err := ranger.GetPolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateHbaseTable(myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}
	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rollbackCreateHbaseTable(myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}

		return brokerapi.IsAsync(false), errors.New(string(respbody))
	} else {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rollbackCreateHbaseTable(myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}
		err = json.Unmarshal(respbody, &info)
		if err != nil {
			rollbackCreateHbaseTable(myServiceInfo.Database)
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
		rollbackCreateHbaseTable(myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}

	err = ldap.Bind(ldapUser, ldapPassword)
	if err != nil {
		rollbackCreateHbaseTable(myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}

	for _, user := range userList {
		err = deleteAccount(ldap, user)
		if err != nil {
			rollbackCreateHbaseTable(myServiceInfo.Database)
			return brokerapi.IsAsync(false), err
		}
	}
	fmt.Printf("Delete account %v done......\n", userList)

	_, err = ranger.DeletePolicy(rangerEndpoint, rangerUser, rangerPassword, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateAccount(myServiceInfo.User)
		rollbackCreateHbaseTable(myServiceInfo.Database)
		return brokerapi.IsAsync(false), err
	}
	fmt.Printf("Delete policy %s done......\n", myServiceInfo.HbasePolicyInfo.PolicyName)

	return brokerapi.IsAsync(false), nil
}

func (handler *Hbase_sharedHandler) DoBind(myServiceInfo *ServiceInfo, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, Credentials, error) {
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
	fmt.Printf("Create account %s done......\n", newAccount)

	info := ranger.HbasePolicyInfo{}
	//
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
		_, err = ranger.UpdateHbasePolicy(rangerEndpoint, rangerUser, rangerPassword, info, myServiceInfo.Policy_id)
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
	fmt.Printf("Update policy %s done......\n", myServiceInfo.HbasePolicyInfo.PolicyName)

	mycredentials := Credentials{
		Uri:      myServiceInfo.Url,
		Hostname: strings.Split(myServiceInfo.Url, ":")[0],
		Port:     strings.Split(myServiceInfo.Url, ":")[1],
		Username: newAccount,
		Password: "",
		Name:     myServiceInfo.Database,
	}

	myBinding := brokerapi.Binding{Credentials: mycredentials}

	return myBinding, mycredentials, nil
}

func (handler *Hbase_sharedHandler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {
	fmt.Println("DoUnbind......")

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

	info := ranger.HbasePolicyInfo{}

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

	ranger.RemoveUserFromHbasePermission(&info, mycredentials.Username)

	fmt.Println(info.PermMapList)

	_, err = ranger.UpdateHbasePolicy(rangerEndpoint, rangerUser, rangerPassword, info, myServiceInfo.Policy_id)
	if err != nil {
		rollbackCreateAccount(mycredentials.Username)
		return err
	}
	fmt.Printf("Delete %s from policy %s done......\n", mycredentials.Username, myServiceInfo.HbasePolicyInfo.PolicyName)

	return nil
}

func init() {
	register("hbase_shared", &Hbase_sharedHandler{})
	hbaseUrl = getenv("HBASEURL")
	ldapUrl = getenv("LDAPURL")
	ldapUser = getenv("LDAPUSER")
	ldapPassword = getenv("LDAPPASSWORD")
	rangerEndpoint = getenv("RANGERENDPOINT")
	rangerUser = getenv("RANGERUSER")
	rangerPassword = getenv("RANGERPASSWORD")
}

func createHbaseTable(tableName string) error {
	client, err := goh.NewTcpClient(hbaseUrl, goh.TBinaryProtocol, false)
	if err != nil {
		return err
	}

	if err = client.Open(); err != nil {
		return err
	}

	defer client.Close()

	cols := make([]*goh.ColumnDescriptor, 1)
	cols[0] = goh.NewColumnDescriptorDefault("default")

	if _, err = client.CreateTable(tableName, cols); err != nil {
		fmt.Println(err)
	}

	return nil
}

func deleteHbaseTable(tableName string) error {
	client, err := goh.NewTcpClient(hbaseUrl, goh.TBinaryProtocol, false)
	if err != nil {
		return err
	}

	if err = client.Open(); err != nil {
		return err
	}

	defer client.Close()

	err = client.DisableTable(tableName)
	if err != nil {
		return err
	}

	err = client.DeleteTable(tableName)
	if err != nil {
		return err
	}

	return nil
}

func newHbasePolicyInfo(repoName, policyName, tableName string) ranger.HbasePolicyInfo {

	info := ranger.NewHbasePolicyInfo()
	info.RepositoryType = "hbase"
	info.RepositoryName = repoName
	info.PolicyName = policyName
	info.Tables = tableName
	info.ColumnFamilies = "*"
	info.Columns = "*"

	return info
}

func rollbackDeleteHbaseTable(tableName string) {
	fmt.Println("Error occurred ! Rollback delete hbase table......")

	var err error
	var client *goh.HClient
	for i := 0; i < 10; i++ {
		client, err = goh.NewTcpClient(hbaseUrl, goh.TBinaryProtocol, false)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}

		if err = client.Open(); err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
		defer client.Close()

		err = client.DisableTable(tableName)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
			fmt.Println(err)
		}

		err = client.DeleteTable(tableName)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
	}
	if err == nil {
		fmt.Printf("Rollback delete Hbase table %s done......\n", tableName)
	} else {
		fmt.Println("Rollback failed......")
	}

}

func rollbackCreateHbaseTable(tableName string) {
	fmt.Println("Error occurred ! Rollback create Hbase table......")

	var err error
	var client *goh.HClient
	for i := 0; i < 10; i++ {
		client, err = goh.NewTcpClient(hbaseUrl, goh.TBinaryProtocol, false)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}

		if err = client.Open(); err != nil {
			time.Sleep(time.Second * 2)
			continue
		}

		defer client.Close()

		cols := make([]*goh.ColumnDescriptor, 1)
		cols[0] = goh.NewColumnDescriptorDefault("default")

		if _, err = client.CreateTable(tableName, cols); err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
	}

	if err == nil {
		fmt.Printf("Rollback create Hbase table %s done......\n", tableName)
	} else {
		fmt.Println("Rollback failed......")
	}
}
