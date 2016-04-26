package ranger

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

func DeletePolicy(addr, username, password string, policyId int) (bool, error) {

	url := fmt.Sprintf("http://%s%s/%d", addr, POLICYURL, policyId)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != http.StatusNoContent {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		return false, errors.New(string(respbody))
	}

	return true, nil
}

func RemoveUserFromHdfsPermission(info *HdfsPolicyInfo, a ...string) {
	permList := info.PermMapList
	userList := make([]string, 0)
	var groupList [1]string = [1]string{"broker"}
	var pList [3]string = [3]string{"read", "write", "execute"}

	for _, perm := range permList {
		for _, v := range perm.UserList {
			userList = append(userList, v)
		}
	}
	fmt.Println(userList)

	for k, v := range userList {
		if v == a[0] {
			kk := k + 1
			userList = append(userList[:k], userList[kk:]...)
		}
	}
	fmt.Println(userList)
	info.PermMapList = make([]Permission, 1)
	info.PermMapList[0].UserList = userList
	info.PermMapList[0].GroupList = groupList[:]
	info.PermMapList[0].PermList = pList[:]
}

func RemoveUserFromHbasePermission(info *HbasePolicyInfo, a ...string) {
	permList := info.PermMapList
	userList := make([]string, 0)
	var groupList [1]string = [1]string{"broker"}
	var pList [4]string = [4]string{"read", "write", "create", "admin"}

	for _, perm := range permList {
		for _, v := range perm.UserList {
			userList = append(userList, v)
		}
	}
	fmt.Println(userList)

	for k, v := range userList {
		if v == a[0] {
			kk := k + 1
			userList = append(userList[:k], userList[kk:]...)
		}
	}
	fmt.Println(userList)
	info.PermMapList = make([]Permission, 1)
	info.PermMapList[0].UserList = userList
	info.PermMapList[0].GroupList = groupList[:]
	info.PermMapList[0].PermList = pList[:]
}
