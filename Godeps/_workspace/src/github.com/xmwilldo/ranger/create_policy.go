package ranger

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

func CreatePolicy(addr, username, password string, info HdfsPolicyInfo) (int, error) {

	b, err := json.Marshal(info)
	if err != nil {
		return CREATEFAILCODE, err
	}
	reqbody := bytes.NewBuffer([]byte(b))

	req, err := http.NewRequest("POST", "http://"+addr+POLICYURL, reqbody)
	if err != nil {
		return CREATEFAILCODE, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return CREATEFAILCODE, err
	}

	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return CREATEFAILCODE, errors.New("read respbody error.")
		}
		return CREATEFAILCODE, errors.New(string(respbody))
	}

	respbody, err := ioutil.ReadAll(resp.Body)
	result := result{}
	json.Unmarshal(respbody, &result)

	return int(result.Id), nil
}

func AddUserToPermission(perm *Permission, a ...string) {

	for _, user := range a {
		perm.UserList = append(perm.UserList, user)
	}
}

func AddGroupToPermission(perm *Permission, a ...string) {

	for _, group := range a {
		perm.GroupList = append(perm.GroupList, group)
	}

}

func AddPermToPermission(p *Permission, a ...string) {

	for _, perm := range a {
		p.PermList = append(p.PermList, perm)
	}

}

func AddPermissionToPolicy(info *HdfsPolicyInfo, p Permission) {
	info.PermMapList = append(info.PermMapList, p)
}
