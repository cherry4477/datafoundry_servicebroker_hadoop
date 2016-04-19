package ranger

import (
	"fmt"
	"net/http"
	//"io/ioutil"
	//"errors"
	//
	//	"encoding/json"
)

func GetPolicy(addr, username, password string, policyId int) (resp *http.Response, err error) {

	//info := result{}
	url := fmt.Sprintf("http://%s%s/%d", addr, POLICYURL, policyId)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return resp, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return resp, err
	}

	//if resp.StatusCode != http.StatusOK {
	//	respbody, err := ioutil.ReadAll(resp.Body)
	//	if err != nil {
	//		return resp, err
	//	}
	//	//err = errors.New(string(respbody))
	//	return resp, errors.New(string(respbody))
	//} else {
	//	respbody, err := ioutil.ReadAll(resp.Body)
	//	if err != nil {
	//		return resp, err
	//	}
	//	err = json.Unmarshal(respbody, &info)
	//	if err != nil {
	//		return resp, err
	//	}
	//}

	return resp, err
}
