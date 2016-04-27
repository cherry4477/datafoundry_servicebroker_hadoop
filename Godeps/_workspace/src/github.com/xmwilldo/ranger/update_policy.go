package ranger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

func UpdateHdfsPolicy(addr, username, password string, info HdfsPolicyInfo, policyId int) (int, error) {

	b, err := json.Marshal(info)
	if err != nil {
		return UPDATEFAILCODE, err
	}
	reqbody := bytes.NewBuffer([]byte(b))

	url := fmt.Sprintf("http://%s%s/%d", addr, POLICYURL, policyId)
	req, err := http.NewRequest("PUT", url, reqbody)
	if err != nil {
		return UPDATEFAILCODE, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return UPDATEFAILCODE, err
	}

	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return UPDATEFAILCODE, errors.New(string("read respbody error."))
		}
		return UPDATEFAILCODE, errors.New(string(respbody))
	}

	return policyId, nil
}

func UpdateHbasePolicy(addr, username, password string, info HbasePolicyInfo, policyId int) (int, error) {

	b, err := json.Marshal(info)
	if err != nil {
		return UPDATEFAILCODE, err
	}
	reqbody := bytes.NewBuffer([]byte(b))

	url := fmt.Sprintf("http://%s%s/%d", addr, POLICYURL, policyId)
	req, err := http.NewRequest("PUT", url, reqbody)
	if err != nil {
		return UPDATEFAILCODE, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return UPDATEFAILCODE, err
	}

	if resp.StatusCode != http.StatusOK {
		respbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return UPDATEFAILCODE, errors.New(string("read respbody error."))
		}
		return UPDATEFAILCODE, errors.New(string(respbody))
	}

	return policyId, nil
}
