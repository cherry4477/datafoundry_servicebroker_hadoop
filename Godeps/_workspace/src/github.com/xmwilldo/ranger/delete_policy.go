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
