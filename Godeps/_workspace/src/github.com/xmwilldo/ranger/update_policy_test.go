package ranger_test

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/xmwilldo/ranger"
	"io/ioutil"
	"net/http"
	"testing"
)

func Test_UpdateHdfsPolicy(t *testing.T) {
	info := ranger.NewHdfsPolicyInfo()
	info.RepositoryType = "hdfs"
	info.RepositoryName = "ocdp_hadoop"
	info.PolicyName = "46976efa0016521"
	info.ResourceName = "/servicebroker/aac358ad7937265"

	p := ranger.InitPermission()
	ranger.AddUserToPermission(&p, "wm_876eb2a19bc376d")
	ranger.AddGroupToPermission(&p, "broker")
	ranger.AddPermToPermission(&p, "read")
	ranger.AddPermissionToHdfsPolicy(&info, p)

	fmt.Println(info)

	policyId, err := ranger.UpdateHdfsPolicy("10.1.130.127:6080", "admin", "admin", info, 78)
	fmt.Println(policyId)
	require.NoError(t, err)
}

func Test_UpdateHbasePolicy(t *testing.T) {
	info := ranger.NewHbasePolicyInfo()
	resp, err := ranger.GetPolicy("10.1.130.127:6080", "admin", "admin", 302)
	if resp.StatusCode != http.StatusOK {
		_, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
	} else {
		respbody, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		err = json.Unmarshal(respbody, &info)
		require.NoError(t, err)
	}

	info.PolicyName = "test_create1"
	info.Tables = "test_create1"
	fmt.Println(info)

	policyId, err := ranger.UpdateHbasePolicy("10.1.130.127:6080", "admin", "admin", info, 302)
	fmt.Println(policyId)
	require.NoError(t, err)
}
