package ranger_test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/xmwilldo/ranger"
	"testing"
)

func Test_CreateHdfsPolicy(t *testing.T) {
	info := ranger.NewHdfsPolicyInfo()
	info.RepositoryType = "hdfs"
	info.RepositoryName = "ocdp_hadoop"
	info.PolicyName = "mypolicy1"
	info.ResourceName = "/servicebroker"

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, "brokertest1")
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm, "read", "write")
	ranger.AddPermissionToHdfsPolicy(&info, perm)

	created, err := ranger.CreateHdfsPolicy("10.1.130.127:6080", "admin", "admin", info)
	require.NoError(t, err)

	fmt.Println(created)
}

func Test_CreateHbasePolicy(t *testing.T) {
	info := ranger.NewHbasePolicyInfo()
	info.RepositoryType = "hbase"
	info.RepositoryName = "ocdp_hbase"
	info.PolicyName = "testpolicy1"
	info.Tables = "test_create"
	info.ColumnFamilies = "*"
	info.Columns = "*"

	perm := ranger.InitPermission()
	ranger.AddUserToPermission(&perm, "brokertest1")
	ranger.AddGroupToPermission(&perm, "broker")
	ranger.AddPermToPermission(&perm, "read", "write", "create", "admin")
	ranger.AddPermissionToHbasePolicy(&info, perm)

	fmt.Println(info)
	createdId, err := ranger.CreateHbasePolicy("10.1.130.127:6080", "admin", "admin", info)
	require.NoError(t, err)

	fmt.Println(createdId)
}
