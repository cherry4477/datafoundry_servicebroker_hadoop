package ranger_test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/xmwilldo/ranger"
	"testing"
)

func Test_DeletePolicy(t *testing.T) {
	isDelete, err := ranger.DeletePolicy("10.1.130.127:6080", "admin", "admin", 44)
	require.NoError(t, err)
	fmt.Println(isDelete)
}
