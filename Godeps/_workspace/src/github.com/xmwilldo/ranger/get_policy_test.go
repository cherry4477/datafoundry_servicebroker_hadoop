package ranger_test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/xmwilldo/ranger"
	"testing"
)

func Test_GetPolicy(t *testing.T) {
	info, err := ranger.GetPolicy("10.1.130.127:6080", "admin", "admin", 178)
	require.NoError(t, err)
	fmt.Println(info)
}
