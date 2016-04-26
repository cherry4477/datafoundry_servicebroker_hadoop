package ranger

const (
	POLICYURL      = "/service/public/api/policy"
	CREATEFAILCODE = -1
	UPDATEFAILCODE = -1
)

var (
	rangerEndpoint string
	rangerUser     string
	rangerPassword string
)

type HdfsPolicyInfo struct {
	PolicyName     string       `json:"policyName"`
	ResourceName   string       `json:"resourceName"`
	Description    string       `json:"description, omitempty"`
	RepositoryName string       `json:"repositoryName"`
	RepositoryType string       `json:"repositoryType"`
	IsEnabled      bool         `json:"isEnabled"`
	IsRecursive    bool         `json:"isRecursive"`
	IsAuditEnabled bool         `json:"isAuditEnabled"`
	PermMapList    []Permission `json:"permMapList, omitempty"`
}

type HbasePolicyInfo struct {
	PolicyName     string       `json:"policyName"`
	Tables         string       `json:"tables"`
	ColumnFamilies string       `json:"columnFamilies"`
	Columns        string       `json:"columns"`
	Description    string       `json:"description, omitempty"`
	RepositoryName string       `json:"repositoryName"`
	RepositoryType string       `json:"repositoryType"`
	IsEnabled      bool         `json:"isEnabled"`
	IsAuditEnabled bool         `json:"isAuditEnabled"`
	TableType      string       `json:"tableType"`
	ColumnType     string       `json:"columnType"`
	PermMapList    []Permission `json:"permMapList, omitempty"`
}

type result struct {
	Id             int          `json:"id"`
	createDate     string       `json:"createDate"`
	UpdateDate     string       `json:"updateDate"`
	Owner          string       `json:"owner"`
	UpdatedBy      string       `json:"updatedBy"`
	PolicyName     string       `json:"policyName"`
	ResourceName   string       `json:"resourceName"`
	Description    string       `json:"description, omitempty"`
	RepositoryName string       `json:"repositoryName"`
	RepositoryType string       `json:"repositoryType"`
	PermMapList    []Permission `json:"permMapList, omitempty"`
	IsEnabled      bool         `json:"isEnabled"`
	IsRecursive    bool         `json:"isRecursive"`
	IsAuditEnabled bool         `json:"isAuditEnabled"`
	Version        string       `json:"version"`
	ReplacePerm    bool         `json:"replacePerm"`
}

type Permission struct {
	UserList  []string `json:"userList, omitempty"`
	GroupList []string `json:"groupList, omitempty"`
	PermList  []string `json:"permList, omitempty"`
}

func NewHdfsPolicyInfo() (info HdfsPolicyInfo) {
	info.IsEnabled = true
	info.IsRecursive = true
	info.IsAuditEnabled = true
	info.PermMapList = make([]Permission, 0)

	return info
}

func NewHbasePolicyInfo() (info HbasePolicyInfo) {
	info.IsEnabled = true
	info.IsAuditEnabled = true
	info.TableType = "Inclusion"
	info.ColumnType = "Inclusion"
	return info
}

func InitPermission() Permission {
	p := Permission{}
	p.UserList = make([]string, 0)
	p.GroupList = make([]string, 0)
	p.PermList = make([]string, 0)

	return p
}
