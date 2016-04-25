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

func NewUserList(a ...string) []string {
	userList := make([]string, 0)
	for _, user := range a {
		userList = append(userList, user)
	}

	return userList
}

func NewGroupList(a ...string) []string {
	groupList := make([]string, 0)
	for _, group := range a {
		groupList = append(groupList, group)
	}

	return groupList
}

func NewPermList(a ...string) []string {
	permList := make([]string, 0)
	for _, perm := range a {
		permList = append(permList, perm)
	}

	return permList
}

func InitPermission() Permission {
	p := Permission{}
	p.UserList = make([]string, 0)
	p.GroupList = make([]string, 0)
	p.PermList = make([]string, 0)

	return p
}

func updatePermission(p *Permission, userList, groupList, permList []string) {
	if userList == nil {
		userList = make([]string, 0)
	}
	if groupList == nil {
		groupList = make([]string, 0)
	}
	if permList == nil {
		permList = make([]string, 0)
	}
	p.UserList = userList
	p.GroupList = groupList
	p.PermList = permList
	return
}

func NewPermMapList(a ...Permission) []Permission {
	permissionList := make([]Permission, 0)
	for _, permission := range a {
		permissionList = append(permissionList, permission)
	}

	return permissionList
}
