package adb

type Config struct {
	Endpoints []string `envconfig:"ARANGODB_ENDPOINTS" required:"true" json:"adb_endpoints"`
	User      string   `envconfig:"ARANGODB_USER" required:"true" json:"adb_user"`
	Password  string   `envconfig:"ARANGODB_PASSWORD" required:"true" json:"adb_password"`
	Database  string   `envconfig:"ARANGODB_DATABASE" required:"true" json:"adb_database"`
}
