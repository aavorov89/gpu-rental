// Package gpurental embeds the SQL migrations. The file lives outside
// migrations/ because the Goose command treats every Go file there as a migration.
package gpurental

import "embed"

//go:embed migrations/*.sql
var Migrations embed.FS
