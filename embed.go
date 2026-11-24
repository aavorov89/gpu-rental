// Keep the embed directive here. Goose treats every Go file in migrations/
// as a migration.
package gpurental

import "embed"

//go:embed migrations/*.sql
var Migrations embed.FS
