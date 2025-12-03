package sequala.migrate.postgres

import sequala.migrate.{MigrationGenerator, MigrationStep, SchemaDiff}
import sequala.schema.DataType
import sequala.schema.postgres.*

object PostgresMigrationGenerator
    extends MigrationGenerator[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]

type PostgresMigrationStep = MigrationStep[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]
type PostgresSchemaDiff = SchemaDiff[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]
