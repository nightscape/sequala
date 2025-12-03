package sequala.migrate.postgres

import sequala.migrate.{MigrationGenerator, MigrationStep}
import sequala.schema.{DataType, SchemaDiffOp}
import sequala.schema.postgres.*

object PostgresMigrationGenerator
    extends MigrationGenerator[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]

type PostgresMigrationStep = MigrationStep[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]
type PostgresSchemaDiff = SchemaDiffOp
