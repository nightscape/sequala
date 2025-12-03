package sequala.migrate.oracle

import sequala.migrate.{MigrationGenerator, MigrationStep, SchemaDiff}
import sequala.schema.DataType
import sequala.schema.oracle.*

object OracleMigrationGenerator extends MigrationGenerator[OracleDataType, OracleColumnOptions, OracleTableOptions]

type OracleMigrationStep = MigrationStep[OracleDataType, OracleColumnOptions, OracleTableOptions]
type OracleSchemaDiff = SchemaDiff[OracleDataType, OracleColumnOptions, OracleTableOptions]
