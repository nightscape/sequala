package sequala.migrate.oracle

import sequala.migrate.{MigrationGenerator, MigrationStep}
import sequala.schema.{DataType, SchemaDiffOp}
import sequala.schema.oracle.*

object OracleMigrationGenerator extends MigrationGenerator[OracleDataType, OracleColumnOptions, OracleTableOptions]

type OracleMigrationStep = MigrationStep[OracleDataType, OracleColumnOptions, OracleTableOptions]
type OracleSchemaDiff = SchemaDiffOp
