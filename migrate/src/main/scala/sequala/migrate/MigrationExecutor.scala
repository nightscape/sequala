package sequala.migrate

import sequala.schema.*

import java.sql.{Connection, SQLException}
import java.time.Instant
import scala.concurrent.duration.*

trait SchemaDiffRenderer[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions] {
  def renderCreateTable(ct: CreateTable[DT, CO, TO]): String
  def renderDropTable(dt: DropTable[?]): String
  def renderAlterTable(at: AlterTable[DT, CO, TO, AlterTableAction[DT, CO]]): Seq[String]
  def renderCreateIndex(ci: CreateIndex[?]): String
  def renderDropIndex(di: DropIndex): String
  def renderSetTableComment(stc: SetTableComment): String
  def renderSetColumnComment(scc: SetColumnComment): String

  def render(diff: SchemaDiffOp): Seq[String] = diff match {
    case ct: CreateTable[DT, CO, TO] @unchecked => Seq(renderCreateTable(ct))
    case dt: DropTable[?] => Seq(renderDropTable(dt))
    case at: AlterTable[DT, CO, TO, AlterTableAction[DT, CO]] @unchecked => renderAlterTable(at)
    case ci: CreateIndex[?] => Seq(renderCreateIndex(ci))
    case di: DropIndex => Seq(renderDropIndex(di))
    case stc: SetTableComment => Seq(renderSetTableComment(stc))
    case scc: SetColumnComment => Seq(renderSetColumnComment(scc))
  }
}

enum TransactionMode {
  case Transactional
  case AutoCommit
  case DryRun
}

trait ExecutionCallback {
  def onStepStarting(stepIndex: Int, sql: String, comment: String): Unit = ()
  def onStepCompleted(stepIndex: Int, sql: String, duration: Duration): Unit = ()
  def onStepFailed(stepIndex: Int, sql: String, error: Throwable): Unit = ()
  def onRollbackStarting(stepIndex: Int, reverseSql: String): Unit = ()
  def onRollbackCompleted(stepIndex: Int, reverseSql: String): Unit = ()
}

object ExecutionCallback {
  val NoOp: ExecutionCallback = new ExecutionCallback {}
}

trait MigrationExecutor[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions] {
  def renderer: SchemaDiffRenderer[DT, CO, TO]
  def supportsTransactionalDDL: Boolean

  def execute(
    connection: Connection,
    steps: Seq[MigrationStep[DT, CO, TO]],
    mode: TransactionMode = TransactionMode.Transactional,
    callback: ExecutionCallback = ExecutionCallback.NoOp,
    rollbackOnFailure: Boolean = true
  ): ExecutionResult = {
    val startedAt = Instant.now()
    val startNanos = System.nanoTime()
    val originalAutoCommit = connection.getAutoCommit

    val effectiveMode = mode match {
      case TransactionMode.Transactional if !supportsTransactionalDDL => TransactionMode.AutoCommit
      case other => other
    }

    effectiveMode match {
      case TransactionMode.DryRun =>
        val dryRunSteps = steps.zipWithIndex.flatMap { case (step, idx) =>
          val sqls = renderer.render(step.statement)
          sqls.map { sql =>
            StepResult(idx, sql, step.comment, StepStatus.Skipped, Duration.Zero)
          }
        }
        val endNanos = System.nanoTime()
        ExecutionResult(dryRunSteps, (endNanos - startNanos).nanos, startedAt, Instant.now())

      case TransactionMode.Transactional =>
        connection.setAutoCommit(false)
        try {
          val results = executeSteps(connection, steps, callback)
          val failedStep = results.find(_.status == StepStatus.Failed)
          failedStep match {
            case Some(_) if rollbackOnFailure =>
              connection.rollback()
              val rolledBackResults = results.map { r =>
                if r.status == StepStatus.Success then r.copy(status = StepStatus.RolledBack)
                else r
              }
              val endNanos = System.nanoTime()
              ExecutionResult(rolledBackResults, (endNanos - startNanos).nanos, startedAt, Instant.now())
            case Some(_) =>
              connection.rollback()
              val endNanos = System.nanoTime()
              ExecutionResult(results, (endNanos - startNanos).nanos, startedAt, Instant.now())
            case None =>
              connection.commit()
              val endNanos = System.nanoTime()
              ExecutionResult(results, (endNanos - startNanos).nanos, startedAt, Instant.now())
          }
        } finally {
          connection.setAutoCommit(originalAutoCommit)
        }

      case TransactionMode.AutoCommit =>
        connection.setAutoCommit(true)
        var executedSteps: List[(Int, MigrationStep[DT, CO, TO], Seq[String])] = Nil
        var results: List[StepResult] = Nil
        var failed = false

        for (step, idx) <- steps.zipWithIndex if !failed do {
          val sqls = renderer.render(step.statement)
          executedSteps = (idx, step, sqls) :: executedSteps
          val stepStartNanos = System.nanoTime()

          for sql <- sqls if !failed do {
            callback.onStepStarting(idx, sql, step.comment)
            try {
              val stmt = connection.createStatement()
              stmt.execute(sql)
              stmt.close()
              val stepEndNanos = System.nanoTime()
              callback.onStepCompleted(idx, sql, (stepEndNanos - stepStartNanos).nanos)
              results =
                StepResult(idx, sql, step.comment, StepStatus.Success, (stepEndNanos - stepStartNanos).nanos) :: results
            } catch {
              case e: SQLException =>
                val stepEndNanos = System.nanoTime()
                callback.onStepFailed(idx, sql, e)
                results = StepResult(
                  idx,
                  sql,
                  step.comment,
                  StepStatus.Failed,
                  (stepEndNanos - stepStartNanos).nanos,
                  Some(e)
                ) :: results
                failed = true
                if rollbackOnFailure then {
                  rollbackExecutedSteps(connection, executedSteps.tail, callback)
                }
            }
          }
        }

        val endNanos = System.nanoTime()
        ExecutionResult(results.reverse, (endNanos - startNanos).nanos, startedAt, Instant.now())
    }
  }

  private def executeSteps(
    connection: Connection,
    steps: Seq[MigrationStep[DT, CO, TO]],
    callback: ExecutionCallback
  ): Seq[StepResult] = {
    var results: List[StepResult] = Nil
    var failed = false

    for (step, idx) <- steps.zipWithIndex if !failed do {
      val sqls = renderer.render(step.statement)
      for sql <- sqls if !failed do {
        val stepStartNanos = System.nanoTime()
        callback.onStepStarting(idx, sql, step.comment)
        try {
          val stmt = connection.createStatement()
          stmt.execute(sql)
          stmt.close()
          val stepEndNanos = System.nanoTime()
          callback.onStepCompleted(idx, sql, (stepEndNanos - stepStartNanos).nanos)
          results =
            StepResult(idx, sql, step.comment, StepStatus.Success, (stepEndNanos - stepStartNanos).nanos) :: results
        } catch {
          case e: SQLException =>
            val stepEndNanos = System.nanoTime()
            callback.onStepFailed(idx, sql, e)
            results = StepResult(
              idx,
              sql,
              step.comment,
              StepStatus.Failed,
              (stepEndNanos - stepStartNanos).nanos,
              Some(e)
            ) :: results
            failed = true
        }
      }
    }

    results.reverse
  }

  private def rollbackExecutedSteps(
    connection: Connection,
    executedSteps: List[(Int, MigrationStep[DT, CO, TO], Seq[String])],
    callback: ExecutionCallback
  ): Unit = {
    for (idx, step, _) <- executedSteps do {
      step.reverse.foreach { reverseStmt =>
        val reverseSqls = renderer.render(reverseStmt)
        for sql <- reverseSqls do {
          callback.onRollbackStarting(idx, sql)
          try {
            val stmt = connection.createStatement()
            stmt.execute(sql)
            stmt.close()
            callback.onRollbackCompleted(idx, sql)
          } catch {
            case _: SQLException => ()
          }
        }
      }
    }
  }

  def executeDryRun(steps: Seq[MigrationStep[DT, CO, TO]]): DryRunResult = {
    val statements = steps.flatMap { step =>
      val sqls = renderer.render(step.statement)
      val reverseSqls = step.reverse.map(r => renderer.render(r)).getOrElse(Seq.empty)
      sqls.zipWithIndex.map { case (sql, i) =>
        DryRunStatement(sql, step.comment, reverseSqls.lift(i))
      }
    }
    DryRunResult(statements)
  }
}
