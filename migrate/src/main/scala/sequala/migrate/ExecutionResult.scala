package sequala.migrate

import java.time.Instant
import scala.concurrent.duration.Duration

case class StepResult(
  stepIndex: Int,
  sql: String,
  comment: String,
  status: StepStatus,
  executionTime: Duration,
  error: Option[Throwable] = None
)

enum StepStatus:
  case Success
  case Failed
  case Skipped
  case RolledBack

case class ExecutionResult(
  steps: Seq[StepResult],
  totalExecutionTime: Duration,
  startedAt: Instant,
  completedAt: Instant
):
  def successful: Boolean = steps.forall(_.status == StepStatus.Success)
  def failedStep: Option[StepResult] = steps.find(_.status == StepStatus.Failed)
  def executedCount: Int = steps.count(s => s.status == StepStatus.Success || s.status == StepStatus.RolledBack)

case class DryRunResult(statements: Seq[DryRunStatement])

case class DryRunStatement(sql: String, comment: String, reverseSql: Option[String])
