from datetime import datetime
from typing import Optional

from flask import current_app

from dataqe_app import db, scheduler
from dataqe_app.models import TestCase, TestExecution, TestMismatch
from dataqe_app.bridge.dataqe_bridge import DataQEBridge


class ExecutionService:
    """Service for executing test cases and recording results."""

    def __init__(self, bridge: Optional[DataQEBridge] = None):
        self.bridge = bridge or DataQEBridge()

    def run_test_case(self, test_case_id: int, executed_by: Optional[int] = None) -> TestExecution:
        """Execute a test case immediately and store the result."""
        test_case = TestCase.query.get(test_case_id)
        if not test_case:
            raise ValueError("Test case not found")

        execution = TestExecution(
            test_case_id=test_case_id,
            status="PENDING",
            executed_by=executed_by,
        )
        db.session.add(execution)
        db.session.commit()

        try:
            result = self.bridge.execute_test_case(test_case, execution)
            mismatches = result.get("mismatches", [])
            for m in mismatches:
                mismatch = TestMismatch(
                    execution_id=execution.id,
                    row_identifier=m.get("row_id"),
                    column_name=m.get("column"),
                    source_value=m.get("source_value"),
                    target_value=m.get("target_value"),
                    mismatch_type=m.get("type"),
                )
                db.session.add(mismatch)

            execution.status = result.get("status", "ERROR")
            execution.records_compared = result.get("records_compared", 0)
            execution.mismatches_found = result.get("mismatches_found", len(mismatches))
            execution.log_file = result.get("log_file")
            execution.error_message = result.get("error_message")
        except Exception as exc:  # pragma: no cover - defensive
            current_app.logger.error(f"Execution failed: {exc}")
            execution.status = "ERROR"
            execution.error_message = str(exc)
        finally:
            execution.end_time = datetime.utcnow()
            if execution.execution_time:
                execution.duration = (
                    execution.end_time - execution.execution_time
                ).total_seconds()
            db.session.commit()

        return execution

    def schedule_test_case(self, test_case_id: int, run_time: datetime) -> None:
        """Schedule a test case for later execution."""
        from dataqe_app.utils.helpers import run_scheduled_test

        scheduler.add_job(
            func=run_scheduled_test,
            trigger="date",
            run_date=run_time,
            args=[test_case_id],
            replace_existing=False,
        )
