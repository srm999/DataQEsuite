from dataqe_app import db
from dataqe_app.models import TestCase, User
from dataqe_app.executions.service import ExecutionService
import uuid


def run_scheduled_test(test_case_id: int) -> None:
    """Run a scheduled test case within an application context."""
    from flask import current_app

    with current_app.app_context():
        test_case = TestCase.query.get(test_case_id)
        if test_case and test_case.test_yn == 'Y':
            system_user = User.query.filter_by(username="system").first()
            if not system_user:
                system_user = User(
                    username="system",
                    email="system@dataqe.local",
                    is_admin=True,
                )
                system_user.set_password(str(uuid.uuid4()))
                db.session.add(system_user)
                db.session.commit()

            service = ExecutionService()
            service.run_test_case(test_case_id, executed_by=system_user.id)
