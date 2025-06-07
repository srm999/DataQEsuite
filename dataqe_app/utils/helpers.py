from dataqe_app import db
from dataqe_app.models import TestExecution, TestMismatch, TestCase, User
from datetime import datetime
from dataqe_app.bridge.dataqe_bridge import DataQEBridge
import uuid
import os

dataqa_bridge = DataQEBridge()

def run_scheduled_test(test_case_id):
    """Run a scheduled test"""
    from flask import current_app
    with current_app.app_context():
        test_case = TestCase.query.get(test_case_id)
        if test_case and test_case.test_yn == 'Y':
            system_user = User.query.filter_by(username='system').first()
            if not system_user:
                system_user = User(username='system', email='system@dataqe.local', is_admin=True)
                system_user.set_password(str(uuid.uuid4()))
                db.session.add(system_user)
                db.session.commit()

            execution = TestExecution(
                test_case_id=test_case_id,
                status='PENDING',
                executed_by=system_user.id
            )
            db.session.add(execution)
            db.session.commit()

            result = execute_test_case_logic(test_case, execution)

            execution.end_time = datetime.utcnow()
            execution.duration = (execution.end_time - execution.execution_time).total_seconds()
            execution.status = result['status']
            execution.records_compared = result.get('records_compared', 0)
            execution.mismatches_found = result.get('mismatches_found', 0)

            if result['status'] == 'FAILED' and test_case.team:
                recipients = [user.email for user in test_case.team.users if user.email]
                # send_test_failure_notification(execution, recipients)  # Optional email integration

            db.session.commit()

def execute_test_case_logic(test_case, execution):
    """Execute test case using the data validation framework"""
    try:
        result = dataqa_bridge.execute_test_case(test_case, execution)

        if result['status'] == 'FAILED' and result.get('log_file'):
            try:
                import pandas as pd
                mismatch_df = pd.read_excel(result['log_file'], sheet_name='Mismatch Analysis')
                mismatches = []
                for idx, row in mismatch_df.iterrows():
                    mismatch = {
                        'row_id': str(idx),
                        'column': row.get('Column', 'Unknown'),
                        'source_value': str(row.get('Source_Value', '')),
                        'target_value': str(row.get('Target_Value', '')),
                        'type': row.get('__mismatch_type', 'VALUE_MISMATCH')
                    }
                    mismatches.append(mismatch)
                result['mismatches'] = mismatches[:100]
            except Exception as e:
                print(f"Error reading mismatches: {e}")
                result['mismatches'] = []

        return result

    except Exception as e:
        return {
            'status': 'ERROR',
            'error_message': str(e),
            'records_compared': 0,
            'mismatches_found': 0,
            'mismatches': []
        }
