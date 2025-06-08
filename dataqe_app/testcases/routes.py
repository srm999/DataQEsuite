from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from dataqe_app import db
from dataqe_app.models import TestCase, ScheduledTest, TestExecution, TestMismatch, User
from dataqe_app.utils.helpers import run_scheduled_test
from datetime import datetime
import os
import uuid
from apscheduler.triggers.cron import CronTrigger


testcases_bp = Blueprint('testcases', __name__)

@testcases_bp.route('/testcase/<int:testcase_id>/delete', methods=['POST'])
@login_required
def delete_testcase(testcase_id):
    test_case = TestCase.query.get_or_404(testcase_id)

    if not current_user.is_admin and current_user.team_id != test_case.team_id:
        flash('Access denied', 'error')
        return redirect(url_for('dashboard'))

    team_id = test_case.team_id
    tcid = test_case.tcid
    project = test_case.team.project
    project_input_folder = os.path.join(project.folder_path, 'input')

    # Delete source and target files if they exist
    for data_file in [test_case.src_data_file, test_case.tgt_data_file]:
        if data_file:
            file_path = os.path.join(project_input_folder, data_file)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")

    # Delete schedules
    ScheduledTest.query.filter_by(test_case_id=testcase_id).delete()

    # Delete executions and mismatches
    for execution in TestExecution.query.filter_by(test_case_id=testcase_id).all():
        TestMismatch.query.filter_by(execution_id=execution.id).delete()
        if execution.log_file and os.path.exists(execution.log_file):
            try:
                os.remove(execution.log_file)
            except Exception as e:
                print(f"Error deleting log file: {e}")

    TestExecution.query.filter_by(test_case_id=testcase_id).delete()
    db.session.delete(test_case)
    db.session.commit()

    flash(f'Test case {tcid} deleted successfully', 'success')
    return redirect(url_for('dashboard') if not current_user.is_admin else url_for('team_detail', team_id=team_id))


@testcases_bp.route('/schedule/create/<int:test_case_id>', methods=['GET', 'POST'])
@login_required
def create_schedule(test_case_id):
    test_case = TestCase.query.get_or_404(test_case_id)

    if not current_user.is_admin and current_user.team_id != test_case.team_id:
        flash('Access denied')
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        schedule_type = request.form.get('schedule_type')
        schedule_time = request.form.get('schedule_time')
        schedule_days = request.form.get('schedule_days', '')

        schedule = ScheduledTest(
            test_case_id=test_case_id,
            schedule_type=schedule_type,
            schedule_time=schedule_time,
            schedule_days=schedule_days,
            created_by=current_user.id
        )

        hour, minute = schedule_time.split(':')

        if schedule_type == 'DAILY':
            trigger = CronTrigger(hour=int(hour), minute=int(minute))
        elif schedule_type == 'WEEKLY':
            days = schedule_days.split(',')
            trigger = CronTrigger(day_of_week=','.join(days), hour=int(hour), minute=int(minute))

        from dataqe_app import scheduler
        job_id = f'test_{test_case_id}_{uuid.uuid4().hex}'
        scheduler.add_job(
            func=run_scheduled_test,
            trigger=trigger,
            args=[test_case_id],
            id=job_id,
            replace_existing=True
        )

        db.session.add(schedule)
        db.session.commit()

        flash('Schedule created successfully')
        return redirect(url_for('testcase_detail', testcase_id=test_case_id))

    return render_template('create_schedule.html', test_case=test_case)


@testcases_bp.route('/debug/last-execution')
@login_required
def debug_last_execution():
    execution = TestExecution.query.order_by(TestExecution.execution_time.desc()).first()
    if execution:
        return jsonify({
            'id': execution.id,
            'test_case_id': execution.test_case_id,
            'status': execution.status,
            'error_message': execution.error_message,
            'execution_time': execution.execution_time.isoformat() if execution.execution_time else None,
            'end_time': execution.end_time.isoformat() if execution.end_time else None
        })
    return jsonify({'error': 'No executions found'})


@testcases_bp.route('/testcase/new', methods=['GET', 'POST'], endpoint='new_testcase')
def new_testcase():
    """Placeholder for creating a new test case."""
    if request.method == 'POST':
        # For now simply acknowledge the post and redirect back
        flash('Test case creation not implemented', 'info')
        team_id = request.args.get('team_id') or request.form.get('team_id')
        if team_id:
            return redirect(url_for('team_detail', team_id=team_id))
        return redirect(url_for('dashboard'))

    return render_template('placeholder.html', title='New Test Case')


@testcases_bp.route('/testcase/<int:testcase_id>/edit', methods=['GET', 'POST'], endpoint='edit_testcase')
def edit_testcase(testcase_id):
    """Placeholder for editing a test case."""
    if request.method == 'POST':
        flash('Editing test cases is not implemented', 'info')
        return redirect(url_for('testcase_detail', testcase_id=testcase_id))

    return render_template('placeholder.html', title='Edit Test Case')
