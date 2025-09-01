from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from dataqe_app import db
from dataqe_app.models import TestCase, ScheduledTest, TestExecution, TestMismatch, User, Project, Team
from dataqe_app.utils.helpers import run_scheduled_test
from datetime import datetime
import os
import uuid
from werkzeug.utils import secure_filename
from apscheduler.triggers.cron import CronTrigger
from dataqe_app.testcases.forms import TestCaseForm


testcases_bp = Blueprint('testcases', __name__)


@testcases_bp.route('/testcase/<int:testcase_id>/delete', methods=['POST'])
@login_required
def delete_testcase(testcase_id):
    test_case = TestCase.query.get_or_404(testcase_id)

    if not current_user.is_admin and current_user not in test_case.project.users:
        flash('Access denied', 'error')
        return redirect(url_for('dashboard'))

    project = test_case.project
    project_input_folder = os.path.join(project.folder_path, 'input')

    for data_file in [test_case.src_data_file, test_case.tgt_data_file]:
        if data_file:
            file_path = os.path.join(project_input_folder, data_file)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")

    ScheduledTest.query.filter_by(test_case_id=testcase_id).delete()

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

    flash(f'Test case {test_case.tcid} deleted successfully', 'success')
    return redirect(url_for('dashboard') if not current_user.is_admin else url_for('projects.project_detail', project_id=project.id))


@testcases_bp.route('/schedule/create/<int:test_case_id>', methods=['GET', 'POST'])
@login_required
def create_schedule(test_case_id):
    test_case = TestCase.query.get_or_404(test_case_id)

    if not current_user.is_admin and current_user not in test_case.project.users:
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
@login_required
def new_testcase():
    """Create a new test case."""
    project_id = request.args.get('project_id') or request.form.get('project_id')
    team_id = request.args.get('team_id') or request.form.get('team_id')
    team_id = int(team_id) if team_id else None
    if not project_id and team_id:
        team = Team.query.get_or_404(team_id)
        project = team.project
        project_id = project.id
    elif project_id:
        project = Project.query.get_or_404(project_id)
    else:
        flash('Project not specified', 'error')
        return redirect(url_for('dashboard'))

    if not current_user.is_admin and current_user not in project.users:
        if project.team_id and current_user.team_id == project.team_id:
            pass
        elif team_id and current_user.team_id == team_id:
            pass
        else:
            flash('Access denied', 'error')
            return redirect(url_for('dashboard'))

    connections = project.connections
    form = TestCaseForm()

    if form.validate_on_submit():
        project_input_folder = os.path.join(project.folder_path, 'input')
        os.makedirs(project_input_folder, exist_ok=True)

        tcid = form.tcid.data


        test_case = TestCase(
            tcid=tcid,
            tc_name=tc_name,
            table_name=table_name,
            test_type=test_type,
            test_yn=test_yn,
            src_data_file=src_data_file,
            tgt_data_file=tgt_data_file,
            src_connection_id=src_conn_id,
            tgt_connection_id=tgt_conn_id,
            filters=filters,
            delimiter=delimiter,
            pk_columns=pk_columns,
            date_fields=date_fields,
            percentage_fields=percentage_fields,
            threshold_percentage=threshold_percentage,
            header_columns=header_columns,
            skip_rows=skip_rows,
            src_sheet_name=src_sheet_name,
            tgt_sheet_name=tgt_sheet_name,
            project_id=project.id,
            team_id=team_id,
            creator_id=current_user.id,
        )
        db.session.add(test_case)
        db.session.commit()

        flash('Test case created successfully', 'success')
        return redirect(url_for('projects.project_detail', project_id=project.id))
    return render_template('testcase_new.html', project=project, connections=connections)
    return redirect(url_for('dashboard'))

    project = Project.query.get_or_404(project_id)


    if not current_user.is_admin and current_user not in project.users:
        flash('Access denied', 'error')
        return redirect(url_for('dashboard'))

    connections = project.connections

    if request.method == 'POST':
        tcid = request.form.get('tcid')
        table_name = request.form.get('table_name')
        test_type = request.form.get('test_type')
        tc_name = request.form.get('tc_name')
        test_yn = 'Y' if request.form.get('test_yn') else 'N'
        src_conn_id = request.form.get('src_connection_id') or None
        tgt_conn_id = request.form.get('tgt_connection_id') or None
        delimiter = request.form.get('delimiter')
        filters = request.form.get('filters')
        pk_columns = request.form.get('pk_columns')
        date_fields = request.form.get('date_fields')
        percentage_fields = request.form.get('percentage_fields')
        threshold_percentage = request.form.get('threshold_percentage')
        header_columns = request.form.get('header_columns')
        skip_rows = request.form.get('skip_rows')
        src_sheet_name = request.form.get('src_sheet_name')
        tgt_sheet_name = request.form.get('tgt_sheet_name')
        src_input_type = request.form.get('src_input_type')
        tgt_input_type = request.form.get('tgt_input_type')
        src_query = request.form.get('src_query')
        tgt_query = request.form.get('tgt_query')

        project_input_folder = os.path.join(project.folder_path, 'input')
        os.makedirs(project_input_folder, exist_ok=True)

        src_file = request.files.get('src_file')
        tgt_file = request.files.get('tgt_file')

        src_data_file = None
        if form.src_input_type.data == 'query':
            src_filename = f"{tcid}_src.sql"
            with open(os.path.join(project_input_folder, src_filename), 'w') as f:
                f.write(form.src_query.data)
            src_data_file = src_filename
        elif form.src_file.data and form.src_file.data.filename:
            filename = f"{uuid.uuid4().hex}_{secure_filename(form.src_file.data.filename)}"
            form.src_file.data.save(os.path.join(project_input_folder, filename))
            src_data_file = filename

        tgt_data_file = None
        if form.tgt_input_type.data == 'query':
            tgt_filename = f"{tcid}_tgt.sql"
            with open(os.path.join(project_input_folder, tgt_filename), 'w') as f:
                f.write(form.tgt_query.data)
            tgt_data_file = tgt_filename
        elif form.tgt_file.data and form.tgt_file.data.filename:
            filename = f"{uuid.uuid4().hex}_{secure_filename(form.tgt_file.data.filename)}"
            form.tgt_file.data.save(os.path.join(project_input_folder, filename))
            tgt_data_file = filename

        test_case = TestCase(
            tcid=tcid,
            tc_name=form.tc_name.data,
            table_name=form.table_name.data,
            test_type=form.test_type.data,
            test_yn='Y' if form.test_yn.data else 'N',
            src_data_file=src_data_file,
            tgt_data_file=tgt_data_file,
            src_connection_id=form.src_connection_id.data or None,
            tgt_connection_id=form.tgt_connection_id.data or None,
            filters=form.filters.data,
            delimiter=form.delimiter.data,
            pk_columns=form.pk_columns.data,
            date_fields=form.date_fields.data,
            percentage_fields=form.percentage_fields.data,
            threshold_percentage=form.threshold_percentage.data,
            header_columns=form.header_columns.data,
            skip_rows=form.skip_rows.data,
            src_sheet_name=form.src_sheet_name.data,
            tgt_sheet_name=form.tgt_sheet_name.data,
            project_id=project.id,
            creator_id=current_user.id,
        )
        db.session.add(test_case)
        db.session.commit()

        flash('Test case created successfully', 'success')
        return redirect(url_for('projects.project_detail', project_id=project.id))

    return render_template('testcase_new.html', project=project, connections=connections, form=form)


@testcases_bp.route('/testcase/<int:testcase_id>/edit', methods=['GET', 'POST'], endpoint='edit_testcase')
@login_required
def edit_testcase(testcase_id):
    """Edit an existing test case."""
    test_case = TestCase.query.get_or_404(testcase_id)

    if not current_user.is_admin and current_user not in test_case.project.users:
        flash('Access denied', 'error')
        return redirect(url_for('dashboard'))

    project = test_case.project
    connections = project.connections
    project_input_folder = os.path.join(project.folder_path, 'input')
    os.makedirs(project_input_folder, exist_ok=True)

    form = TestCaseForm(obj=test_case)

    if request.method == 'GET':
        if test_case.src_data_file and test_case.src_data_file.endswith('_src.sql'):
            src_path = os.path.join(project_input_folder, test_case.src_data_file)
            if os.path.exists(src_path):
                with open(src_path) as f:
                    form.src_query.data = f.read()
            form.src_input_type.data = 'query'
        else:
            form.src_input_type.data = 'file'

        if test_case.tgt_data_file and test_case.tgt_data_file.endswith('_tgt.sql'):
            tgt_path = os.path.join(project_input_folder, test_case.tgt_data_file)
            if os.path.exists(tgt_path):
                with open(tgt_path) as f:
                    form.tgt_query.data = f.read()
            form.tgt_input_type.data = 'query'
        else:
            form.tgt_input_type.data = 'file'

    if form.validate_on_submit():
        test_case.tcid = form.tcid.data
        test_case.table_name = form.table_name.data
        test_case.test_type = form.test_type.data
        test_case.tc_name = form.tc_name.data
        test_case.test_yn = 'Y' if form.test_yn.data else 'N'
        test_case.src_connection_id = form.src_connection_id.data or None
        test_case.tgt_connection_id = form.tgt_connection_id.data or None
        test_case.delimiter = form.delimiter.data
        test_case.filters = form.filters.data
        test_case.pk_columns = form.pk_columns.data
        test_case.date_fields = form.date_fields.data
        test_case.percentage_fields = form.percentage_fields.data
        test_case.threshold_percentage = form.threshold_percentage.data
        test_case.header_columns = form.header_columns.data
        test_case.skip_rows = form.skip_rows.data
        test_case.src_sheet_name = form.src_sheet_name.data
        test_case.tgt_sheet_name = form.tgt_sheet_name.data

        tcid = form.tcid.data

        if form.src_input_type.data == 'query':
            src_filename = f"{tcid}_src.sql"
            with open(os.path.join(project_input_folder, src_filename), 'w') as f:
                f.write(form.src_query.data)
            test_case.src_data_file = src_filename
        elif form.src_file.data and form.src_file.data.filename:
            if test_case.src_data_file:
                old_path = os.path.join(project_input_folder, test_case.src_data_file)
                if os.path.exists(old_path):
                    os.remove(old_path)
            filename = f"{uuid.uuid4().hex}_{secure_filename(form.src_file.data.filename)}"
            form.src_file.data.save(os.path.join(project_input_folder, filename))
            test_case.src_data_file = filename

        if form.tgt_input_type.data == 'query':
            tgt_filename = f"{tcid}_tgt.sql"
            with open(os.path.join(project_input_folder, tgt_filename), 'w') as f:
                f.write(form.tgt_query.data)
            test_case.tgt_data_file = tgt_filename
        elif form.tgt_file.data and form.tgt_file.data.filename:
            if test_case.tgt_data_file:
                old_path = os.path.join(project_input_folder, test_case.tgt_data_file)
                if os.path.exists(old_path):
                    os.remove(old_path)
            filename = f"{uuid.uuid4().hex}_{secure_filename(form.tgt_file.data.filename)}"
            form.tgt_file.data.save(os.path.join(project_input_folder, filename))
            test_case.tgt_data_file = filename

        db.session.commit()
        flash('Test case updated successfully', 'success')
        return redirect(url_for('testcase_detail', testcase_id=test_case.id))

    src_sql = form.src_query.data if form.src_input_type.data == 'query' else None
    tgt_sql = form.tgt_query.data if form.tgt_input_type.data == 'query' else None
    return render_template('testcase_edit.html', project=project, test_case=test_case, connections=connections, form=form, src_sql=src_sql, tgt_sql=tgt_sql)


@testcases_bp.route('/testcase/<int:testcase_id>', methods=['GET'])
@login_required
def testcase_detail(testcase_id):
    """Display detailed information about a test case."""
    test_case = TestCase.query.get_or_404(testcase_id)

    if not current_user.is_admin and current_user not in test_case.project.users:
        flash('Access denied', 'error')
        return redirect(url_for('dashboard'))

    project_input_folder = os.path.join(
        test_case.project.folder_path, 'input'
    )
    src_sql = None
    tgt_sql = None
    if test_case.src_data_file:
        src_path = os.path.join(project_input_folder, test_case.src_data_file)
        if os.path.exists(src_path):
            with open(src_path) as f:
                src_sql = f.read()
    if test_case.tgt_data_file:
        tgt_path = os.path.join(project_input_folder, test_case.tgt_data_file)
        if os.path.exists(tgt_path):
            with open(tgt_path) as f:
                tgt_sql = f.read()

    sorted_executions = sorted(
        test_case.executions,
        key=lambda e: e.execution_time or datetime.min,
        reverse=True
    )

    return render_template(
        'testcase_detail.html',
        test_case=test_case,
        src_sql=src_sql,
        tgt_sql=tgt_sql,
        sorted_executions=sorted_executions[:10]
    )
