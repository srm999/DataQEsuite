from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from flask_login import login_required, current_user
from sqlalchemy import func
from dataqe_app import db
from dataqe_app.models import TestCase, TestExecution, TestMismatch
import os

executions_bp = Blueprint('executions', __name__)

@executions_bp.route('/execution/<int:execution_id>')
@login_required
def execution_detail(execution_id):
    execution = TestExecution.query.get_or_404(execution_id)
    if not current_user.is_admin and current_user.team_id != execution.test_case.team_id:
        flash('Access denied')
        return redirect(url_for('dashboard'))

    mismatches = TestMismatch.query.filter_by(execution_id=execution_id).all()

    log_content = None
    if execution.log_file and os.path.exists(execution.log_file):
        try:
            with open(execution.log_file, 'r') as f:
                log_content = f.read()
        except:
            log_content = "Error reading log file"

    return render_template('execution_detail.html', execution=execution, mismatches=mismatches, log_content=log_content)


@executions_bp.route('/executions')
@login_required
def execution_history():
    page = request.args.get('page', 1, type=int)
    if current_user.is_admin:
        executions = TestExecution.query.order_by(TestExecution.execution_time.desc()).paginate(page=page, per_page=20)
    else:
        executions = TestExecution.query.join(TestCase).filter(
            TestCase.team_id == current_user.team_id
        ).order_by(TestExecution.execution_time.desc()).paginate(page=page, per_page=20)

    return render_template('execution_history.html', executions=executions)


@executions_bp.route('/results-dashboard')
@login_required
def results_dashboard():
    if current_user.is_admin:
        total_executions = TestExecution.query.count()
        passed_executions = TestExecution.query.filter_by(status='PASSED').count()
        failed_executions = TestExecution.query.filter_by(status='FAILED').count()
        error_executions = TestExecution.query.filter_by(status='ERROR').count()
        recent_executions = TestExecution.query.order_by(TestExecution.execution_time.desc()).limit(10).all()
        problem_tests = db.session.query(
            TestCase,
            func.count(TestExecution.id).label('failure_count')
        ).join(TestExecution).filter(
            TestExecution.status == 'FAILED'
        ).group_by(TestCase).order_by(func.count(TestExecution.id).desc()).limit(5).all()
    else:
        total_executions = TestExecution.query.join(TestCase).filter(TestCase.team_id == current_user.team_id).count()
        passed_executions = TestExecution.query.join(TestCase).filter(TestCase.team_id == current_user.team_id, TestExecution.status == 'PASSED').count()
        failed_executions = TestExecution.query.join(TestCase).filter(TestCase.team_id == current_user.team_id, TestExecution.status == 'FAILED').count()
        error_executions = TestExecution.query.join(TestCase).filter(TestCase.team_id == current_user.team_id, TestExecution.status == 'ERROR').count()
        recent_executions = TestExecution.query.join(TestCase).filter(TestCase.team_id == current_user.team_id).order_by(TestExecution.execution_time.desc()).limit(10).all()
        problem_tests = db.session.query(
            TestCase,
            func.count(TestExecution.id).label('failure_count')
        ).join(TestExecution).filter(TestCase.team_id == current_user.team_id, TestExecution.status == 'FAILED').group_by(TestCase).order_by(func.count(TestExecution.id).desc()).limit(5).all()

    return render_template('results_dashboard.html',
                           total_executions=total_executions,
                           passed_executions=passed_executions,
                           failed_executions=failed_executions,
                           error_executions=error_executions,
                           recent_executions=recent_executions,
                           problem_tests=problem_tests)


@executions_bp.route('/execution/<int:execution_id>/download_log')
@login_required
def download_log(execution_id):
    execution = TestExecution.query.get_or_404(execution_id)
    if not current_user.is_admin and current_user.team_id != execution.test_case.team_id:
        flash('Access denied')
        return redirect(url_for('dashboard'))

    if not execution.log_file or not os.path.exists(execution.log_file):
        flash('Log file not found', 'error')
        return redirect(url_for('executions.execution_detail', execution_id=execution_id))

    filename = f"{execution.test_case.tcid}_execution_{execution_id}_{execution.execution_time.strftime('%Y%m%d')}.xlsx"

    if execution.log_file.endswith('.xlsx'):
        return send_file(
            execution.log_file,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        return send_file(
            execution.log_file,
            as_attachment=True,
            download_name=filename.replace('.xlsx', '.txt'),
            mimetype='text/plain'
        )
