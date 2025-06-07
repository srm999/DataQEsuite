from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from dataqe_app import db
from dataqe_app.models import TestCase, ScheduledTest
from apscheduler.triggers.cron import CronTrigger
from dataqe_app.utils.helpers import run_scheduled_test
import uuid

scheduler_bp = Blueprint('scheduler', __name__)

@scheduler_bp.route('/schedule/create/<int:test_case_id>', methods=['GET', 'POST'])
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
