import sys
import types
from datetime import datetime

from flask import Blueprint

# Stub auth blueprint
auth_module = types.ModuleType('dataqe_app.auth.routes')
auth_bp = Blueprint('auth', __name__)
@auth_bp.route('/login')
def login():
    return 'login'
@auth_bp.route('/logout')
def logout():
    return 'logout'
auth_module.auth_bp = auth_bp
sys.modules.setdefault('dataqe_app.auth', types.ModuleType('dataqe_app.auth'))
sys.modules['dataqe_app.auth.routes'] = auth_module

# Stub DataQEBridge
bridge_module = types.ModuleType('dataqe_app.bridge.dataqe_bridge')
class DataQEBridge:
    def __init__(self, app=None):
        self.app = app
    def init_app(self, app):
        self.app = app
    def execute_test_case(self, *a, **kw):
        return {"status": "PASSED", "records_compared": 5, "mismatches": []}
bridge_module.DataQEBridge = DataQEBridge
sys.modules.setdefault('dataqe_app.bridge', types.ModuleType('dataqe_app.bridge'))
sys.modules['dataqe_app.bridge.dataqe_bridge'] = bridge_module

# Ensure ExecutionService picks up our stub
import dataqe_app.executions.service as service_module
service_module.DataQEBridge = DataQEBridge

import apscheduler.schedulers.background
apscheduler.schedulers.background.BackgroundScheduler.start = lambda self, *a, **k: None

from dataqe_app import create_app, db, scheduler
from dataqe_app.models import Project, User, TestCase, Connection, TestExecution

from dataqe_app import login_manager

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def _login(client, user_id):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user_id)


def setup_app(tmp_path):
    app = create_app()
    app.testing = True
    with app.app_context():
        db.drop_all()
        db.create_all()
        project_folder = tmp_path / "proj"
        project_folder.mkdir()
        (project_folder / "input").mkdir()
        project = Project(name='Demo', folder_path=str(project_folder))
        db.session.add(project)
        db.session.commit()
        conn = Connection(name='Src', project_id=project.id)
        db.session.add(conn)
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add(user)
        db.session.commit()
        tc = TestCase(tcid='TC1', tc_name='Test', table_name='t', test_type='Completeness',
                      project_id=project.id, test_yn='Y', src_connection_id=conn.id, tgt_connection_id=conn.id)
        db.session.add(tc)
        db.session.commit()
        return app, user.id, tc.id


def test_api_execute_creates_execution(tmp_path):
    app, uid, tc_id = setup_app(tmp_path)
    with app.test_client() as client:
        _login(client, uid)
        resp = client.post(f'/api/execute/{tc_id}')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'execution_id' in data
        with app.app_context():
            exec_rec = TestExecution.query.get(data['execution_id'])
            assert exec_rec is not None
            assert exec_rec.status == 'PASSED'
            assert exec_rec.executed_by == uid


def test_api_schedule_adds_job(tmp_path):
    app, uid, tc_id = setup_app(tmp_path)
    called = {}

    def fake_add_job(func, trigger, run_date=None, args=None, **kwargs):
        called['func'] = func
        called['trigger'] = trigger
        called['run_date'] = run_date
        called['args'] = args
        return None

    scheduler.add_job = fake_add_job

    with app.test_client() as client:
        _login(client, uid)
        run_at = datetime.utcnow().isoformat()
        resp = client.post(f'/api/schedule/{tc_id}', data={'run_at': run_at})
        assert resp.status_code == 200
        assert called['func'] is not None
        assert called['args'] == [tc_id]
