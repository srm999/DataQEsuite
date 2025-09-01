import sys
import types
import os
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
        return {"status": "SUCCESS"}
bridge_module.DataQEBridge = DataQEBridge
sys.modules.setdefault('dataqe_app.bridge', types.ModuleType('dataqe_app.bridge'))
sys.modules['dataqe_app.bridge.dataqe_bridge'] = bridge_module

import apscheduler.schedulers.background
apscheduler.schedulers.background.BackgroundScheduler.start = lambda self, *a, **k: None

from dataqe_app import create_app, db, login_manager
from dataqe_app.models import Project, User, Connection, TestCase as TestCaseModel


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def login(client, user_id):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user_id)


def setup_project(tmp_path):
    """Create project, connections and user for tests."""
    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        project_folder = tmp_path / "proj"
        project_folder.mkdir(parents=True)
        (project_folder / "input").mkdir()
        project = Project(name='Demo', folder_path=str(project_folder))
        db.session.add(project)
        db.session.commit()
        conn1 = Connection(name='SrcConn', project_id=project.id)
        conn2 = Connection(name='TgtConn', project_id=project.id)
        db.session.add_all([conn1, conn2])
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add(user)
        db.session.commit()
        uid = user.id
        pid = project.id
    return app, project_folder, uid, pid


def test_new_testcase_route_saves_queries(tmp_path):
    app, project_folder, uid, pid = setup_project(tmp_path)
    with app.test_client() as client:
        login(client, uid)
        resp = client.post(
            f'/testcase/new?project_id={pid}',
            data={
                'tcid': 'TC1',
                'tc_name': 'Test',
                'table_name': 'tbl',
                'test_type': 'CCD_Validation',
                'delimiter': ',',
                'pk_columns': 'id',
                'filters': '',
                'src_input_type': 'query',
                'src_query': 'select 1',
                'tgt_input_type': 'query',
                'tgt_query': 'select 2'
            },
            follow_redirects=True
        )
        assert resp.status_code == 200
    with app.app_context():
        tc = TestCaseModel.query.filter_by(tcid='TC1').first()
        assert tc is not None
        src_path = project_folder / 'input' / f'{tc.tcid}_src.sql'
        tgt_path = project_folder / 'input' / f'{tc.tcid}_tgt.sql'
        assert src_path.exists() and src_path.read_text() == 'select 1'
        assert tgt_path.exists() and tgt_path.read_text() == 'select 2'


def test_new_testcase_requires_pk(tmp_path):
    app, project_folder, uid, pid = setup_project(tmp_path)
    with app.test_client() as client:
        login(client, uid)
        resp = client.post(
            f'/testcase/new?project_id={pid}',
            data={
                'tcid': 'TC2',
                'tc_name': 'Test',
                'table_name': 'tbl',
                'test_type': 'CCD_Validation',
                'delimiter': ',',
                'src_input_type': 'query',
                'src_query': 'select 1',
                'tgt_input_type': 'query',
                'tgt_query': 'select 2'
            },
            follow_redirects=True
        )
        assert resp.status_code == 200
    with app.app_context():
        assert TestCaseModel.query.filter_by(tcid='TC2').first() is None


def test_new_testcase_requires_name(tmp_path):
    app, project_folder, uid, pid = setup_project(tmp_path)
    with app.test_client() as client:
        login(client, uid)
        resp = client.post(
            f'/testcase/new?project_id={pid}',
            data={
                'tcid': 'TC3',
                'table_name': 'tbl',
                'test_type': 'CCD_Validation',
                'pk_columns': 'id',
                'src_input_type': 'query',
                'src_query': 'select 1',
                'tgt_input_type': 'query',
                'tgt_query': 'select 2'
            },
            follow_redirects=True
        )
        assert resp.status_code == 200
    with app.app_context():
        assert TestCaseModel.query.filter_by(tcid='TC3').first() is None


def test_edit_testcase_overwrites_sql(tmp_path):
    app, project_folder, uid, pid = setup_project(tmp_path)
    with app.app_context():
        (project_folder / 'input' / 'TC1_src.sql').write_text('old src')
        (project_folder / 'input' / 'TC1_tgt.sql').write_text('old tgt')
        tc = TestCaseModel(
            tcid='TC1',
            tc_name='Old',
            table_name='tbl',
            test_type='CCD_Validation',
            pk_columns='id',
            project_id=pid,
            src_data_file='TC1_src.sql',
            tgt_data_file='TC1_tgt.sql'
        )
        db.session.add(tc)
        db.session.commit()
        tcid = tc.id
    with app.test_client() as client:
        login(client, uid)
        resp = client.post(
            f'/testcase/{tcid}/edit',
            data={
                'tcid': 'TC1',
                'tc_name': 'New',
                'table_name': 'tbl2',
                'test_type': 'CCD_Validation',
                'pk_columns': 'id',
                'src_input_type': 'query',
                'src_query': 'new src',
                'tgt_input_type': 'query',
                'tgt_query': 'new tgt'
            },
            follow_redirects=True
        )
        assert resp.status_code == 200
    with app.app_context():
        updated = TestCaseModel.query.get(tcid)
        src_path = project_folder / 'input' / f'{updated.tcid}_src.sql'
        tgt_path = project_folder / 'input' / f'{updated.tcid}_tgt.sql'
        assert src_path.exists() and src_path.read_text() == 'new src'
        assert tgt_path.exists() and tgt_path.read_text() == 'new tgt'


def test_testcase_detail_route(tmp_path):
    app, project_folder, uid, pid = setup_project(tmp_path)
    with app.app_context():
        tc = TestCaseModel(
            tcid='TC2',
            table_name='tbl',
            test_type='CCD_Validation',
            project_id=pid
        )
        db.session.add(tc)
        db.session.commit()
        tcid = tc.id
    with app.test_client() as client:
        login(client, uid)
        resp = client.get(f'/testcase/{tcid}')
        assert resp.status_code == 200
        assert b'Test Case Details' in resp.data
        assert b'TC2' in resp.data
