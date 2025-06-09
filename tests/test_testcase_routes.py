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


def test_new_testcase_route(tmp_path):
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

    with app.test_client() as client:
        login(client, uid)
        # verify dropdown shows connections
        get_resp = client.get(f'/testcase/new?project_id={pid}')

        team = Team(name='Team1')
        db.session.add_all([project, team])
        db.session.commit()
        project.team_id = team.id

        conn1 = Connection(name='SrcConn', project_id=project.id)
        conn2 = Connection(name='TgtConn', project_id=project.id)
        db.session.add_all([conn1, conn2])


        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        user.team_id = team.id
        db.session.add(user)
        db.session.commit()
        uid = user.id
        tid = team.id

    with app.test_client() as client:
        login(client, uid)

        # verify dropdown shows connections
        get_resp = client.get(f'/testcase/new?team_id={tid}')
        assert get_resp.status_code == 200
        html = get_resp.data.decode()
        assert 'SrcConn' in html and 'TgtConn' in html

        resp = client.post(
            f'/testcase/new?project_id={pid}',

            data={
                'tcid': 'TC1',
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

        resp = client.post(f'/testcase/new?team_id={tid}', data={
            'tcid': 'TC1',
            'tc_name': 'Test',
            'table_name': 'tbl',
            'test_type': 'CCD_Validation',
            'delimiter': ','
        }, follow_redirects=True)


        assert resp.status_code == 200
        with app.app_context():
            tc = TestCaseModel.query.filter_by(tcid='TC1').first()
            assert tc is not None
            assert tc.project_id == pid
            src_path = project_folder / 'input' / tc.src_data_file
            tgt_path = project_folder / 'input' / tc.tgt_data_file
            assert src_path.exists() and tgt_path.exists()
            assert src_path.read_text() == 'select 1'
            assert tgt_path.read_text() == 'select 2'


def test_edit_testcase_route(tmp_path):
    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        proj_folder = tmp_path / "proj2"
        proj_folder.mkdir(parents=True)
        (proj_folder / "input").mkdir()
        project = Project(name='Demo', folder_path=str(proj_folder))
        project = Project(name='Demo', folder_path=str(proj_folder))
        db.session.add(project)
        db.session.commit()
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add(user)
        db.session.commit()
        # existing file for query
        old_query = proj_folder / 'input' / 'old.sql'
        old_query.write_text('old src')
        tc = TestCaseModel(
            tcid='TC1',
            tc_name='Old',
            table_name='tbl',
            test_type='CCD_Validation',
            project_id=project.id,
            src_data_file='old.sql'
        )
        db.session.add(tc)
        db.session.commit()
        uid = user.id
        tcid = tc.id

    with app.test_client() as client:
        login(client, uid)
        resp = client.post(
            f'/testcase/{tcid}/edit',
            data={
                'tcid': 'TC1',
                'tc_name': 'NewName',
                'table_name': 'tbl2',
                'test_type': 'CCD_Validation',
                'src_input_type': 'query',
                'src_query': 'new src',
                'tgt_input_type': 'query',
                'tgt_query': 'new tgt'
            },
            follow_redirects=True
        )

        resp = client.post(f'/testcase/{tcid}/edit', data={
            'tcid': 'TC1',
            'tc_name': 'NewName',
            'table_name': 'tbl2',
            'test_type': 'CCD_Validation'
        }, follow_redirects=True)

        assert resp.status_code == 200
        with app.app_context():
            updated = TestCaseModel.query.get(tcid)
            assert updated.tc_name == 'NewName'
            assert updated.table_name == 'tbl2'
            src_path = proj_folder / 'input' / updated.src_data_file
            tgt_path = proj_folder / 'input' / updated.tgt_data_file
            assert src_path.exists() and tgt_path.exists()
            assert src_path.read_text() == 'new src'
            assert tgt_path.read_text() == 'new tgt'
            assert not old_query.exists()


def test_testcase_detail_route(tmp_path):
    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        folder = tmp_path / "proj3"
        folder.mkdir(parents=True)
        (folder / "input").mkdir()
        project = Project(name='Demo', folder_path=str(folder))
        db.session.add(project)
        db.session.commit()
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add(user)
        tc = TestCaseModel(tcid='TC2', table_name='tbl', test_type='CCD_Validation', project_id=project.id)
        db.session.add(tc)
        db.session.commit()
        uid = user.id
        tcid = tc.id

    with app.test_client() as client:
        login(client, uid)
        resp = client.get(f'/testcase/{tcid}')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert 'Test Case Details' in html
        assert 'TC2' in html

