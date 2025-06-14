import sys
import types
from flask import Blueprint

# Stub auth blueprint
auth_module = types.ModuleType('dataqe_app.auth.routes')
auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login')
def login():
    return 'login'

auth_module.auth_bp = auth_bp
sys.modules.setdefault('dataqe_app.auth', types.ModuleType('dataqe_app.auth'))
sys.modules['dataqe_app.auth.routes'] = auth_module

# Stub DataQEBridge to avoid heavy imports
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

# Prevent the background scheduler from starting during tests
import apscheduler.schedulers.background
apscheduler.schedulers.background.BackgroundScheduler.start = lambda self, *a, **k: None

from dataqe_app import create_app, db, login_manager

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def login(client, user_id):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user_id)

from dataqe_app.models import Project, User, TestCase as TestCaseModel

def test_project_detail_page():
    app = create_app()



    @app.route('/connections/new/<int:project_id>')
    def new_connection(project_id):
        return 'new connection'


    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Demo Project', description='desc')
        db.session.add(project)
        db.session.commit()
        pid = project.id

    with app.test_client() as client:
        response = client.get(f'/projects/{pid}')
        assert response.status_code == 200
        assert b'Demo Project' in response.data
        assert b'Delete Project' in response.data
        assert b'Test Cases' in response.data
        assert b'Create your first test case' in response.data



def test_add_member_route():
    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Team Project')
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        db.session.add_all([project, user])
        db.session.commit()
        pid = project.id
        uid = user.id

    with app.test_client() as client:
        resp = client.post(f'/projects/{pid}/add_member', data={'user_id': uid}, follow_redirects=True)
        assert resp.status_code == 200
        with app.app_context():
            assert user in Project.query.get(pid).users


def test_new_connection_route():
    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Conn Project')
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add_all([project, user])
        db.session.commit()
        pid = project.id
        uid = user.id

    with app.test_client() as client:
        login(client, uid)
        resp = client.get(f'/connections/new/{pid}')
        assert resp.status_code == 200
        resp = client.post(
            f'/connections/new/{pid}',
            data={'name': 'conn', 'server': 's', 'database': 'd'},
            follow_redirects=True,
        )
        assert resp.status_code == 200


def test_delete_project_route():
    app = create_app()

    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Delete Me')
        db.session.add(project)
        db.session.commit()
        pid = project.id

    with app.test_client() as client:
        resp = client.post(f'/projects/{pid}/delete', follow_redirects=True)
        assert resp.status_code == 200
        with app.app_context():
            assert Project.query.get(pid) is None


def test_project_detail_shows_test_cases():
    app = create_app()

    @app.route('/testcase/new', endpoint='new_testcase')
    def new_testcase():
        return 'new'

    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Demo', folder_path='fp')
        db.session.add(project)
        db.session.commit()
        tc = TestCaseModel(tcid='TC1', table_name='tbl', test_type='CCD', project_id=project.id)
        db.session.add(tc)
        db.session.commit()
        pid = project.id

    with app.test_client() as client:
        resp = client.get(f'/projects/{pid}')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert 'Test Cases' in html
        assert 'TC1' in html


def test_projects_page_user_count():
    app = create_app()

    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='UserCount')
        u1 = User(username='u1', email='u1@example.com')
        u1.set_password('pwd')
        u2 = User(username='u2', email='u2@example.com')
        u2.set_password('pwd')
        project.users.append(u1)
        project.users.append(u2)
        db.session.add_all([project, u1, u2])
        db.session.commit()
        uid = u1.id

    with app.test_client() as client:
        login(client, uid)
        resp = client.get('/projects')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert '<th>Users</th>' in html
        assert '2' in html



def test_projects_link_in_nav_for_user():
    app = create_app()

    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='NavProj')
        user = User(username='nav', email='nav@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add_all([project, user])
        db.session.commit()
        uid = user.id

    with app.test_client() as client:
        login(client, uid)
        resp = client.get('/results-dashboard')
        assert resp.status_code == 200
        assert b'Projects' in resp.data

