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

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

def login(client, user_id):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user_id)

from dataqe_app.models import Project, Team, User


def test_team_detail_page():
    app = create_app()

    @app.route('/testcase/new', endpoint='new_testcase')
    def new_testcase():
        return 'new'

    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Demo')
        team = Team(name='Alpha')
        db.session.add_all([project, team])
        db.session.commit()
        project.team_id = team.id
        user = User(username='u1', email='u1@example.com')
        user.set_password('pass')
        user.team_id = team.id
        db.session.add(user)
        db.session.commit()
        tid = team.id

    with app.test_client() as client:
        resp = client.get(f'/teams/{tid}')
        assert resp.status_code == 200
        assert b'Alpha' in resp.data
        assert b'u1@example.com' in resp.data


def test_available_users_listed():
    app = create_app()

    @app.route('/testcase/new', endpoint='new_testcase')
    def new_testcase():
        return 'new'

    with app.app_context():
        db.drop_all()
        db.create_all()
        team = Team(name='Delta')
        db.session.add(team)
        project = Project(name='Demo')
        db.session.add(project)
        admin = User(username='admin', email='a@example.com', is_admin=True)
        admin.set_password('pwd')
        db.session.add(admin)
        db.session.commit()
        u1 = User(username='m1', email='m1@example.com')
        u1.set_password('pwd')
        u1.team_id = team.id
        u2 = User(username='m2', email='m2@example.com')
        u2.set_password('pwd')
        u3 = User(username='m3', email='m3@example.com')
        u3.set_password('pwd')
        db.session.add_all([u1, u2, u3])
        db.session.commit()
        project.team_id = team.id
        db.session.commit()
        tid = team.id
        admin_id = admin.id

    with app.test_client() as client:
        login(client, admin_id)
        resp = client.get(f'/teams/{tid}')
        assert resp.status_code == 200
        html = resp.data.decode()
        options = "".join(line.strip() for line in html.splitlines() if "<option" in line)
        assert "m2@example.com" in options
        assert "m3@example.com" in options
        assert "m1@example.com" not in options


def test_add_member_route():
    app = create_app()

    @app.route('/testcase/new', endpoint='new_testcase')
    def new_testcase():
        return 'new'

    with app.app_context():
        db.drop_all()
        db.create_all()
        team = Team(name='Beta')
        user = User(username='u2', email='u2@example.com')
        user.set_password('pwd')
        db.session.add_all([team, user])
        db.session.commit()
        tid = team.id
        uid = user.id

    with app.test_client() as client:
        resp = client.post(f'/teams/{tid}/add_member', data={'user_id': uid}, follow_redirects=True)
        assert resp.status_code == 200
        with app.app_context():
            assert User.query.get(uid).team_id == tid


def test_remove_member_route():
    app = create_app()

    @app.route('/testcase/new', endpoint='new_testcase')
    def new_testcase():
        return 'new'

    with app.app_context():
        db.create_all()
        team = Team(name='Gamma')
        user = User(username='u3', email='u3@example.com')
        user.set_password('pwd3')
        db.session.add_all([team, user])
        db.session.commit()
        user.team_id = team.id
        db.session.commit()
        tid = team.id
        uid = user.id

    with app.test_client() as client:
        resp = client.post(f'/teams/{tid}/remove_member/{uid}', follow_redirects=True)
        assert resp.status_code == 200
        with app.app_context():
            assert User.query.get(uid).team_id is None


def test_edit_user_route():
    app = create_app()

    @app.route('/testcase/new', endpoint='new_testcase')
    def new_testcase():
        return 'new'

    with app.app_context():
        db.drop_all()
        db.create_all()
        user = User(username='old', email='old@example.com')
        user.set_password('pwd')
        db.session.add(user)
        db.session.commit()
        uid = user.id

    with app.test_client() as client:
        resp = client.post(
            f'/users/{uid}/edit',
            data={'username': 'new', 'email': 'new@example.com', 'password': 'newpwd', 'team_id': '', 'is_admin': ''},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        with app.app_context():
            assert User.query.get(uid).username == 'new'
