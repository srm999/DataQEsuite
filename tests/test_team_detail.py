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
    return None

from dataqe_app.models import Project, Team, User


def test_team_detail_page():
    app = create_app()
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


def test_add_member_route():
    app = create_app()
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
