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
    return None

from dataqe_app.models import Project, Team




def test_project_detail_page():
    app = create_app()


    @app.route('/teams/new/<int:project_id>')
    def new_team(project_id):
        return 'new team'

    @app.route('/connections/new/<int:project_id>')
    def new_connection(project_id):
        return 'new connection'

    with app.app_context():
        db.create_all()
        project = Project(name='Demo Project', description='desc')
        db.session.add(project)
        db.session.commit()
        pid = project.id

    with app.test_client() as client:
        response = client.get(f'/projects/{pid}')
        assert response.status_code == 200
        assert b'Demo Project' in response.data



def test_new_team_route():
    app = create_app()
    with app.app_context():
        db.create_all()
        project = Project(name='Team Project')
        db.session.add(project)
        db.session.commit()
        pid = project.id

    with app.test_client() as client:
        resp = client.get(f'/teams/new/{pid}')
        assert resp.status_code == 200
        resp = client.post(f'/teams/new/{pid}', data={'name': 'Alpha'}, follow_redirects=True)
        assert resp.status_code == 200
        assert b'Alpha' in resp.data


def test_new_connection_route():
    app = create_app()
    with app.app_context():
        db.create_all()
        project = Project(name='Conn Project')
        db.session.add(project)
        db.session.commit()
        pid = project.id

    with app.test_client() as client:
        resp = client.get(f'/connections/new/{pid}')
        assert resp.status_code == 200
        resp = client.post(f'/connections/new/{pid}', data={'name': 'conn'}, follow_redirects=True)
        assert resp.status_code == 200



