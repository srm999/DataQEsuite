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
from dataqe_app.models import Project


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


