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

from dataqe_app.models import Project, Connection, User


def test_connection_creation():
    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        project = Project(name='Conn Save Project')
        user = User(username='u', email='u@example.com')
        user.set_password('pwd')
        project.users.append(user)
        db.session.add_all([project, user])
        db.session.commit()
        pid = project.id
        uid = user.id

    with app.test_client() as client:
        login(client, uid)
        resp = client.post(
            f'/connections/new/{pid}',
            data={
                'name': 'TestConn',
                'server': 'srv',
                'database': 'db'
            },
            follow_redirects=True,
        )
        assert resp.status_code == 200

    with app.app_context():
        conn = Connection.query.filter_by(name='TestConn').order_by(Connection.id.desc()).first()
        assert conn is not None
        assert conn.project_id == pid
        assert conn.server == 'srv'
        assert conn.database == 'db'
