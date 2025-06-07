from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_mail import Mail
from apscheduler.schedulers.background import BackgroundScheduler
import os

# Initialize extensions
db = SQLAlchemy()
login_manager = LoginManager()
mail = Mail()
background_scheduler = BackgroundScheduler()



def create_app():
    app = Flask(__name__)

    # Config
    app.config['SECRET_KEY'] = 'dev-key-for-testing'
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dataqe.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['SQL_FOLDER'] = os.path.join('static', 'sql_files')
    app.config['DEBUG'] = True

    # Email config
    app.config['MAIL_SERVER'] = 'smtp.gmail.com'
    app.config['MAIL_PORT'] = 587
    app.config['MAIL_USE_TLS'] = True
    app.config['MAIL_USERNAME'] = 'your-email@gmail.com'
    app.config['MAIL_PASSWORD'] = 'your-app-password'
    app.config['MAIL_DEFAULT_SENDER'] = 'DataQE Suite <your-email@gmail.com>'

    # Ensure directories exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['SQL_FOLDER'], exist_ok=True)

    # Initialize extensions
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    mail.init_app(app)
    background_scheduler.start()

    # Register Blueprints
    from dataqe_app.auth.routes import auth_bp
    from dataqe_app.testcases.routes import testcases_bp
    from dataqe_app.executions.routes import executions_bp
    from dataqe_app.scheduler.routes import scheduler_bp
    from dataqe_app.default_route import main_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(testcases_bp)
    app.register_blueprint(executions_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(main_bp)

    @app.cli.command("init-db")
    def init_db_command():
        """Initialize the database."""
        db.create_all()

        from dataqe_app.models import User
        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin', email='admin@example.com', is_admin=True)
            admin.set_password('admin')
            db.session.add(admin)
            db.session.commit()
            print("Admin user created.")

        print("Database initialized.")

    from dataqe_app.models import Project

    @app.route('/projects', endpoint='main.projects')
    def placeholder_projects():
        projects = Project.query.all()
        return render_template("projects.html", projects=projects)

    @app.route('/projects/new', methods=['GET', 'POST'], endpoint='main.new_project')
    def placeholder_new_project():
        if request.method == 'POST':
            name = request.form.get("name")
            description = request.form.get("description")
            if name:
                new_project = Project(name=name, description=description)
                db.session.add(new_project)
                db.session.commit()
                return redirect(url_for('main.projects'))
        return render_template("project_new.html")

    @app.route('/users', endpoint='main.users')
    def placeholder_users():
        return render_template("user_dashboard.html")

    @app.route('/dashboard', endpoint='main.dashboard')
    @app.route('/dashboard-legacy', endpoint='dashboard')
    def placeholder_dashboard():
        return render_template("admin_dashboard.html")

    @app.route('/results-dashboard', endpoint='results_dashboard')
    def placeholder_results_dashboard():
        return render_template("results_dashboard.html")

    # Placeholder routes for template endpoints
    @app.route('/project/<int:project_id>', endpoint='project_detail')
    @app.route('/project/<int:project_id>', endpoint='main.project_detail')
    def placeholder_project_detail(project_id):
        return f"Project detail placeholder for {project_id}"

    @app.route('/team/<int:team_id>', endpoint='team_detail')
    def placeholder_team_detail(team_id):
        return f"Team detail placeholder for {team_id}"

    @app.route('/testcase/<int:testcase_id>', endpoint='testcase_detail')
    @app.route('/testcase/<int:testcase_id>', endpoint='testcases.testcase_detail')
    def placeholder_testcase_detail(testcase_id):
        return f"Testcase detail placeholder for {testcase_id}"

    @app.route('/user/<int:user_id>/edit', methods=['GET', 'POST'], endpoint='edit_user')
    @app.route('/user/<int:user_id>/edit', methods=['GET', 'POST'], endpoint='main.edit_user')
    def placeholder_edit_user(user_id):
        return f"Edit user placeholder for {user_id}"

    @app.route('/users/new', methods=['GET', 'POST'], endpoint='new_user')
    @app.route('/users/new', methods=['GET', 'POST'], endpoint='main.new_user')
    def placeholder_new_user():
        return "New user placeholder"

    @app.route('/project/<int:project_id>/teams/new', methods=['GET', 'POST'], endpoint='new_team')
    def placeholder_new_team(project_id):
        return f"New team placeholder for project {project_id}"

    @app.route('/project/<int:project_id>/connections/new', methods=['GET', 'POST'], endpoint='new_connection')
    def placeholder_new_connection(project_id):
        return f"New connection placeholder for project {project_id}"

    @app.route('/team/<int:team_id>/members/add', methods=['POST'], endpoint='add_team_member')
    def placeholder_add_team_member(team_id):
        return f"Add team member placeholder for team {team_id}"

    @app.route('/team/<int:team_id>/members/<int:user_id>/remove', methods=['POST'], endpoint='remove_team_member')
    def placeholder_remove_team_member(team_id, user_id):
        return f"Remove team member placeholder for team {team_id} user {user_id}"

    @app.route('/testcase/<int:testcase_id>/download/<source_or_target>', endpoint='download_testcase_file')
    def placeholder_download_testcase_file(testcase_id, source_or_target):
        return f"Download {source_or_target} file placeholder for testcase {testcase_id}"

    @app.route('/execution/<int:execution_id>/download_log_file', endpoint='download_log_file')
    def placeholder_download_log_file(execution_id):
        return f"Download log file placeholder for execution {execution_id}"

    @app.route('/testcase/<int:test_case_id>/execute', methods=['POST'], endpoint='execute_test')
    def placeholder_execute_test(test_case_id):
        return f"Execute test placeholder for testcase {test_case_id}"

    @app.route('/testcase/<int:testcase_id>/execute/ui', endpoint='execute_testcase_ui')
    def placeholder_execute_testcase_ui(testcase_id):
        return f"Execute testcase UI placeholder for {testcase_id}"

    return app
