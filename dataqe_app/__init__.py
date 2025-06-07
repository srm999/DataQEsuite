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

    return app
