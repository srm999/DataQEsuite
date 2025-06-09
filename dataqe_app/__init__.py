from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_mail import Mail
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import or_, inspect, text

                db.session.execute(text('ALTER TABLE test_case ADD COLUMN project_id INTEGER'))

# Initialize extensions
db = SQLAlchemy()
login_manager = LoginManager()
mail = Mail()
background_scheduler = BackgroundScheduler()

from dataqe_app.models import User, Project, TestCase


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

    # Perform simple migrations for older databases
    with app.app_context():
        inspector = inspect(db.engine)
        if 'test_case' in inspector.get_table_names():
            cols = [c['name'] for c in inspector.get_columns('test_case')]
            if 'project_id' not in cols:
                db.session.execute('ALTER TABLE test_case ADD COLUMN project_id INTEGER')
                db.session.commit()
        db.create_all()

    # Register Blueprints
    from dataqe_app.auth.routes import auth_bp
    from dataqe_app.testcases.routes import testcases_bp, testcase_detail
    from dataqe_app.executions.routes import executions_bp
    from dataqe_app.scheduler.routes import scheduler_bp
    from dataqe_app.default_route import main_bp
    from dataqe_app.projects.routes import projects_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(testcases_bp)
    app.register_blueprint(executions_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(projects_bp)

    # Allow templates to reference testcase_detail without blueprint prefix
    app.add_url_rule(
        '/testcase/<int:testcase_id>',
        endpoint='testcase_detail',
        view_func=testcase_detail,
    )
    app.add_url_rule(
        '/testcase/<int:testcase_id>',
        endpoint='main.testcase_detail',
        view_func=testcase_detail,
    )

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


    @app.route('/users', endpoint='main.users')
    def users():
        all_users = User.query.all()
        return render_template("users.html", users=all_users)

    @app.route('/dashboard', endpoint='main.dashboard')
    @app.route('/dashboard-legacy', endpoint='dashboard')
    def placeholder_dashboard():
        """Redirect legacy dashboard links to the results dashboard."""
        return redirect(url_for('executions.results_dashboard'))

    @app.route('/results-dashboard', endpoint='results_dashboard')
    def placeholder_results_dashboard():
        return render_template("results_dashboard.html")


    @app.route('/teams/<int:team_id>', endpoint='team_detail')
    def team_detail(team_id):
        team = Team.query.get_or_404(team_id)
        test_cases = TestCase.query.filter_by(team_id=team_id).all()
        users = team.users
        available_users = User.query.filter(
            or_(User.team_id.is_(None), User.team_id != team_id)
        ).all()
        return render_template(
            'team_detail.html',
            team=team,
            test_cases=test_cases,
            users=users,
            available_users=available_users,
        )

    @app.route('/teams/<int:team_id>/add_member', methods=['POST'], endpoint='add_team_member')
    def add_team_member(team_id):
        user_id = request.form.get('user_id')
        user = User.query.get_or_404(user_id)
        user.team_id = team_id
        db.session.commit()
        flash('Member added successfully', 'success')
        return redirect(url_for('team_detail', team_id=team_id))

    @app.route('/teams/<int:team_id>/remove_member/<int:user_id>', methods=['POST'], endpoint='remove_team_member')
    def remove_team_member(team_id, user_id):
        user = User.query.get_or_404(user_id)
        user.team_id = None
        db.session.commit()
        flash('Member removed', 'success')
        return redirect(url_for('team_detail', team_id=team_id))


    @app.route('/users/<int:user_id>/edit', methods=['GET', 'POST'], endpoint='edit_user')
    @app.route('/users/<int:user_id>/edit', methods=['GET', 'POST'], endpoint='main.edit_user')
    def edit_user(user_id):
        user = User.query.get_or_404(user_id)
        if request.method == 'POST':
            user.username = request.form.get('username')
            user.email = request.form.get('email')
            password = request.form.get('password')
            project_ids = request.form.getlist('project_ids')
            user.is_admin = bool(request.form.get('is_admin'))
            user.projects = Project.query.filter(Project.id.in_(project_ids)).all()
            if password:
                user.set_password(password)
            db.session.commit()
            return redirect(url_for('main.users'))

        projects = Project.query.all()
        return render_template('user_edit.html', user=user, projects=projects)

    @app.route('/users/new', methods=['GET', 'POST'], endpoint='new_user')
    @app.route('/users/new', methods=['GET', 'POST'], endpoint='main.new_user')
    def new_user():
        if request.method == 'POST':
            username = request.form.get('username')
            email = request.form.get('email')
            password = request.form.get('password')
            project_ids = request.form.getlist('project_ids')
            is_admin = bool(request.form.get('is_admin'))

            user = User(username=username, email=email, is_admin=is_admin)
            user.set_password(password)
            user.projects = Project.query.filter(Project.id.in_(project_ids)).all()
            db.session.add(user)
            db.session.commit()
            return redirect(url_for('main.users'))

        projects = Project.query.all()
        return render_template('user_new.html', projects=projects)

    @app.route('/user/<int:user_id>/delete', methods=['POST'], endpoint='delete_user')
    def delete_user(user_id):
        user = User.query.get_or_404(user_id)
        db.session.delete(user)
        db.session.commit()
        flash('User deleted', 'success')
        return redirect(url_for('main.users'))

    return app
