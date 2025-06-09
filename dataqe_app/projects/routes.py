from flask import Blueprint, render_template, request, redirect, url_for
from dataqe_app import db
from dataqe_app.models import Project, Connection, User

projects_bp = Blueprint('projects', __name__)

@projects_bp.route('/projects')
def projects():
    projects = Project.query.all()
    return render_template('projects.html', projects=projects)

@projects_bp.route('/projects/new', methods=['GET', 'POST'])
def new_project():
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        folder_path = request.form.get('folder_path')
        if name:
            project = Project(name=name, description=description, folder_path=folder_path)
            db.session.add(project)
            db.session.commit()
            return redirect(url_for('projects.projects'))
    return render_template('project_new.html')

@projects_bp.route('/projects/<int:project_id>')
def project_detail(project_id):
    project = Project.query.get_or_404(project_id)
    connections = getattr(project, 'connections', [])
    users = project.users
    test_cases = project.test_cases
    available_users = User.query.filter(~User.projects.any(id=project_id)).all()
    return render_template(
        'project_detail.html',
        project=project,
        users=users,
        connections=connections,
        test_cases=test_cases,
        available_users=available_users,
    )



@projects_bp.route('/connections/new/<int:project_id>', methods=['GET', 'POST'])
def new_connection(project_id):
    """Create a new connection for the given project."""
    project = Project.query.get_or_404(project_id)
    if request.method == 'POST':
        name = request.form.get('name')
        server = request.form.get('server')
        database = request.form.get('database')
        is_excel = bool(request.form.get('is_excel'))
        warehouse = request.form.get('warehouse')
        role = request.form.get('role')
        if name:
            conn = Connection(
                name=name,
                server=server,
                database=database,
                is_excel=is_excel,
                warehouse=warehouse,
                role=role,
                project_id=project.id,
            )
            db.session.add(conn)
            db.session.commit()
            return redirect(url_for('projects.project_detail', project_id=project.id))
    return render_template('connection_new.html', project=project)


@projects_bp.route('/projects/<int:project_id>/delete', methods=['POST'])
def delete_project(project_id):
    """Delete a project and its connections."""
    project = Project.query.get_or_404(project_id)
    Connection.query.filter_by(project_id=project_id).delete()
    db.session.delete(project)
    db.session.commit()
    return redirect(url_for('projects.projects'))


@projects_bp.route('/projects/<int:project_id>/add_member', methods=['POST'])
def add_project_member(project_id):
    """Assign an existing user to the project."""
    user_id = request.form.get('user_id')
    project = Project.query.get_or_404(project_id)
    user = User.query.get_or_404(user_id)
    if user not in project.users:
        project.users.append(user)
        db.session.commit()
    return redirect(url_for('projects.project_detail', project_id=project_id))


@projects_bp.route('/projects/<int:project_id>/remove_member/<int:user_id>', methods=['POST'])
def remove_project_member(project_id, user_id):
    """Remove a user from the project."""
    project = Project.query.get_or_404(project_id)
    user = User.query.get_or_404(user_id)
    if user in project.users:
        project.users.remove(user)
        db.session.commit()
    return redirect(url_for('projects.project_detail', project_id=project_id))

