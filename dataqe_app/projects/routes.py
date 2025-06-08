from flask import Blueprint, render_template, request, redirect, url_for
from dataqe_app import db
from dataqe_app.models import Project, Team, Connection

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
    teams = [project.team] if getattr(project, 'team', None) else []
    connections = getattr(project, 'connections', [])
    return render_template('project_detail.html', project=project, teams=teams, connections=connections)

@projects_bp.route('/teams/new/<int:project_id>', methods=['GET', 'POST'])
def new_team(project_id):
    """Create a new team for the given project."""
    project = Project.query.get_or_404(project_id)
    if request.method == 'POST':
        name = request.form.get('name')
        if name:
            team = Team(name=name)
            db.session.add(team)
            db.session.flush()
            project.team_id = team.id
            db.session.commit()
            return redirect(url_for('projects.project_detail', project_id=project_id))
    return render_template('team_new.html', project=project)


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

