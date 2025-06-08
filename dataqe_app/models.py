from dataqe_app import db
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    project = db.relationship('Project', backref='team', uselist=False)
    users = db.relationship('User', backref='team', lazy=True)

class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)  # Add this
    description = db.Column(db.Text, nullable=True)   # And this
    folder_path = db.Column(db.String(255), nullable=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'))
    connections = db.relationship('Connection', backref='project', lazy=True)

class Connection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    server = db.Column(db.String(255))
    database = db.Column(db.String(255))
    is_excel = db.Column(db.Boolean, default=False)
    warehouse = db.Column(db.String(255))
    role = db.Column(db.String(255))
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'))
    is_admin = db.Column(db.Boolean, default=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class TestCase(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tcid = db.Column(db.String(50), unique=True)
    tc_name = db.Column(db.String(100))
    table_name = db.Column(db.String(100))
    test_type = db.Column(db.String(50))
    test_yn = db.Column(db.String(1), default='Y')
    src_data_file = db.Column(db.String(255))
    tgt_data_file = db.Column(db.String(255))
    src_connection_id = db.Column(db.Integer, db.ForeignKey('connection.id'))
    tgt_connection_id = db.Column(db.Integer, db.ForeignKey('connection.id'))
    src_connection = db.relationship('Connection', foreign_keys=[src_connection_id])
    tgt_connection = db.relationship('Connection', foreign_keys=[tgt_connection_id])
    filters = db.Column(db.Text)
    delimiter = db.Column(db.String(10))
    pk_columns = db.Column(db.String(255))
    date_fields = db.Column(db.String(255))
    percentage_fields = db.Column(db.String(255))
    threshold_percentage = db.Column(db.Float)
    src_sheet_name = db.Column(db.String(255))
    tgt_sheet_name = db.Column(db.String(255))
    header_columns = db.Column(db.String(255))
    skip_rows = db.Column(db.String(255))
    creator_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())
    creator = db.relationship('User', foreign_keys=[creator_id])
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'))
    team = db.relationship('Team', backref='test_cases')

class TestExecution(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    test_case_id = db.Column(db.Integer, db.ForeignKey('test_case.id'))
    execution_time = db.Column(db.DateTime, default=db.func.current_timestamp())
    end_time = db.Column(db.DateTime)
    duration = db.Column(db.Float)
    status = db.Column(db.String(20))
    executed_by = db.Column(db.Integer, db.ForeignKey('user.id'))
    records_compared = db.Column(db.Integer)
    mismatches_found = db.Column(db.Integer)
    log_file = db.Column(db.String(255))
    error_message = db.Column(db.Text)
    test_case = db.relationship('TestCase', backref='executions')

class TestMismatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    execution_id = db.Column(db.Integer, db.ForeignKey('test_execution.id'))
    row_identifier = db.Column(db.String(100))
    column_name = db.Column(db.String(100))
    source_value = db.Column(db.Text)
    target_value = db.Column(db.Text)
    mismatch_type = db.Column(db.String(50))

class ScheduledTest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    test_case_id = db.Column(db.Integer, db.ForeignKey('test_case.id'))
    schedule_type = db.Column(db.String(20))
    schedule_time = db.Column(db.String(10))
    schedule_days = db.Column(db.String(50))
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'))
