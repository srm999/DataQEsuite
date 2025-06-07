from flask import Blueprint, render_template, redirect, url_for

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'], endpoint='login')
@auth_bp.route('/auth/login', methods=['GET', 'POST'], endpoint='auth.login')
def login():
    return render_template('login.html')

@auth_bp.route('/logout', endpoint='auth.logout')
def logout():
    return redirect(url_for('auth.login'))
