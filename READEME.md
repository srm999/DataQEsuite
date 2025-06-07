# DataQE Suite

A modular Flask web application for comparing datasets between source and target systems using a configurable test case interface.

---

## 🚀 Features
- Login/logout with role-based access (admin/team)
- Create, execute, and schedule test cases
- Track mismatches and execution logs
- Dashboard with pass/fail/error summaries

---

## 🏗️ Project Structure
```
dataqe_app/
├── __init__.py             # App factory and extension setup
├── models.py               # SQLAlchemy models
├── auth/
│   └── routes.py           # Login and logout
├── testcases/
│   └── routes.py           # Test case management and scheduling
├── executions/
│   └── routes.py           # Execution results and dashboard
├── scheduler/              # (optional: for custom scheduling logic)
│   └── routes.py
├── utils/
│   └── helpers.py          # Shared logic and scheduling hooks
├── bridge/
│   └── dataqe_bridge.py    # Core validation engine bridge

run.py                      # Entry point to launch the app
requirements.txt            # Required dependencies
```

---

## 🛠️ Setup

1. **Clone repo** and create virtual environment:
```bash
git clone https://github.com/yourname/dataqe-suite.git
cd dataqe-suite
python3 -m venv venv
source venv/bin/activate
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Initialize the database**
```bash
flask --app run.py init-db
```

4. **Run the app**
```bash
flask --app run.py run
```

---

## 📅 Scheduler
The APScheduler is initialized automatically. You can define scheduled jobs for test cases via the UI.

---

## 🔐 Default Admin Login
- **Username:** admin
- **Password:** admin *(change this in production!)*

---

## 🧪 Testing
You can test executions through the dashboard or trigger scheduled runs using `run_scheduled_test()` in `helpers.py`.

---

## 📁 Uploads
- Upload and SQL files are stored under `/uploads` and `/static/sql_files`

---

## 📬 Email Configuration
Update the `MAIL_*` settings in `__init__.py` to match your SMTP server or use environment variables.

---

## ✅ License
MIT

---

Feel free to contribute or raise issues!
