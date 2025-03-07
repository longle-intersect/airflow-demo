from flask import Blueprint
from flask_login import current_user
from airflow.www.app import create_app
from airflow.plugins_manager import AirflowPlugin

# Define a Blueprint for the custom route
bp = Blueprint("custom_view", __name__)

@bp.route("/whoami")
def whoami():
    if current_user.is_authenticated:
        user_info = {
            "username": current_user.username,
            "roles": [role.name for role in current_user.roles],
            "id": current_user.id
        }
        return f"Current user: {user_info}"
    return "Not logged in"

# Register the Blueprint as an Airflow plugin
class WhoAmIPlugin(AirflowPlugin):
    name = "whoami_plugin"
    flask_blueprints = [bp]

# Save this as a plugin file, e.g., `plugins/whoami_plugin.py`