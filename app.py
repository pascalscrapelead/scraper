import json
import os
from tasks import flask_app, print_value
from flask import request, jsonify


def validate_api_key():
    api_key = request.headers.get('x-api-key')
    if not api_key or api_key != os.environ.get('API_KEY'):
        return jsonify({'message': 'You are not authorized to access this application'}), 403


@flask_app.before_request
def before_request():
    return validate_api_key()


@flask_app.post('/api/v5/projects')
def api_projects():
    task_data = request.get_json()
    task = print_value.apply_async(args=[json.dumps(task_data)])
    return jsonify({
        "message": "Data received",
        "data": 'data'
    }), 200


if __name__ == "__main__":
    flask_app.run(debug=True, host='0.0.0.0')
