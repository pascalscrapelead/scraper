import json

from celery_tasks.vars import celery
from config import create_app
from scraper.assos.parse_asos import main

flask_app = create_app()
celery_app = flask_app.extensions["celery"]


@celery.task(ignore_result=True)
def print_value(value):
    main(json.loads(value))
    return None
