import os

from celery import Celery

from dotenv import load_dotenv
load_dotenv()

BROKER = os.environ['BROKER']
celery = Celery('celery_transcribe', broker='', include='tasks')

flaskapp = None
# daylight_url = os.environ["API_URL"]


def setup_celery_vars(app):
    app.config['CELERY_BROKER_URL'] = ''
    app.config['CELERY_RESULT_BACKEND'] = 'rpc://'
    flaskapp = app

    # flaskapp.logger.info("DEBUG ONLY REMOVE: Establishing celery connection: {}".format(app.config['CELERY_BROKER_URL']))
    # celery = Celery('celery_transcribe', broker=app.config['CELERY_BROKER_URL'], include='celery_tasks.tasks')
    flaskapp.logger.info("Connecting to rabbitmq broker.")
    celery.conf.update(app.config)
    flaskapp.logger.info("Connected to rabbitmq broker.")

