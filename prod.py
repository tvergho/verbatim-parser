import api
import waitress
import os
import logging

logger = logging.getLogger('waitress')
logger.setLevel(logging.INFO)

waitress.serve(api.app, port=os.environ['PORT'], url_scheme='https')