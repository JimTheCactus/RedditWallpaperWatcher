import logging
import json
import time

import tornado.web

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher.management_app")

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        bubba = self.get_argument("bubba", default=None)

        if bubba is None:
            self.send_error(400, message="'bubba' query parameter missing")
            return

        result = {'now': time.localtime()}
        logger.info(f"Got request for path '{self.request.uri}'")
        self.write(json.dumps(result))
    
    def write_error(self, status_code:int, message: str = "", **kwargs):
        self.set_status(status_code)
        self.write(f"<html><head><title>Exception</title></head><body><h1>{status_code}: {message}</h1></body></html>")


def _get_server_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

def launch_management():
    app = _get_server_app()
    app.listen(8080)
