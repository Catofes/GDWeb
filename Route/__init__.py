# -*- coding: UTF-8 -*-
from wsgiref import simple_server
from falcon_multipart.middleware import MultipartMiddleware
from Route.middleware import BeginSQL
from Utils.error import RError
import traceback, falcon

from Route.file import RFile
from Route.path import RPath, RPathUpload
from Route.admin import RAdmin


class App:
    @staticmethod
    def error_handle(ex, req, resp, params):
        if isinstance(ex, falcon.HTTPError):
            raise ex
        else:
            traceback.print_exc()
            raise RError(0)

    def __init__(self):
        self.app = falcon.API(middleware=[
            # middleware.RequireJSON(),
            # middleware.JSONTranslator(),
            MultipartMiddleware(),
            BeginSQL()
        ]
        )
        self.app.add_error_handler(Exception, handler=App.error_handle)

        r_file = RFile()
        r_path = RPath()
        r_path_upload = RPathUpload()
        r_admin = RAdmin()
        self.app.add_route('/file/{file_id}', r_file)
        self.app.add_route('/path/{path_id}', r_path)
        self.app.add_route('/path/', r_path)
        self.app.add_route('/upload/path/{path_id}', r_path_upload)
        self.app.add_route('/upload/path/', r_path_upload)
        self.app.add_route('/admin', r_admin)

    def run(self):
        httpd = simple_server.make_server('0.0.0.0', 8421, self.app)
        httpd.serve_forever()
