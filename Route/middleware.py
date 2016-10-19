import falcon
from Utils.database import RDateBasePool


class BeginSQL(object):
    def process_request(self, req, resp):
        req.context['sql'] = RDateBasePool().begin()

    def process_response(self, req, resp, resource):
        if resp.status != falcon.HTTP_200 and resp.status != falcon.HTTP_204:
            req.context['sql'].rollback()
        else:
            req.context['sql'].commit()
