from Utils.error import RError
from Utils.config import RConfig
from Drive.file import DFile
from urllib.parse import quote
import falcon


class RFile:
    def __init__(self):
        pass

    def on_get(self, req, resp, file_id):
        db = req.context['sql']
        result = db.execute("SELECT * FROM path WHERE id=%s", (file_id,))
        if not result:
            raise RError(404)
        result = result[0]
        resp.set_header("content-disposition",
                        "inline; filename*=UTF-8\'\'%s" % quote(result['name'], encoding='utf-8'))
        resp.set_header("content-type", result['mime'])
        resp.set_header("Cache-Control", "max-age=864000")
        resp.set_header("file_md5", result['md5'])
        if req.range:
            start = req.range[0]
            if req.range[1] >= start:
                length = req.range[1] - start + 1
            elif req.range[1] < 0:
                length = result['size'] - req.range[0] + req.range[1] + 1
            else:
                raise RError(400)
            resp.status = falcon.HTTP_206
            resp.set_header("Cache-Control", "")
            resp.set_header("Content-Range", "bytes %s-%s/%s" % (start, start + length - 1, result['size']))
        else:
            start = 0
            length = result['size']
        if result['status'] == 1:
            path = RConfig().work_dir + RConfig().upload_path + result['id']
            stream = open(path, "rb")
            stream.seek(start)
        else:
            stream = DFile(file_id, db, start, length)

        resp.set_header("content-length", length)
        resp.set_stream(stream, length)
