import uuid, falcon
from Utils.error import RError
from Utils.config import RConfig


class RAdmin():
    def __init__(self):
        pass

    def on_post(self, req, resp):
        db = req.context['sql']
        admin = req.get_param("admin")
        if admin != RConfig().admin_password:
            raise RError(403)
        operate = req.get_param("operate")
        if operate == "create_folder":
            parent_id = req.get_param("parent_id")
            name = req.get_param("name")
            folder_id = str(uuid.uuid4())
            parent = db.execute("SELECT * FROM path WHERE id = %s", (parent_id,))[0]
            path = parent['path'] + "/" + name
            db.execute("INSERT INTO path(id, parent_id, type, name, create_at, path, status, size) "
                       "VALUES (%s,%s,0,%s,now(),%s,0,0)", (folder_id, parent_id, name, path))
            resp.status = falcon.HTTP_204
            return
        if operate == "delete_item":
            item_id = req.get_param("item_id")
            db.execute("DELETE FROM path WHERE id = %s", (item_id,))
            resp.status = falcon.HTTP_204
