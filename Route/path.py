from Utils.error import RError
from Utils.config import RConfig
import re, uuid


class RPath:
    def __init__(self):
        self.text = ""

    def _generate_head(self):
        self.text = ""
        self.text += """
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<title>GDWeb</title>
<meta name="description" content="">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="//cdn.bootcss.com/bootstrap/3.3.7/css/bootstrap.min.css" rel="stylesheet">
<link href="//cdn.bootcss.com/dropzone/4.3.0/min/dropzone.min.css" rel="stylesheet">
</head>
<body>
<div class="container">
"""

    def _generate_end(self, admin):
        self.text += """
</div>
<script src="//cdn.bootcss.com/jquery/3.1.1/jquery.min.js"></script>
<script src="//cdn.bootcss.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
<script src="//cdn.bootcss.com/dropzone/4.3.0/min/dropzone.min.js"></script>
"""
        if admin:
            self.text += """
<script>
            function s(operate, parent_id, admin){
                if(operate=="create"){
                    name = $("#createFolder").val()
                    $.post("/admin",{
                        operate: "create_folder",
                        parent_id: parent_id,
                        name: name,
                        admin: admin
                    },function(data){alert("Create Folder Success.")})
                }else if (operate=="delete"){
                    item_id = $("#deleteItem").val()
                    $.post("/admin",{
                        operate: "delete_item",
                        parent_id: parent_id,
                        item_id: item_id,
                        admin: admin
                    },function(data){alert("Delete Item Success.")})
                }
            };
            Dropzone.options.uploadFile = {
                url: window.location.href,
                uploadMultiple: true,
                maxFilesize: 10240
            }

</script>
"""
        self.text += """
</body>
</html>
"""

    @staticmethod
    def _get_size(size):
        key = ["B", "KB", "MB", "GB"]
        i = 1
        while i < len(key) and size > 1024:
            i += 1
            size /= 1024
        return "%s%s" % (round(size, 2), key[i - 1])

    def on_get(self, req, resp, path_id=None):
        admin = False
        if "admin" in req.params and req.params['admin'] == RConfig().admin_password:
            admin = True
        self._generate_head()
        db = req.context['sql']
        if not path_id:
            path_id = db.execute("SELECT * FROM path WHERE parent_id IS NULL", ())[0]['id']
        path = db.execute("SELECT * FROM path WHERE id=%s;", (path_id,))
        if not path:
            raise RError(404)
        path = path[0]
        children = db.execute("SELECT * FROM path WHERE parent_id=%s", (path_id,))
        self.text += """<h2 id="pathName">%s</h2>""" % path['name']
        self.text += """<h4 id="absolutePath">%s/</h4>""" % path['path']
        self.text += """
<table class="table">
    <tr>
        <td>Name</td>
        <td>Create Time</td>
        <td>Size</td>
    </tr>
"""
        if path['parent_id']:
            self.text += """
    <tr>
        <td><a href="/path/%s">%s/</a></td>
        <td>%s</td>
        <td>%s</td>
    </tr>
""" % (path['parent_id'], "..", path['create_at'], 0)
        files = []
        folders = []
        for child in children:
            if child['type'] == 1:
                files.append(child)
            if child['type'] == 0:
                folders.append(child)
        sorted(files, key=lambda x: x['name'])
        sorted(folders, key=lambda x: x['name'])
        for f in folders:
            if admin:
                self.text += """
    <tr>
        <td><a href="/path/%s?admin=%s">%s/</a></td>
        <td>%s</td>
        <td>%s</td>
    </tr>
""" % (f["id"], RConfig().admin_password, f["name"], f["create_at"], 0)
            else:
                self.text += """
    <tr>
        <td><a href="/path/%s">%s/</a></td>
        <td>%s</td>
        <td>%s</td>
    </tr>
""" % (f["id"], f["name"], f["create_at"], 0)

        for f in files:
            self.text += """
    <tr>
        <td><a href="/file/%s">%s</a></td>
        <td>%s</td>
        <td>%s</td>
    </tr>
""" % (f["id"], f["name"], f["create_at"], self._get_size(f["size"]))
        self.text += "</table>"
        if admin:
            self.text += """
        <div class="form-group">
            <label for="CreateFolder">Create Folder</label>
            <input type="text" class="form-control" id="createFolder" placeholder="Folder Name">
        </div>
        <button onclick="s('create','%s','%s')" class="btn btn-default">Submit</button>
""" % (path_id, req.params['admin'])
            self.text += """
        <div class="form-group">
            <label for="CreateFolder">Delete Item</label>
            <input type="text" class="form-control" id="deleteItem" placeholder="Item Id">
        </div>
        <button onclick="s('delete','%s','%s')" class="btn btn-default">Submit</button>
""" % (path_id, req.params['admin'])
            self.text += """
<form class="dropzone" id="uploadFile"></form>
"""
        self._generate_end(admin)
        resp.body = self.text
        resp.set_header("content-type", "text/html; charset=utf-8")

    def on_post(self, req, resp, path_id=None):
        db = req.context['sql']
        if req.get_param('admin') != RConfig().admin_password:
            raise RError(403)
        if not path_id:
            path_id = db.execute("SELECT * FROM path WHERE parent_id IS NULL", ())[0]['id']
        path = db.execute("SELECT * FROM path WHERE id=%s;", (path_id,))[0]
        for k, v in req.params.items():
            if re.match("^file\[\d*\]$", k):
                file_name = v.filename
                file_id = str(uuid.uuid4())
                file_path = RConfig().work_dir + RConfig().upload_path + file_id
                file_length = 0
                with open(file_path, "wb") as f:
                    while True:
                        data = v.file.read(8196)
                        if not data:
                            break
                        file_length += len(data)
                        f.write(data)
                db.execute("INSERT INTO path(id, parent_id, type, name, create_at, path, status, size) VALUES "
                           "(%s, %s, 1, %s, now(), %s, 1, %s)",
                           (file_id, path_id, file_name, path['path'] + "/" + file_name, file_length))
