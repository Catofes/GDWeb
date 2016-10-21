import argparse, uuid, queue, zlib, os, time
from Utils.config import RConfig
from Utils.database import RDateBasePool
from Drive.auth import RAuth
from pydrive.drive import GoogleDrive
from pydrive.auth import AuthError


class SplitData:
    file_id = ""
    id = 0
    offset = 0
    data = None
    length = 0
    crc = ""

    def __lt__(self, other):
        return self.offset < other.offset


class GDBlock:
    def __init__(self):
        self.config = RConfig()
        self.db = None
        self.splits = queue.PriorityQueue()
        self.files = {}

    def generate(self):
        self.db = RDateBasePool().begin()
        self.load_file()
        while self.generate_a_block():
            pass
        self.db.commit()
        self.delete_file()
        self.upload_blocks()

    def delete_file(self):
        for k, file in self.files.items():
            file_path = self.config.work_dir + self.config.upload_path + k
            os.remove(file_path)

    def load_file(self):
        results = self.db.execute("SELECT * FROM path WHERE type=1 AND status=1;", ())
        for result in results:
            file_id = result['id']
            self.files[file_id] = dict(result)
            file_path = self.config.work_dir + self.config.upload_path + result['id']
            with open(file_path, 'rb') as input_file:
                split_id = 0
                size = 0
                while True:
                    data = input_file.read(self.config.split_size)
                    if not data:
                        break
                    split_id += 1
                    split = SplitData()
                    split.id = split_id
                    split.file_id = file_id
                    split.data = data
                    split.offset = size
                    split.length = len(data)
                    size += split.length
                    split.crc = zlib.crc32(data)
                    self.splits.put((self.config.split_size - split.length, split))
                self.files[file_id]['size'] = size
            self.db.execute("UPDATE path SET status=0 WHERE id=%s", (file_id,))

    def generate_a_block(self):
        block_id = str(uuid.uuid4())
        path = self.config.work_dir + self.config.block_path + block_id
        if self.splits.empty():
            return False
        with open(path, "wb") as block:
            length = 0
            while True:
                if self.splits.empty():
                    break
                split = self.splits.get()[1]
                if split.length + length > self.config.max_block_size:
                    self.splits.put((self.config.split_size - split.length, split))
                    break
                block.write(split.data)
                self.db.execute(
                    "INSERT INTO file(id, size, split, split_offset, split_length, split_crc, block_id, block_offset)"
                    "VALUES (%s,'%s','%s','%s','%s','%s',%s,'%s');",
                    (split.file_id, self.files[split.file_id]['size'], split.id, split.offset, split.length, split.crc,
                     block_id, length))
                length += split.length
        self.db.execute("INSERT INTO cache(block_id, priority, upload) VALUES (%s, '%s', 1);",
                        (block_id, 1))
        return True

    def upload_blocks(self):
        results = RDateBasePool().execute("SELECT * FROM cache WHERE upload = 1", ())
        for result in results:
            self.db = RDateBasePool().begin()
            try:
                self.upload_a_block(result)
                self.upload_a_block(result)
            except AuthError:
                self.db.rollback()
                print("Auth Error when upload %s." % (result['block_id']))
                continue
            except IOError as e:
                self.db.rollback()
                print("IO Error when upload %s, %s." % (result['block_id'], str(e)))
                continue
            self.db.commit()

    def upload_a_block(self, block):
        auth, user_id, folder = RAuth().get_auth()
        drive = GoogleDrive(auth)
        path = self.config.work_dir + self.config.block_path + block['block_id']
        f = drive.CreateFile({"title": block['block_id'], "parents": [{"kind": "drive#fileLink", "id": folder}]})
        f.SetContentFile(path)
        f.Upload()
        print("Block %s Uploaded" % block['block_id'])
        self.db.execute("INSERT INTO block(id, block_length, file_id, status, auth_id) VALUES (%s,%s,%s,%s,%s);"
                        "UPDATE cache SET upload=0 WHERE block_id= %s;",
                        (block['block_id'], f['fileSize'], f['id'], self.config.re_upload_limit, user_id,
                         block['block_id']))


if __name__ == '__main__':
    while True:
        GDBlock().generate()
        time.sleep(60)
