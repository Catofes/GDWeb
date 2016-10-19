from Utils.config import RConfig
from Utils.database import RDateBasePool
from Drive.auth import RAuth
from pydrive.drive import GoogleDrive
from pydrive.auth import AuthError
import random, time, os


class BlockBroken(Exception):
    pass


class RManage:
    def __init__(self):
        self.config = RConfig()
        self.db = None
        self.auth = RAuth()
        self.time = time.time()

    def run(self):
        while True:
            time.sleep(1)
            self.download()
            self.down_priority()
            self.re_upload_block()

    def download(self):
        self.delete_block()
        blocks = RDateBasePool().execute("SELECT * FROM cache WHERE upload=-1")
        for block in blocks:
            self.db = RDateBasePool().begin()
            try:
                self.download_one_block(block['block_id'])
            except AuthError:
                self.db.rollback()
                print("Auth Error when download %s。" % (block['block_id']))
                continue
            except IOError as e:
                self.db.rollback()
                print("IO Error when download %s, %s。" % (block['block_id'], str(e)))
                continue
            except BlockBroken:
                self.db.rollback()
                print("Block Broken at %s." % (block['block_id']))
                continue
            self.db.commit()
            print("Download block %s." % block['block_id'])

    def download_one_block(self, block_id):
        blocks = self.db.execute("SELECT * FROM block WHERE id =%s AND status>0", (block_id,))
        if not blocks:
            raise BlockBroken()
        block = random.choice(blocks)
        print("Download block %s from file %s." % (block['id'], block['file_id']))
        auth = self.auth.get_auth(block['auth_id'])
        drive = GoogleDrive(auth)
        f = drive.CreateFile({'id': block['file_id']})
        path = self.config.work_dir + self.config.block_path + block_id
        f.GetContentFile(path)
        self.db.execute("UPDATE cache SET upload=0 WHERE block_id = %s", (block_id,))

    def down_priority(self):
        if self.time + 3600 < time.time():
            RDateBasePool().execute("UPDATE cache SET priority=priority-'%s';", (self.config.minus_priority,))
            self.time = time.time()

    def delete_block(self):
        result = RDateBasePool().execute(
            "SELECT SUM(t.block_length) AS size FROM"
            "(SELECT block_length FROM block WHERE id IN (SELECT block_id FROM cache WHERE upload<1) GROUP BY id) AS t",
            ())
        size = result[0]['size']
        if size < self.config.cache_max_size:
            return
        blocks = RDateBasePool().execute(
            "SELECT id,block_length,upload FROM block JOIN cache ON block_id WHERE upload<1 GROUP BY id ORDER BY priority",
            ())
        for block in blocks:
            if size < self.config.cache_max_size:
                break
            print("Delete block %s." % block['id'])
            RDateBasePool().execute("DELETE FROM cache WHERE block_id=%s", (block['id'],))
            if block['upload'] == 0:
                path = self.config.work_dir + self.config.block_path + block['id']
                os.remove(path)
            size -= block['block_length']
            blocks = blocks[1:]

    def re_upload_block(self):
        blocks = RDateBasePool().execute("SELECT id,file_id FROM block WHERE status<=0 GROUP BY id", ())
        for block in blocks:
            self.db = RDateBasePool().begin()
            result = self.db.execute("SELECT * FROM cache WHERE block_id = %s", (block['id']))
            if result and result[0]['upload'] >= 0:
                self.db.execute("UPDATE cache SET upload=1 WHERE id = %s;"
                                "DELETE FROM block WHERE file_id=%s", (block['id'], block['file_id']))
                self.db.commit()
                continue
            elif not result:
                self.db.execute("INSERT INTO cache(block_id, priority, upload) VALUES (%s,'%s',1);"
                                "DELETE FROM block WHERE file_id=%s",
                                (block['id'], self.config.default_priority, block['file_id']))
            else:
                self.db.execute("UPDATE cache SET upload=1 WHERE id = %s;"
                                "DELETE FROM block WHERE file_id=%s", (block['id'], block['file_id']))
            try:
                self.download_one_block(block['id'])
                self.delete_block()
            except AuthError:
                self.db.rollback()
                print("Auth Error when download %s。" % (block['id']))
                continue
            except IOError as e:
                self.db.rollback()
                print("IO Error when download %s, %s。" % (block['id'], str(e)))
                continue
            except BlockBroken:
                self.db.rollback()
                print("Block Broken at %s." % (block['id']))
                continue
            self.db.commit()
            print("Re Download block %s." % block['id'])
