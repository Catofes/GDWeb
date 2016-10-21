import io, time, zlib
from Utils.database import RDataBaseConnection, RDateBasePool
from Utils.config import RConfig
from Drive.auth import RAuth
from Utils.error import RError


class DSplit:
    offset = 0
    length = 0
    crc = ""
    block_id = ""
    block_offset = 0


class DownloadError(Exception):
    def __init__(self, block_id="Unknown"):
        print("Download Error At: %s" % block_id)


class DFile(io.RawIOBase):
    def __init__(self, file_id: str, db: RDataBaseConnection, start: int = 0, length: int = -1):
        io.RawIOBase.__init__(self)
        self.config = RConfig()
        self.db = db
        self.start = start
        self.length = length
        self.splits = list(db.execute("SELECT * FROM file WHERE id =%s ORDER BY split DESC", (file_id,)))
        if not self.splits:
            raise RError(404)
        RDateBasePool().execute("UPDATE cache SET priority=priority+%s "
                                "WHERE upload>-1 AND block_id IN (SELECT block_id FROM file WHERE id = %s)",
                                (self.config.add_priority, file_id))
        RDateBasePool().execute("INSERT INTO cache(block_id, priority, upload) "
                                "SELECT block_id, '%s',-1 FROM file WHERE id = %s "
                                "AND block_id NOT IN(SELECT block_id FROM cache) GROUP BY block_id;",
                                (self.config.default_priority, file_id))
        self.size = self.splits[0]['size']
        self.split_id = -1
        self.split_data = io.BytesIO()
        self.split = None

    def load_split(self, split):
        self.split_id = split['split']
        self.split = DSplit()
        self.split.offset = split['split_offset']
        self.split.length = split['split_length']
        self.split.crc = split['split_crc']
        self.split.block_id = split['block_id']
        self.split.block_offset = split['block_offset']
        result = self.db.execute("SELECT * FROM cache WHERE block_id=%s AND upload>-1;", (self.split.block_id,))
        if result:
            self.split_data = io.BytesIO()
            path = self.config.work_dir + self.config.block_path + result[0]['block_id']
            with open(path, "rb") as block_data:
                block_data.seek(self.split.block_offset)
                tmp = block_data.read(self.split.length)
                if zlib.crc32(tmp) != self.split.crc:
                    raise RError(2)
                if not tmp:
                    return
                self.split_data.write(tmp)
                self.split_data.seek(0)
        else:
            result = self.db.execute("SELECT * FROM block WHERE id =%s AND status>0; ", (self.split.block_id,))
            self.split_data = io.BytesIO()
            if not result:
                return
            result = result[0]
            file_id = result['file_id']
            service = RAuth().get_credential(result['auth_id'])
            request = service.files().get_media(fileId=file_id)
            request.headers['Range'] = "bytes=%s-%s" % \
                                       (self.split.block_offset, self.split.block_offset + self.split.length - 1)
            try:
                print("Get Split %s at Block %s in %s." %
                      (self.split_id, self.split.block_id, str(request.headers['Range'])))
                tmp = request.execute()
                if zlib.crc32(tmp) != self.split.crc:
                    raise RError(2)
                self.split_data.write(tmp)
                self.split_data.seek(0)
                self.db.execute("UPDATE block SET status='%s' WHERE file_id = %s",
                                (self.config.re_upload_limit, file_id))
            except Exception:
                RDateBasePool().execute("UPDATE block SET status=status-1 WHERE file_id = %s", (file_id,))
                raise DownloadError(self.split.block_id)

    def find_split(self):
        while True:
            if len(self.splits) == 0:
                return None
            split = self.splits.pop()
            if split['split_offset'] <= self.start < split['split_offset'] + split['split_length']:
                self.splits.append(split)
                return split
            else:
                pass

    def read(self, size=-1):
        if self.length == 0:
            return None
        split = self.find_split()
        if not split:
            return None
        times = 0
        if self.split_id != split['split']:
            while times < self.config.retry_times:
                try:
                    self.load_split(split)
                except DownloadError:
                    print("Download Error at split %s" % self.split_id)
                    times += 1
                    time.sleep(1)
                    continue
                break
        if times == self.config.retry_times:
            raise RError(2)
        data = self.split_data.read(size)
        self.start += len(data)
        self.length -= len(data)
        return data
