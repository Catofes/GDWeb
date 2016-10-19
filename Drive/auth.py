from Utils.config import RConfig
from Utils.singleton import Singleton
from Utils.database import RDateBasePool
from oauth2client.file import Storage
import random, httplib2
from collections import deque
from apiclient import discovery
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


class RAuth(Singleton):
    def __init__(self):
        if hasattr(self, '_init'):
            return
        self._init = True
        self.db = RDateBasePool()

    def get_credential(self, id):
        result = self.db.execute("SELECT * FROM auth WHERE id = %s", (id,))
        result = random.choice(result)
        store = Storage(result['credential_file'])
        credentials = store.get()
        if not credentials or credentials.invalid:
            return self.generate_credential(id)
        http = credentials.authorize(httplib2.Http())
        service = discovery.build('drive', 'v3', http=http)
        return service

    def get_auth(self, id=None):
        if id:
            result = self.db.execute("SELECT * FROM auth WHERE id = %s", (id,))
        else:
            result = self.db.execute("SELECT * FROM auth;", ())
        result = random.choice(result)
        return GoogleAuth(settings_file=result['setting_file']), result['id'], result['folder']


def test():
    result = RDateBasePool().execute("SELECT * FROM auth", ())
    result = result[0]
    gauth = GoogleAuth(settings_file=result['setting_file'])
    drive = GoogleDrive(auth=gauth)
    file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
    for file1 in file_list:
        print('title: %s, id: %s' % (file1['title'], file1['id']))


if __name__ == '__main__':
    test()
