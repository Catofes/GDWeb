# -*- coding: UTF-8 -*-
try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
except ImportError:
    # Fall back to psycopg2cffi
    from psycopg2cffi import compat
    compat.register()

from Utils.singleton import Singleton
from Utils.config import RConfig
from Utils.error import RError


class RDateBasePool:
    pass


class RDataBaseConnection:
    def __init__(self, db, pool: RDateBasePool):
        self.db = db
        self.pool = pool
        try:
            self.cursor = self.db.cursor()
        except Exception as e:
            print("SQL ERROR: GET CURSOR ERROR.")
            self.pool.end(self.db)
            raise RError(1)

    def execute(self, sql: str, param: tuple) -> dict:
        try:
            self.cursor.execute(sql, param)
        except psycopg2.Error as e:
            self.cursor.close()
            self.pool.end(self.db)
            if e.pgcode == '22P02':
                raise RError(1)
            print("SQL ERROR: Execute Error Execute [%s] %r" % (sql, param))
            print(e.pgerror)
            raise RError(1)
        try:
            result = self.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            result = []
        if not result:
            result = []
        return result

    def executemany(self, sql: str, param: tuple):
        try:
            self.cursor.executemany(sql, param)
        except psycopg2.Error as e:
            self.cursor.close()
            self.pool.end(self.db)
            print("SQL ERROR: Execute Error Execute [%s] %r" % (sql, param))
            print(e.pgerror)
            raise RError(1)
        return True

    def commit(self):
        self.db.commit()
        self.pool.end(self.db)

    def rollback(self):
        self.db.rollback()
        self.pool.end(self.db)


class RDateBasePool(Singleton):
    def __init__(self):
        if hasattr(self, '_init'):
            return
        self._init = True
        config = RConfig()
        self._db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=config.db_min_cached,
            maxconn=config.db_max_connections,
            database=config.db_database,
            user=config.db_user,
            password=config.db_password,
            host=config.db_host,
            port=config.db_port,
            cursor_factory=psycopg2.extras.DictCursor,
            connect_timeout=3
        )

    def execute(self, sql: str, param: tuple) -> dict:
        try:
            db = self._db_pool.getconn()
        except Exception as e:
            print("SQL_Pool Error: CONN [%s] %r" % (sql, param))
            raise RError(1)
        try:
            cursor = db.cursor()
        except Exception as e:
            print("SQL_POOL Error: CURS [%s] %r" % (sql, param))
            self._db_pool.putconn(db)
            raise RError(1)
        try:
            cursor.execute(sql, param)
        except psycopg2.Error as e:
            cursor.close()
            self._db_pool.putconn(db)
            print("SQL_POOL Error: Execute [%s] %r" % (sql, param))
            print(e.pgerror)
            raise RError(1)
        try:
            result = cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            result = []
        db.commit()
        cursor.close()
        self._db_pool.putconn(db)
        if not result:
            result = []
        return result

    def begin(self):
        try:
            db = self._db_pool.getconn()
        except Exception as e:
            print("SQL_POOL Error: GET CONNECTION")
            raise RError(1)
        return RDataBaseConnection(db, self)

    def end(self, con: RDataBaseConnection):
        try:
            self._db_pool.putconn(con)
        except Exception:
            pass
