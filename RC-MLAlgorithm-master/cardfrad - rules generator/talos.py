## Talos File;
## Online Download Hive Data

# -*- coding: utf8 -*-
import time
import threading
import traceback

from pytalos.client import AsyncTalosClient

USERNAME = "talos.payrc.ae"
PASSWORD = "Payrc.ae0"

class QueryError(Exception):
    pass

class DumpError(Exception):
    pass

class Query(threading.Thread):
    def __init__(self, sql, dump=None, retry=1, timeout=7200):
        super(Query, self).__init__()

        self.sql = sql
        self.dumpfile = dump
        self.retry = retry
        self.timeout = timeout
        self._tried = 0

        self.finish = False
        self.error = None
        self.qid = None
        self.client = None

    def _get_query_info(self, retry=3):
        _tried = 0
        while True:
            _tried += 1
            try:
                return self.client.get_query_info(self.qid)
            except Exception, e:
                if _tried >= retry:
                    raise e

                time.sleep(1)

    def query(self):
        self.client = AsyncTalosClient(username=USERNAME, password=PASSWORD)
        self.client.open_session()

        dsn, engine = "hdim", "hive"

        while True:
            self._tried += 1
            try:
                self.qid = self.client.submit(dsn=dsn,
                                              statement=self.sql, engine=engine)

                start_time = time.time()
                while True:
                    if self.timeout and time.time() > start_time + self.timeout:
                        raise Exception('Query timeout.')

                    query_info = self._get_query_info()
                    if query_info["status"] == "FINISHED":
                        return self.client.fetch_all(self.qid)
                    elif query_info["status"] in ["ERROR", "FAILED", "KILLED"]:
                        raise Exception(query_info["engine_log"])

                    time.sleep(3)

            except Exception, e:
                traceback.print_exc()
                if self._tried >= self.retry:
                    raise e

                time.sleep(3)

    def run(self):
        try:
            self.result = self.query()
        except Exception, e:
            msg = '%s\n%s' % (traceback.format_exc(), self.sql)
            self.error = QueryError(msg)
            self.finish = True
            raise e

        try:
            if self.dumpfile is not None:
                self._dump(self.result)
        except Exception, e:
            self.error = DumpError(traceback.format_exc())
            self.finish = True
            raise e

    def _dump(self, result):
        header = map(lambda c: c['name'], result['columns'])

        # data = dict(header=header, lines=lines)
        # pickle.dump(data, open('%s.pkl' % self.dumpfile, 'wb+'))

        with open(self.dumpfile, 'wt+') as csv_file:
            csv_file.write('\t'.join(header).encode('utf-8'))
            csv_file.write('\n')
            for line in result['data']:
                line_str = '\t'.join(line).encode('utf-8')
                csv_file.write(line_str)
                csv_file.write('\n')



