import sqlalchemy as sa


'''
Below is a code to enable foreign key in sqlite3.
See https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#foreign-key-support.
'''
@sa.event.listens_for(sa.engine.Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class DbHandler(object):

    def __init__(self, uri):
        self.engine = sa.create_engine(uri)
        self.meta = sa.MetaData()
        self.tables = {}

        self._declare_tables()

    def _declare_tables(self):
        pvs = sa.Table('pvs', self.meta,
                       sa.Column('pvname', sa.TEXT, primary_key=True),
                       sa.Column('ring', sa.TEXT)
                       )

        cpvs = sa.Table('current_pvs', self.meta,
                        sa.Column('pvname', sa.TEXT,
                                  sa.ForeignKey('pvs.pvname'),
                                  primary_key=True),
                        sa.Column('msg', sa.TEXT)
                        )
        asig = sa.Table('abort_signals', self.meta,
                        sa.Column('abt_signal_id', sa.Integer,
                                  primary_key=True, autoincrement=True),
                        sa.Column('pvname', sa.TEXT,
                                  sa.ForeignKey('pvs.pvname')),
                        sa.Column('msg', sa.TEXT, index=True, unique=False),
                        sa.Column('pv_ts', sa.TEXT),
                        sa.Column('abt_ts', sa.TEXT, index=True, unique=False),
                        sa.Column('reset_cnt', sa.Integer),
                        sa.Column('trg_cnt', sa.Integer),
                        sa.Column('int_cnt', sa.Integer)
                        )
        abts = sa.Table('aborts', self.meta,
                        sa.Column('abt_id', sa.Integer,
                                  primary_key=True, autoincrement=True),
                        sa.Column('abt_time', sa.TEXT, index=True, unique=True)
                        )
        al = sa.Table('abort_list', self.meta,
                      sa.Column('abt_id', sa.Integer,
                                sa.ForeignKey('aborts.abt_id'),
                                primary_key=True),
                      sa.Column('abt_signal_id', sa.Integer,
                                sa.ForeignKey('abort_signals.abt_signal_id'),
                                primary_key=True)
                      )

        self.tables['pvs'] = pvs
        self.tables['current_pvs'] = cpvs
        self.tables['abort_signals'] = asig
        self.tables['aborts'] = abts
        self.tables['abort_list'] = al

        self.meta.create_all(self.engine)

    def fetch_all_pvs(self):
        conn = self.engine.connect(close_with_result=True)

        s = sa.select([self.tables['pvs']])
        result = conn.execute(s)

        r = result.fetchall()
        result.close()

        return r

    def fetch_current_pvs(self):
        t_pvs = self.tables['pvs']
        t_cpvs = self.tables['current_pvs']

        conn = self.engine.connect(close_with_result=True)

        s = (
              sa.select([t_cpvs.c.pvname, t_pvs.c.ring, t_cpvs.c.msg]).
              select_from(t_cpvs.join(t_pvs))
             )
        result = conn.execute(s)

        r = result.fetchall()
        result.close()

        return r

    def fetch_aborts(self):
        conn = self.engine.connect(close_with_result=True)

        s = sa.select([self.tables['aborts']])
        result = conn.execute(s)

        r = result.fetchall()
        result.close()

        return r

    def fetch_abort_signals(self, ring=None, first=True,
                            include_no_abt_id=False, astart=None, aend=None,
                            sstart=None, send=None):
        conn = self.engine.connect(close_with_result=True)
        t_abts = self.tables['aborts']
        t_as = self.tables['abort_signals']
        t_al = self.tables['abort_list']
        t_pvs = self.tables['pvs']

        if first:
            s = sa.select([
                           t_abts.c.abt_id,
                           sa.func.min(t_as.c.abt_ts).label('ts'),
                           t_as.c.pvname,
                           t_as.c.msg,
                           t_pvs.c.ring,
                           t_as.c.reset_cnt,
                           t_as.c.trg_cnt,
                           t_as.c.int_cnt
                          ]).group_by(
                           t_abts.c.abt_id,
                           t_as.c.pvname
                          )
        else:
            s = sa.select([
                           t_abts.c.abt_id,
                           t_as.c.abt_ts.label('ts'),
                           t_as.c.pvname,
                           t_as.c.msg,
                           t_pvs.c.ring,
                           t_as.c.reset_cnt,
                           t_as.c.trg_cnt,
                           t_as.c.int_cnt
                           ])

        if include_no_abt_id:
            s = s.select_from(t_as.outerjoin(t_al).
                              outerjoin(t_abts).join(t_pvs))
        else:
            s = s.select_from(t_as.join(t_al).join(t_abts).join(t_pvs))

        if ring:
            s = s.where(t_pvs.c.ring == ring)
        if astart:
            s = s.where(t_abts.c.abt_time > astart)
        if aend:
            s = s.where(t_abts.c.abt_time < aend)
        if sstart:
            s = s.where(t_as.c.abt_ts > sstart)
        if send:
            s = s.where(t_as.c.abt_ts < send)

        s = s.order_by(
                         t_abts.c.abt_id,
                         t_as.c.reset_cnt,
                         t_as.c.trg_cnt,
                         t_as.c.int_cnt
                       )

        result = conn.execute(s)

        r = result.fetchall()
        result.close()

        return r

    def update_current_pvs(self, pvs):
        table = self.tables['current_pvs']
        conn = self.engine.connect()

        conn.execute(table.delete())
        conn.execute(table.insert(), pvs)

        conn.close()

    def insert_pvs(self, pvs):
        table = self.tables['pvs']

        with self.engine.begin() as conn:
            s = sa.select([table])
            result = conn.execute(s)
            all_row = result.fetchall()
            db_pvs = {row['pvname'] for row in all_row}
            result.close()

            for pv in pvs:
                if pv['pvname'] not in db_pvs:
                    conn.execute(table.insert(), pv)

    def insert_abort_signals(self, signals, abt_id=None):
        ids = []
        with self.engine.begin() as conn:
            for signal in signals:
                result = conn.execute(self.tables['abort_signals'].insert(),
                                      signal)
                ids.append(result.inserted_primary_key)

            if abt_id:
                abt_ids = [{'abt_id': abt_id, 'abt_signal_id': i[0]}
                           for i in ids]
                conn.execute(self.tables['abort_list'].insert(), abt_ids)
            result.close()

        return ids

    def insert_abort(self, timestamp):
        with self.engine.begin() as conn:
            result = conn.execute(self.tables['aborts'].insert(),
                                  {'abt_time': timestamp})
            abt_id = result.inserted_primary_key

        return abt_id[0]

    def update_abort(self, abt_id, timestamp):
        with self.engine.begin() as conn:
            table = self.tables['aborts']
            stmt = (
                      table.update().
                      where(table.c.abt_id == abt_id).
                      values(abt_time=timestamp)
                    )
            result = conn.execute(stmt)
