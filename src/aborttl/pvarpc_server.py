import time
import argparse
from datetime import datetime

import pvaccess as pva

from .dbhandler import DbHandler


class AbortRPC(object):

    def __init__(self, uri):
        self._dh = DbHandler(uri)

    def data2annotation(self, data):
        time = []
        title = []
        tags = []
        text = []
        abt_id = None
        temp_text = ''

        for ts in data:
            if ts.msg == '':
                continue

            if abt_id != ts.abt_id:
                if temp_text:
                    text.append(str(temp_text))

                temp_text = ''
                abt_id = ts.abt_id
                date, nano = ts.ts.split('.')
                dt = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                time.append(int(dt.strftime('%s') + nano[0:3]))
                title.append(str(ts.msg))
                tags.append(str(ts.ring))

            if temp_text:
                temp_text += "</br>"
            temp_text += ts.ts + " " + ts.msg

        if temp_text:
            text.append(str(temp_text))

        return {"time": time, "title": title, "tags": tags, "text": text}

    def get_signals(self, x):
        try:
            starttime = x.getString('starttime')
            endtime = x.getString('endtime')
        except (pva.FieldNotFound, pva.InvalidRequest):
            return pva.PvBoolean(False)

        ring = x.getString('ring') if x.hasField('ring') else None
        msg = x.getString('message') if x.hasField('message') else ''

        # ex) 2019-01-01T00:00:00 => 2019-01-01 00:00:00
        starttime = starttime.replace("T"," ")
        endtime = endtime.replace("T"," ")

        timestamp_data = self._dh.fetch_abort_signals(ring=ring,
                                                      first=False,
                                                      include_no_abt_id=True,
                                                      sstart=starttime,
                                                      send=endtime)

        data = {'abt_id':[], 'time': [], 'msg': [], 'pvname': [], 'ring': []}
        for d in timestamp_data:
            data['abt_id'].append(d['abt_id'])
            data['time'].append(d['ts'])
            data['msg'].append(d['msg'])
            data['pvname'].append(d['pvname'])
            data['ring'].append(d['ring'])

        vals = {"column0": [pva.INT],
                "column1": [pva.STRING],
                "column2": [pva.STRING],
                "column3": [pva.STRING],
                "column4": [pva.STRING]}
        table = pva.PvObject({"labels": [pva.STRING], "value": vals},
                              'epics:nt/NTTable:1.0')
        table.setScalarArray("labels", ['abt_id', "time", "msg", "pvname", "ring"])
        table.setStructure("value", {"column0": data["abt_id"],
                                     "column1": data["time"],
                                     "column2": data["msg"],
                                     "column3": data["pvname"],
                                     "column4": data["ring"]})

        return table

    def get_annotations(self, x):
        try:
            starttime = x.getString('starttime')
            endtime = x.getString('endtime')
        except (pva.FieldNotFound, pva.InvalidRequest):
            return pva.PvBoolean(False)

        entity = x.getString('entity') if x.hasField('entity') else 'BOTH'
        msg = x.getString('message') if x.hasField('message') else ''

        if entity in ['LER', 'HER']:
            ring = entity
        else:
            entity = 'BOTH'
            ring = None

        # ex) 2019-01-01T00:00:00 => 2019-01-01 00:00:00
        starttime = starttime.replace("T"," ")
        endtime = endtime.replace("T"," ")

        timestamp_data = self._dh.fetch_abort_signals(ring=ring,
                                                      astart=starttime,
                                                      aend=endtime)

        ann = self.data2annotation(timestamp_data)

        vals = {"column0": [pva.ULONG],
                "column1": [pva.STRING],
                "column2": [pva.STRING],
                "column3": [pva.STRING]}
        table = pva.PvObject({"labels": [pva.STRING], "value": vals},
                             'epics:nt/NTTable:1.0')
        table.setScalarArray("labels", ["time", "title", "tags", "text"])
        table.setStructure("value", {"column0": ann["time"],
                                     "column1": ann["title"],
                                     "column2": ann["tags"],
                                     "column3": ann["text"]})

        return table

    def get_search(self, x):
        try:
            query = x.getString("entity")
        except (pva.FieldNotFound, pva.InvalidRequest):
            return pva.PvBoolean(False)

        value = [val for val in ["LER", "HER"] if val.startswith(query)]

        pv = pva.PvScalarArray(pva.STRING)
        pv.set(value)

        return pv


def parsearg():
    desc = "Abort timestamp API pvAccess RPC."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-c", "--channel", dest="ch", required=True,
                        help="Channel name")
    parser.add_argument("-u", "--uri", dest="uri",
                        default="sqlite:///:memory:", required=True,
                        help="URI to the database")

    return parser.parse_args()


def main():
    arg = parsearg()
    abort_rpc = AbortRPC(arg.uri)

    srv = pva.RpcServer()
    srv.registerService(arg.ch, abort_rpc.get_signals)
    srv.registerService(arg.ch + ":ann", abort_rpc.get_annotations)
    srv.registerService(arg.ch + ":search", abort_rpc.get_search)
    srv.startListener()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("exit")
    finally:
        alarm_rpc.close()


if __name__ == "__main__":
    main()
