import requests
import collections
import sys
import time

Entry = collections.namedtuple("Entry", "name stream_id rows")

ROW_TYPES = {}


def row_type_for_columns(name, column_names):
    row_type = ROW_TYPES.get((name, column_names))
    if row_type is None:
        row_type = collections.namedtuple(name, column_names)
        ROW_TYPES[(name, column_names)] = row_type
    return row_type


def parse_response(content):
    lines = content.split(b'\xff')
    index = 0
    result = {}
    while lines[index]:
        header = lines[index].split(b'\xfe')
        name, stream_id, rows, cols = header[:4]
        rows, cols = int(rows), int(cols)
        column_names = tuple(header[4 : 4 + cols])
        row_type = row_type_for_columns(name, column_names)
        data = [
            row_type(*line.split(b'\xfe')[:cols])
            for line in lines[index + 1 : index + 1 + rows]
        ]
        result[name] = Entry(name, stream_id, data)
        index += 1 + rows
    return result


def replicate(server, streams):
    return parse_response(requests.get(
        server + "/_synapse/replication",
        verify=False,
        params=streams
    ).content)


def main():
    server = sys.argv[1]

    streams = None
    while not streams:
        try:
            streams = {
                row.name: row.stream_id
                for row in replicate(server, {"streams":"-1"})["streams"].rows
            }
        except:
            time.sleep(0.1)

    while True:
        try:
            results = replicate(server, streams)
        except:
            sys.stdout.write("connection_lost()\n")
            break
        for update in results.values():
            for row in update.rows:
                sys.stdout.write(repr(row) + "\n")
            streams[update.name] = update.stream_id

        time.sleep(0.1)


if __name__=='__main__':
    main()
