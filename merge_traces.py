import json

client = json.load(open('trace_client.json'))
server = json.load(open('trace_server.json'))

# patch client:
for t in client:
    t['pid'] = 'Client'
    t['tid'] = 'Client'

# patch server:
for t in server:
    t['pid'] = 'Server'
    t['tid'] = 'Server'

merged = client + server

json.dump(merged, open('trace.json', 'w'))