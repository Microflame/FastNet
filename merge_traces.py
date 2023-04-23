import json

TAIL = 16 * 1024

print('Read client')
client = json.load(open('trace_client.json'))[-TAIL:]
print('Read server')
server = json.load(open('trace_server.json'))[-TAIL:]

print('Patch client')
for t in client:
    t['pid'] = 'Client'
    t['tid'] = 'Client'

print('Patch server')
for t in server:
    t['pid'] = 'Server'
    t['tid'] = 'Server'

print('Merge')
merged = client + server

print('Save')
json.dump(merged, open('trace.json', 'w'))