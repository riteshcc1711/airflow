import json
json_path = '/usr/local/custom/input.json'
with open(json_path, 'r') as config:
    params = json.load(config)["params"]