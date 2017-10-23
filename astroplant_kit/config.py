import json

def read_config():
    with open('./kit_config.json') as f:
        data = json.load(f)

    return data
