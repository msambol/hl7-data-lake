import json
from hl7_parser import *

event = '../test/event_1.json'

with open(event) as fp:
    data = json.load(fp)

handler(data, '')
