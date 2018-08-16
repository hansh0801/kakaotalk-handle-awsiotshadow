# -*- coding: utf-8 -*-
import json


def lambda_handler(event, context):
    # TODO implement

    keyboard ={'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF'], 'message': {'text': 'hello world'}}
    #data = json.dumps(keyboard)
    #hello = json.loads(data)

    return keyboard
