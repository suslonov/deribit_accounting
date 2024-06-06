#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil import tz

def localize_time(dt):
    return dt.replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())

class MyException(Exception):
    pass

def list_to_dict(l, field):
    return {v[field]: v for v in l if field in v}

def list_to_dict_multi(l, fields):
    return {v[field]: v for v in l for field in fields if field in v}

def instrument_dict(l):
    return list_to_dict_multi(l, ['instrument', 'instrument_name'])

def print_log(*message):
    print(datetime.utcnow(), *message, flush=True)
    
def diff_hours(now_date, prev_state):
    return (now_date - prev_state["datetime"]).total_seconds()/3600
