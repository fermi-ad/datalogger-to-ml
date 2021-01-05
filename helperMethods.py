#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 09:16:00 2021

@author: snag
"""
import requests

def readURL(DEVICE_LIST):
    url = 'https://github.com/fermi-controls/linac-logger-device-cleaner/releases/latest/download/linac_logger_drf_requests.txt'
    req = requests.get(url)

    if req.status_code == requests.codes.ok:
        DEVICE_LIST = [line.rstrip() for line in req.text.split('\n') if line] # Non-blank lines
        return DEVICE_LIST
    else:
        print('Could not fetch ' + url)
