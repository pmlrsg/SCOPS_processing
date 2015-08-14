#! /usr/bin/env python
__author__ = 'stgo'

import os
import common_functions

WEB_CONFIG_DIR = "/users/rsg/arsf/web_processing/configs/"

def main():
    for config in os.listdir(WEB_CONFIG_DIR):
        process = True
        for line in open(WEB_CONFIG_DIR + config, 'r'):
            if "submitted" in line:
                process = False

        if process:
            #using local until happy that all processing works
            common_functions.CallSubprocessOn(["web_qsub.py", "-c", WEB_CONFIG_DIR + config, "--local"])