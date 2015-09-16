#! /usr/bin/env python
__author__ = 'stgo'

import os
import common_functions
import ConfigParser

WEB_CONFIG_DIR = "/users/rsg/arsf/web_processing/configs/"


def main():
   for configfile in os.listdir(WEB_CONFIG_DIR):
      submit = True
      config = ConfigParser.RawConfigParser()
      config.read(WEB_CONFIG_DIR + configfile)

      if config.get("DEFAULT", "submitted") is True:
         submit = False

      if config.get("DEFAULT", "confirmed") is False:
         submit = False

      if submit:
         # using local until happy that all processing works
         common_functions.CallSubprocessOn(["/users/rsg/stgo/PycharmProjects/ARSF_web_processor/web_qsub.py", "-c", WEB_CONFIG_DIR + configfile, "--local"])

if __name__ == '__main__':
   main()