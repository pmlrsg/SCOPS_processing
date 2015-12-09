#! /usr/bin/env python
"""
Cron job for web processing, picks up config files and passes them to web qsub

Author: Stephen Goult

Available functions
main(): finds all config files and tests them for submission requirements.
"""

import sys
#We need to add these so that crontab has the right path(s)
sys.path.insert(0, '/users/rsg/arsf/usr/lib/python/site-packages')
sys.path.insert(0, '/users/rsg/arsf/usr/bin')
sys.path.insert(0, '/users/rsg/arsf/usr/lib_python_links')
import common_functions
import ConfigParser
import os

WEB_CONFIG_DIR = "/users/rsg/arsf/web_processing/configs/"
QSUB_COMMAND = "web_qsub.py"



def main():
   for configfile in os.listdir(WEB_CONFIG_DIR):
      submit = True
      config = ConfigParser.SafeConfigParser()
      config.read(WEB_CONFIG_DIR + configfile)

      if config.get("DEFAULT", "submitted") in "True":
         submit = False

      if config.get("DEFAULT", "confirmed") in "False":
         submit = False

      if submit:
         # using local until happy that all processing works
         common_functions.CallSubprocessOn([QSUB_COMMAND, "-c", WEB_CONFIG_DIR + configfile, "--local"])

if __name__ == '__main__':
   main()
