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
import web_common

def main():
   """
   This iterates over all the config files available and updates so they won't
   double submit or overlap. It should be updated to throttle it'self if a lot
   of jobs are already on the grid/being processed locally
   """
   for configfile in os.listdir(web_common.WEB_CONFIG_DIR):
      submit = True
      config = ConfigParser.SafeConfigParser()
      config.read(web_common.WEB_CONFIG_DIR + "/" + configfile)

      if config.get("DEFAULT", "submitted") in "True":
         submit = False

      if config.get("DEFAULT", "confirmed") in "False":
         submit = False

      if config.get("DEFAULT", "bandratio") in "True":
         if config.get("DEFAULT", "bandratioset") in "False" and config.get("DEFAULT", "bandratiomappedset") in "False":
            submit = False

      if submit:
         # using local until happy that all processing works
         common_functions.CallSubprocessOn([web_common.QSUB_COMMAND, "-c", web_common.WEB_CONFIG_DIR + "/" + configfile, "--local"])

if __name__ == '__main__':
   main()
