#! /usr/bin/env python
###########################################################
# This file has been created by ARSF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################

"""
Cron job for web processing, picks up config files and passes them to web qsub

Author: Stephen Goult

Available functions
main(): finds all config files and tests them for submission requirements.
"""

import sys
if sys.version_info[0] < 3:
   import ConfigParser
else:
   import configparser as ConfigParser
import os
import web_common

from arsf_dem import dem_common_functions

def main():
   """
   This iterates over all the config files available and updates so they won't
   double submit or overlap. It should be updated to throttle it'self if a lot
   of jobs are already on the grid/being processed locally
   """
   for configfile in os.listdir(web_common.WEB_CONFIG_DIR):
      #assume we want to submit stuff until we find evidence to the contrary
      if ".cfg" not in configfile[-4:]:
         continue
      submit = True
      config = ConfigParser.SafeConfigParser()
      config.read(web_common.WEB_CONFIG_DIR + "/" + configfile)

      if config.get("DEFAULT", "submitted") in "True":
         #we don't want to submit twice
         submit = False

      if config.get("DEFAULT", "confirmed") in "False":
         #if it hasn't been confirmed its not being submitted
         submit = False

      if config.get("DEFAULT", "bandratio") in "True":
         #if they said they wanted to bandratio but it isn't finished we shouldn't continue
         if config.get("DEFAULT", "bandratioset") in "False" and config.get("DEFAULT", "bandratiomappedset") in "False":
            submit = False
            #TODO if its existed for more than a day send a reminder with a link
            #to the band ratio page, maybe a cancellation option?

      if submit:
         #finally submit the jobs
         print web_common.QSUB_COMMAND
         qsub = [web_common.QSUB_COMMAND]
         qsub.extend(["-c", "{}/{}".format(web_common.WEB_CONFIG_DIR, configfile)])
         if web_common.FORCE_LOCAL:
            qsub.extend(["--local"])
         dem_common_functions.CallSubprocessOn(qsub)

if __name__ == '__main__':
   main()
