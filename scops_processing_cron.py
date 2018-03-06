#!/usr/bin/env python
###########################################################
# This file has been created by NERC-ARF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################

"""
Cron job for web processing, picks up config files and passes them to web qsub

Author: Stephen Goult

Available functions
main(): finds all config files and tests them for submission requirements.
"""

from __future__ import print_function

import sys
if sys.version_info[0] < 3:
    import ConfigParser
else:
    import configparser as ConfigParser
import os

from scops import scops_common

from arsf_dem import dem_common_functions

def main():
    """
    This iterates over all the config files available and updates so they won't
    double submit or overlap. It should be updated to throttle it'self if a lot
    of jobs are already on the grid/being processed locally
    """
    for configfile in os.listdir(scops_common.WEB_CONFIG_DIR):
        #assume we want to submit stuff until we find evidence to the contrary
        print(configfile)
        if ".cfg" not in configfile[-4:]:
            continue
        submit = True
        config = ConfigParser.SafeConfigParser()
        config.read(scops_common.WEB_CONFIG_DIR + "/" + configfile)

        if config.has_option("DEFAULT", "ftp_dem"):
            if config.getboolean("DEFAULT", "ftp_dem"):
                submit = False
                if config.getboolean("DEFAULT", "ftp_dem_confirmed"):
                    submit = True

        if config.getboolean("DEFAULT", "submitted"):
            #we don't want to submit twice
            submit = False

        if not config.getboolean("DEFAULT", "confirmed"):
            #if it hasn't been confirmed its not being submitted
            submit = False

        if config.getboolean("DEFAULT", "bandratio"):
            #if they said they wanted to bandratio but it isn't finished we shouldn't continue
            if not config.getboolean("DEFAULT", "bandratioset") and not config.getboolean("DEFAULT", "bandratiomappedset"):
                submit = False
                #TODO if its existed for more than a day send a reminder with a link
                #to the band ratio page, maybe a cancellation option?

        if config.getboolean("DEFAULT", "restart"):
            submit = True

        if config.getboolean("DEFAULT", "has_error"):
            submit = False

        if submit:
            #finally submit the jobs
            print(scops_common.QSUB_COMMAND)
            qsub = [scops_common.QSUB_COMMAND]
            qsub.extend(["-c", "{}/{}".format(scops_common.WEB_CONFIG_DIR, configfile)])
            if scops_common.FORCE_LOCAL:
                qsub.extend(["--local"])
            dem_common_functions.CallSubprocessOn(qsub)

if __name__ == '__main__':
    main()
