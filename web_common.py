#!/usr/bin/env python
###########################################################
# This file has been created by ARSF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################

"""
Contains main config variables for the processor chain, these are paths to
executables and default preformat strings. In future this will try to autodetect
best settings but for the moment will just act as a lazy config file until I have
time to update it
"""
import os

#main executable location
COMMON_LOCATION = os.path.dirname(os.path.realpath(__file__))

#forces processing to occur on the machine running the submission cron job
FORCE_LOCAL = False

#the location of the main processing executable
PROCESS_COMMAND = COMMON_LOCATION + "/web_process_apl_line.py"

#the following are output folders, they shouldn't need to be changed and will all exist under the main processing folder
#mask output location
WEB_MASK_OUTPUT = "/level1b/"

#level1b folder relative to delivery
LEV1_FOLDER = "/flightlines/level1b/"

#igm output location
WEB_IGM_OUTPUT = "/igm/"

#mapped file output
WEB_MAPPED_OUTPUT = "/mapped/"

#status file output
STATUS_DIR = "/status/"

#error file dir, must be the same as status
ERROR_DIR = "/status/"

#processing logs file, different to the main web app log folder!
LOG_DIR = "/logs/"

#navigation files folder in the hyperspectral delivery
NAVIGATION_FOLDER = "/flightlines/navigation/"

#core processing output location, workspaces are spawned here
WEB_OUTPUT = "/users/rsg/arsf/web_processing/processing/"

#main dem output folder
WEB_DEM_FOLDER = "/dem/"

#status output folder
WEB_STATUS_OUTPUT = "/status"

#first status, doesn't really matter what this is called
INITIAL_STATUS = "initialising"

#main config output/ingest point
WEB_CONFIG_DIR = "/users/rsg/arsf/web_processing/configs/"

#qsub log dir
QSUB_LOG_DIR = "/users/rsg/arsf/web_processing/logs/qsub/"

#location of the seperation file for UK BNG projections in grass
OSNG_SEPERATION_FILE = "/users/rsg/arsf/dems/ostn02/OSTN02_NTv2.gsb"

SERVER_BASE = 'https://arsf-dandev.nerc.ac.uk'


#both these link variables need to be updated to the main processor URL
DOWNLOAD_LINK = SERVER_BASE + '/processor/downloads/{}?&project={}'

#http location to access the status page
STATUS_LINK = SERVER_BASE +'/processor/status/{}?&project={}'

#it's preferable these aren't updated as they are needed by the web front end,
#if they really have to be they should be updated in arsf_process_page.py as well.
LOG_FILE = "{}/" + LOG_DIR + "/{}_log.txt"

#base status file location and name
STATUS_FILE = "{}/" + STATUS_DIR + "/{}_status.txt"

#where the view vectors can be found
VIEW_VECTOR_FILE = "/sensor_FOV_vectors/{}_fov_fullccd_vectors.bil"

#location of the main qsub command
QSUB_COMMAND = os.path.abspath(os.path.join(__file__, os.pardir)) + '/' + 'web_qsub.py'

#The main queue to be submitted to
QUEUE = "lowpriority.q"

#sender of all emails
SEND_EMAIL = "arsf-processing@pml.ac.uk"

#email to bcc anything sent to
BCC_EMAIL = "arsf-code@pml.ac.uk"

#address errors will be sent to
#TODO change from stgo!
ERROR_EMAIL = "stgo@pml.ac.uk"
