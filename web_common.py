#!/usr/bin/env python
#
# processor_config
# Author: Stephen Goult
"""
Contains main config variables for the processor chain, these are paths to
executables and default preformat strings. In future this will try to autodetect
best settings but for the moment will just act as a lazy config file until I have
time to update it
"""

PROCESS_COMMAND = "users/rsg/stgo/PycharmProjects/ARSF_web_processor/web_process_apl_line.py"

WEB_MASK_OUTPUT = "/level1b/"

WEB_IGM_OUTPUT = "/igm/"

WEB_MAPPED_OUTPUT = "/mapped/"

STATUS_DIR = "/status/"

ERROR_DIR = "/status/"

LOG_DIR = "/logs/"

NAVIGATION_FOLDER = "/flightlines/navigation/"

WEB_OUTPUT = "/users/rsg/arsf/web_processing/processing/"

WEB_DEM_FOLDER = "/dem/"

WEB_STATUS_OUTPUT = "/status"

INITIAL_STATUS = "initialising"

WEB_CONFIG_DIR = "/users/rsg/arsf/web_processing/configs/"



OSNG_SEPERATION_FILE = "/users/rsg/arsf/dems/ostn02/OSTN02_NTv2.gsb"



DOWNLOAD_LINK = 'https://arsf-dandev.nerc.ac.uk/processor/downloads/{}?&project={}'

STATUS_LINK = 'https://arsf-dandev.nerc.ac.uk/processor/status/{}?&project={}'



#it's preferable these aren't updated as they are needed by the web front end,
#if they really have to be they should be updated in arsf_process_page.py as well.
LOG_FILE = "{}/" + LOG_DIR + "/{}_log.txt"

STATUS_FILE = "{}/" + STATUS_DIR + "/{}_status.txt"

VIEW_VECTOR_FILE = "/sensor_FOV_vectors/{}_fov_fullccd_vectors.bil"


QSUB_COMMAND = "/users/rsg/stgo/PycharmProjects/ARSF_web_processor/web_qsub.py"

QUEUE = "lowpriority.q"


SEND_EMAIL = "arsf-processing@pml.ac.uk"

ERROR_EMAIL = "stgo@pml.ac.uk" #TODO change from stgo!
