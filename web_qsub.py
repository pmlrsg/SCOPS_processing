#! /usr/bin/env python
import os
import folder_structure
import common_functions
import datetime
import ConfigParser
import web_process_apl_line
import argparse
from arsf_dem import dem_nav_utilities
import glob

WEB_OUTPUT = "/users/rsg/stgo/network_scratch/web_testing/processing/"
WEB_MASK_OUTPUT = "/level1b/"
WEB_IGM_OUTPUT = "/igm/"
WEB_MAPPED_OUTPUT = "/mapped/"
WEB_DEM_FOLDER = "/dem/"
VIEW_VECTOR_FILE = "/sensor_FOV_vectors/%s_view_vector_list.txt"
OSNG_SEPERATION_FILE = "/users/rsg/arsf/dems/ostn02/OSTN02_NTv2.gsb"
WEB_STATUS_OUTPUT = "/status"
STATUS_FILE = "%s/status/%_status.txt"
INITIAL_STATUS = "initialising"


def web_structure(project_code, jday, year, sortie=None, output_name=None):
    if output_name is not None:
        folder_base=output_name
    else:
        if sortie is not None:
            folder_base = WEB_OUTPUT + project_code + '_' + year + '_' + jday + sortie + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        else:
            folder_base = WEB_OUTPUT + project_code + '_' + year + '_' + jday + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    os.mkdir(folder_base)
    os.mkdir(folder_base + WEB_MASK_OUTPUT)
    os.mkdir(folder_base + WEB_IGM_OUTPUT)
    os.mkdir(folder_base + WEB_MAPPED_OUTPUT)
    os.mkdir(folder_base + WEB_DEM_FOLDER)
    os.mkdir(folder_base + WEB_STATUS_OUTPUT)
    return folder_base


def web_qsub(config, local=False, local_threaded=False, output=None):
    config_file = ConfigParser.RawConfigParser()
    config_file.read(config)
    lines = config_file.sections()
    defaults = config_file.defaults()
    if output is not None:
        output_location = web_structure(defaults["project_code"], defaults["julianday"], defaults["year"], defaults["sortie"], output)
    else:
        try:
            output_location = config_file.get('DEFAULT', 'output_folder')
        except:
            output_location = web_structure(defaults["project_code"], defaults["julianday"], defaults["year"], defaults["sortie"])
            config_file.set('DEFAULT', 'output_folder', output_location)

    folder = folder_structure.FolderStructure(year=defaults["year"],
                                              jday=defaults["julianday"],
                                              projectCode=defaults["project_code"],
                                              fletter=defaults["sortie"],
                                              absolute=True)

    hyper_delivery = glob.glob(folder.getProjPath() + "/delivery/*hyperspectral*/")[0]
    nav_folder = glob.glob(hyper_delivery + "flightlines/navigation/")[0]

    try:
        dem_name = config_file.get('DEFAULT', 'dem_name')
    except Exception, e:
        common_functions.ERROR(e)
        dem_name = (output_location + WEB_DEM_FOLDER + defaults["project_code"] + '_' + defaults["year"] + '_' + defaults["julianday"] + '_' + defaults["projection"] + ".dem").replace(' ', '_')
        dem_nav_utilities.create_apl_dem_from_mosaic(dem_name,
                                                     dem_source=defaults["dem"],
                                                     bil_navigation=nav_folder)

    config_file.set('DEFAULT', "dem_name", dem_name)
    config_file.write(open(config, 'w'))

    for line in lines:
        if dict(config_file.items(line))["process"]:
            if local:
                if local_threaded:
                    #do threaded processing TODO
                    pass
                else:
                    web_process_apl_line.process_web_hyper_line(config, line, output_location)
            else:
                #build a qsub command TODO
                pass


if __name__=='__main__':
   #Get the input arguments
   parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument('--config',
                       '-c',
                       help='web config file',
                       default=None,
                       required=True,
                       metavar="<configfile>")
   parser.add_argument('--local',
                       '-l',
                       help='local processing vs grid',
                       action='store_true',
                       default=False)
   parser.add_argument('--threaded',
                       '-t',
                       help='multithreaded local processing',
                       default=False,
                       metavar="<sensor>")
   parser.add_argument('--output',
                       '-o',
                       help='Force output path and name',
                       default=None,
                       metavar="<folder_name>")
   args = parser.parse_args()


   web_qsub(args.config, local=args.local, local_threaded=args.threaded, output=args.output)