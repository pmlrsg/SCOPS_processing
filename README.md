# The Simple Concurrent Online Processing System (SCOPS) #

## About ##

Backend code for SCOPS. For a full description see the following paper:

M.A. Warren, S. Goult, D. Clewley,
The Simple Concurrent Online Processing System (SCOPS) - An open-source interface for remotely sensed data processing,
Computers & Geosciences,
Volume 115,
2018,
Pages 188-197,
ISSN 0098-3004,
https://doi.org/10.1016/j.cageo.2018.03.013.

## Installation ##

### Pre-requsites ###

* APL (https://github.com/arsf/apl / https://nerc-arf-dan.pml.ac.uk/trac/wiki/Downloads/software/)
* NERC-ARF DEM Scripts (https://github.com/pmlrsg/arsf_dem_scripts)
* GDAL (http://gdal.org/)
* NumExpr (https://github.com/pydata/numexpr)

APL and NERC-ARF DEM scripts will need to be installed from source, GDAL and NumExpr can be installed using your package manager.

### SCOPS ###

To install use:
```
python setup.py install
```
By default this will install to `/usr/local`, you can override this by setting `--prefix`.


## Config ##

For SCOPS to work on your system a number of environmental variables need to be set.
The following is an illustration of the set up required for setup on JASMIN.

```bash
export ERROR_EMAIL=me@my.domain # Email to send error messages to
export WEB_CONFIG_DIR=/home/users/dac/arsf_group_workspace/dac/web_processor_test/configs/ # Directory for config files
export WEB_OUTPUT=/home/users/dac/arsf_group_workspace/dac/web_processor_test/processing/ # Directory for ouput foles
export QSUB_LOG_DIR=/home/users/dac/arsf_group_workspace/dac/web_processor_test/logs/  # Directory for log files
export HYPER_DELIVERY_FOLDER=/hyperspectral # Directory hyperspectral delivery files are stored within
export TEMP_PROCESSING_DIR="" # Directory for local temporary processing (if not set will use WEB_OUTPUT"
export QSUB_SYSTEM=bsub  # System to use for submitting jobs, e.g, bsub, qsub, or local for local processing
export QUEUE=short-serial # Queue to use for jobs
```


## Plugins ##

Follow these instructions to add plugins for further processing options:

1. Add a python script in to plugins directory. This will be picked up by the SCOPS front-end and added into the band math further processing page for selection.
 - the plugin must have a function called run which does all the processing and returns the processed data filename
2. Edit the plugin_args dictionary in scops_process_apl_line.py to add any keyword arguments required for the plugin run function
