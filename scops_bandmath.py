#! /usr/bin/env python

###########################################################
# This file has been created by the NERC-ARF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################
"""
Performs array mathematics on compatible gdal files, can't be used on seperate
files unless it is extended to subset seperate datasets.

Author: Stephen Goult

Available functions
bandmath: runs a given equation on a gdal compatible file and optionally
generates a maskfile to be used in apl
"""
from __future__ import print_function

import gdal
import numpy
import numexpr
import os
import argparse
import re

def bandmath_mask_gen(maskfile, output_name, bands, layers, rows, cols):
    maskbil = gdal.Open(maskfile)
    if layers == 1:
        #if it's one we need to combine it down
        maskarrays=[]
        basemask=numpy.zeros((rows, cols), dtype=float)
        for band in bands:
            numpy.add(maskbil.GetRasterBand(int(band)).ReadAsArray(), basemask)
        maskarray=basemask
    if layers > 1:
        maskarray = maskbil.ReadAsArray()
    destination = gdal.GetDriverByName('ENVI').Create(output_name,
                                              cols,
                                              rows,
                                              layers,
                                              gdal.GDT_Byte,
                                              ["INTERLEAVE=BIL"])
    for i in range(layers):
        outband = destination.GetRasterBand(i+1)
        if layers == 1:
            outband.WriteArray(maskarray)
        else:
            outband.WriteArray(maskarray[i])
    outband = None
    destination = None

def bandmath(bilfile, equation, outputfolder, bands, eqname=None, maskfile=None, badpix_mask=None):
    """
    Function to run a string equation, hand in a bilfile and a list of bands
    required.

    Bands in the equation must be in the format "bandx" where x is the number
    (e.g. band1, band147, band650) if this standard is not kept numexpr will
    throw an exception
    """
    bil = gdal.Open(bilfile)
    banddict = {}
    #we need to build a dictionary of band variables to hand in to numexpr
    print(bands)
    for band in bands:
        print(band)
        banddict["band{}".format(band)] = \
                  bil.GetRasterBand(int(band)).ReadAsArray().astype(numpy.float32)
    #local dict becomes the variable list
    print(banddict)
    result = numexpr.evaluate(equation,
                              local_dict=banddict)
    #need to come up with something sernsible to use as the name
    if eqname is None:
        equation_clean = equation.replace("*", "x").replace("/", '').replace(" ", "_")
    else:
        equation_clean = eqname
    #see if there is more than one band output from numexp
    try:
        layers, rows, cols = numpy.shape(result)
    except:
        #if not we only have one band to deal with
        rows, cols = numpy.shape(result)
        layers = 1

    #build a clever output name
    output_name = os.path.join(outputfolder,
                               os.path.basename(bilfile).replace(".bil", "") +
                               "_{}.bil".format(equation_clean))
    #need to have a destination dataset before gdal will let us write out
    destination = gdal.GetDriverByName('ENVI').Create(output_name,
                                                    cols,
                                                    rows,
                                                    1,
                                                    gdal.GDT_Float32,
                                                    ["INTERLEAVE=BIL"])
    for i in range(layers):
        outband = destination.GetRasterBand(i+1)
        if layers == 1:
            outband.WriteArray(result)
        else:
            outband.WriteArray(result[i])
    outband = None
    destination = None

    if maskfile is not None:
        bandmath_mask_gen(maskfile, output_name.replace(".bil", "_mask.bil"), bands, layers, rows, cols)

    if badpix_mask is not None:
        bandmath_mask_gen(badpix_mask, output_name.replace(".bil","_mask-badpixelmethod.bil"), bands, layers, rows, cols)

    return output_name, layers




if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--equation',
                        '-e',
                        help='string equation presented in quotes, e.g. "(band001 / band002)"',
                        metavar="<equation>")
    parser.add_argument('--output_folder',
                        '-o',
                        help='output folder',
                        default=None,
                        metavar="<output_folder>")
    parser.add_argument('--maskfile',
                        '-m',
                        help='mask file',
                        default=None,
                        metavar="maskfile")
    parser.add_argument('--ename',
                        '-n',
                        help='equation name to append to file, defaults to input equation',
                        default=None,
                        metavar="ename")
    parser.add_argument('bilfile',
                        help='bil file to run on',
                        default=None,
                        metavar="bilfile")
    args = parser.parse_args()

    bands = re.findall(r'band(\d{3})', args.equation)

    if args.output_folder is None:
        output = os.getcwd()
    else:
        output = args.output_folder
    print("Will perform {} on bands {}".format(args.equation, ", ".join(bands)))
    output_name, layers = bandmath(args.bilfile, args.equation, output,
                                   bands, eqname=args.ename,
                                   maskfile=args.maskfile)
    print("Wrote to: {}".format(output_name))
