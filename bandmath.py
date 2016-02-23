#! /usr/bin/env python
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
import scipy
import os
import argparse
import re

def bandmath(bilfile, equation, outputfolder, bands, eqname=None, maskfile=None):
   """
   Function to run a string equation, hand in a bilfile and a list of bands
   required.

   Bands in the equation must be in the format "bandx" where x is the number
   (e.g. band1, band147, band650) if this standard is not kept numexpr will
   throw an exception
   """
   bil = gdal.Open(bilfile)
   banddict = {}
   for band in bands:
      banddict["band{}".format(band)] = bil.GetRasterBand(int(band)).ReadAsArray()
   result = numexpr.evaluate(equation,
                             local_dict=banddict)
   if eqname is None:
      equation_clean = equation.replace("*", "x").replace("/", '').replace(" ", "_")
   else:
      equation_clean = eqname
   try:
      layers, rows, cols = numpy.shape(result)
   except:
      rows, cols = numpy.shape(result)
      layers = 1

   output_name = outputfolder + "/" + os.path.basename(bilfile).replace(".bil", "") + equation_clean + ".bil"
   destination = gdal.GetDriverByName('ENVI').Create(output_name,
                                                   cols,
                                                   rows,
                                                   1,
                                                   gdal.GDT_Float32)
   outband = destination.GetRasterBand(1)
   outband.WriteArray(result)
   destination = None

   if maskfile is not None:
      maskbil = gdal.Open(maskfile)
      if layers == 1:
         #if it's one we need to combine it down
         maskarrays=[]
         basemask=numpy.zeros((rows, cols), dtype=float)
         for band in bands:
            numpy.add(maskbil.GetRasterBand(int(band)).ReadAsArray(), basemask)
      if layers > 1:
         maskarray = maskbil.ReadAsArray()
      destination = gdal.GetDriverByName('ENVI').Create(output_name.replace(".bil", "_mask.bil"),
                                                cols,
                                                rows,
                                                layers,
                                                gdal.GDT_Byte)
      for i in range(layers):
         outband = destination.GetRasterBand(i+1)
         if layers == 1:
            outband.WriteArray(result)
         else:
            outband.WriteArray(result[i])
      outband = None
      destination = None


   return output_name, layers

if __name__ == '__main__':
   parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument('--equation',
                       '-e',
                       help='string equation presented in quotes, e.g. "(band001 / band002)"',
                       default=None,
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
   bandmath(args.bilfile, args.equation, output, bands, eqname=args.ename, maskfile=args.maskfile)
