#! /usr/bin/env python
import gdal
import numpy
import numexpr
import scipy
import os

def bandmath(bilfiles, equation, outputfolder, bands, eqname=None):
   """
   Function to run a string equation, hand in a bilfile and a list of bands
   required.

   Bands in the equation must be in the format "bandx" where x is the number
   (e.g. band1, band147, band650) if this standard is not kept numexpr will
   throw an exception
   """
   for bilfile in bilfiles:
      print bilfile
      bil = gdal.Open(bilfile)
      print bil
      banddict = {}
      for band in bands:
         banddict["band{}".format(band)] = bil.GetRasterBand(band).ReadAsArray()
      result = numexpr.evaluate(equation,
                                local_dict=banddict)
      if eqname is None:
         equation_clean = equation.replace("*", "x").replace("/", "÷").replace(" ", "_")
      else:
         equation_clean = eqname
      rows, cols = numpy.shape(result)
      destination = gdal.GetDriverByName('ENVI').Create(outputfolder
                                                      + "/"
                                                      + os.path.basename(bilfile).replace(".bil", "") 
                                                      + equation_clean
                                                      + ".bil",
                                                      cols + 1,
                                                      rows + 1,
                                                      1,
                                                      gdal.GDT_Float32)
      outband = destination.GetRasterBand(1)
      outband.WriteArray(result)
      outband.FlushCache()

def main():
   return "test2"

if __name__ == main:
   main()
