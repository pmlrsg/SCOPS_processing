#! /usr/bin/env python
import gdal
import numpy
import numexpr
import scipy
import os

def bandmath(bilfile, equation, outputfolder, bands, eqname=None, maskfile=None):
   """
   Function to run a string equation, hand in a bilfile and a list of bands
   required.

   Bands in the equation must be in the format "bandx" where x is the number
   (e.g. band1, band147, band650) if this standard is not kept numexpr will
   throw an exception
   """
   bil = gdal.Open(bilfile)
   maskbil = gdal.Open(maskfile)
   banddict = {}
   for band in bands:
      banddict["band{}".format(band)] = bil.GetRasterBand(int(band)).ReadAsArray()
   result = numexpr.evaluate(equation,
                             local_dict=banddict)
   print "test"
   if eqname is None:
      equation_clean = equation.replace("*", "x").replace("/", '').replace(" ", "_")
   else:
      equation_clean = eqname
   print numpy.shape(result)
   try:
      layers, rows, cols = numpy.shape(result)
   except:
      rows, cols = numpy.shape(result)
      layers = 1
   print rows, cols
   output_name = outputfolder + "/" + os.path.basename(bilfile).replace(".bil", "") + equation_clean + ".bil"
   destination = gdal.GetDriverByName('ENVI').Create(output_name,
                                                   cols,
                                                   rows,
                                                   1,
                                                   gdal.GDT_Float32)
   outband = destination.GetRasterBand(1)
   outband.WriteArray(result)
   destination = None

   print "test"
   if not maskfile is None:
      if layers == 1:
         #if it's one we need to combine it down
         maskarrays=[]
         basemask=numpy.zeros((rows, cols), dtype=float)
         print "test2"
         for band in bands:
            print "test"
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


def main():
   return "test2"

if __name__ == main:
   main()
