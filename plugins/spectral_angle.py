#!/usr/bin/env python
#-*- coding:utf-8 -*-

from __future__ import print_function
import numpy
import gdal
import math
import glob
import os
import collections
import argparse

nodata=2**16-1

def calculate_spectral_angle(hsi_filename,spectra):
    """
    Run a spectral angle classification on a hyperspectral file (hsi_filename) using reference spectra

    hsi_filename = string filename
    spectra = 2D numpy array - each row a different spectra

    returns a dictionary of key (spectra id) and value (spectral angle)
    """
    #open the gdal supported raster file
    gdalfile=gdal.Open(hsi_filename)
    #get the number of bands in the file
    number_of_bands=gdalfile.RasterCount
    if number_of_bands!=spectra.shape[1]:
        raise Exception("The reference spectra must be sampled to the same wavelengths as the hyperspectral data.")
    #define the arrays and set to zero
    hsi_magnitude=numpy.zeros([gdalfile.RasterYSize,gdalfile.RasterXSize])
    spectra_magnitude=numpy.zeros(spectra.shape[0])
    dot_sum=numpy.zeros([spectra.shape[0],gdalfile.RasterYSize,gdalfile.RasterXSize])
    #loop through each of the bands and calculate the values we need
    for i,band in enumerate(range(1,number_of_bands+1)):
        print("Working on band: {}/{}".format(i,number_of_bands))
        band_data=gdalfile.GetRasterBand(band).ReadAsArray().astype(numpy.float32)
        #sum up the band squared to calculate the magnitude later
        hsi_magnitude=hsi_magnitude+(band_data*band_data)
        for spec_index in range(spectra.shape[0]):
            #sum up the spectra squared to calculate the magnitude later
            spectra_magnitude[spec_index]=spectra_magnitude[spec_index]+(spectra[spec_index][i]*spectra[spec_index][i])
            #multiply each band by the spectra scalar
            dot_sum[spec_index]=dot_sum[spec_index]+(band_data*spectra[spec_index][i])

    #square root to get magnitudes
    hsi_magnitude=numpy.sqrt(hsi_magnitude)
    spectra_magnitude=numpy.sqrt(spectra_magnitude)
    #get the cosine of the angle
    angle=collections.OrderedDict()
    for i in range(spectra_magnitude.size):
        #need to deal with denominator being zero else RAM usage hits the roof
        #set it to 1 instead and then set the cos_angle to 0 (90 degrees) for these pixels
        denom=hsi_magnitude*spectra_magnitude[i]
        denom=numpy.where(denom==0,1,denom)
        cos_angle=dot_sum[i]/denom
        cos_angle[numpy.where(denom==1)]=0
        #get the angle
        angle[i+1]=numpy.arccos(cos_angle)
    #tidy up
    gdalfile=None
    return angle

def create_classification_mask(angles):
    """
    Create the classification mask.
    Input is a dictionary of spectral angles (as output from calculate_spectral_angle).
    The keys should be 0, 1, 2, ... for as many reference spectra used.
    Output is the classification mask array.
    """

    #now to generate the classification mask - and set to max value
    classification=numpy.ones(angles[1].shape)*(nodata)
    #the scalar to convert indices to floating point
    scalar=10**numpy.ceil(numpy.log10(angles[1].shape[1]))
    #for each of the classes (test spectra)
    for classkey in angles.keys():
        indices=[]
        #loop through each of the other classes and test if this one has smaller angle
        for otherkey in angles.keys():
            if otherkey is classkey:
                continue
            indices.append(numpy.where(angles[classkey]<angles[otherkey]))

        #now need to pull out only the indices that occur in every set
        float_indices=[]
        for i in indices:
            #convert to a float as row.col for comparison
            float_indices.append(i[0]+i[1]/scalar)

        #get the counts of each unique occurrence
        #if the count is the same as the number of other classes (i.e. total classes - 1) then it is OK
        all_float_indices=numpy.concatenate(float_indices)
        unique,counts=numpy.unique(all_float_indices,return_counts=True)
        #now get the indices that were in all sets
        float_indices=unique[numpy.where(counts==len(angles.keys())-1)]
        #and convert back to tuple indices
        indices=(numpy.array([int(i) for i in float_indices]),
                 numpy.array([int(round(math.modf(i)[0]*scalar)) for i in float_indices]))

        if len(indices[0])!=0:
            #here indices are only the ones where this class is smallest
            classification[indices]=classkey

    return classification

def run(output_folder=None,hsi_filename=None,refspectra=None,filetype="ENVI",hsi_wavelengths=None):
    """
    Function to run the spectral angle classifier. This is the one that is called from SCOPS processor.

    inputs:
        output_folder = directory to write to.
        hsi_filename = hyperspectral bil file to classify.
        refspectra = reference spectra ASCII file.
        filetype = file type to write to.
    returns:
        outputfilename - filename of classification written to disk.
    """

    if refspectra is None:
        raise Exception("Need to pass the reference spectra file.")

    outputfilename=os.path.join(output_folder,os.path.basename(os.path.splitext(hsi_filename)[0])+"_spectral_angle_classification.bsq")
    #list to save the spectra to - this will be stacked into a numpy array
    spectralist=[]
    spectra_id=[]

    #get the wavelengths from the HSI file
    if hsi_wavelengths is None:
        hsi_wavelengths=[]
        gdalfile=gdal.Open(hsi_filename)
        number_of_bands=gdalfile.RasterCount
        for b in range(number_of_bands):
            band=gdalfile.GetRasterBand(b+1)
            hsi_wavelengths.append(float(band.GetMetadataItem("Wavelength")))
        gdalfile=None

    #now resample the ref spectra to match the wavelengths of the hsi
    #here we do not do a full resample - only a 1d interpolation. For wide-band data
    #this will be less accurate and a resampling method based on response functions is recommended
    try:
        #loop through and load in each spectra to test against
        spectradata=numpy.genfromtxt(refspectra,names=True)
        names=list(spectradata.dtype.names)
        for name in names:
            new_spectra=[]
            #skip the wavelengths
            if name in ["wavelength"]:
                continue
            #need to interpolate wavelength to HSI wavelengths
            for w in hsi_wavelengths:
                if w > spectradata['wavelength'].max():
                    #The reference spectra data do not go this high - set the resampled ref spectra to 0 here
                    new_spectra.append(0)
                else:
                    new_spectra.append(numpy.interp(w,spectradata['wavelength'],spectradata[name]))
            spectralist.append(numpy.array(new_spectra))
            spectra_id.append(name)

        spectra=numpy.vstack(spectralist)
    except Exception as exc:
        raise Exception("Failed to read in reference spectra beacuse of {}".format(exc))

    #now run the spectral angle calculation on the hyperspectral file
    angles=calculate_spectral_angle(hsi_filename,spectra)
    classification=create_classification_mask(angles)

    #print out pixels that have no classification for debug purposes
    I=numpy.where(classification>10)
    for i in angles.keys():
        print(i,angles[i][I])

    #write out the classification mask
    driver=gdal.GetDriverByName(filetype)
    try:
        outfile=driver.Create(outputfilename, xsize=classification.shape[1], ysize=classification.shape[0],bands=1, eType=gdal.GDT_UInt16)
    except Exception as exc:
        raise Exception("Failed to create output file: {} because {}".format(outputfilename,exc))
    band=outfile.GetRasterBand(1)
    band.WriteArray(classification)
    ##set band name
    band.SetDescription("Spectral angle classification")
    #need to write the classes to meta data for future reference
    band=outfile.GetRasterBand(1)
    for i,key in enumerate(angles.keys()):
        band.SetMetadataItem(str(key),os.path.basename(spectra_id[i]))
    band.SetNoDataValue(nodata)
    outfile=None

    return outputfilename


if __name__=="__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--output_folder',
                        '-o',
                        help='Output folder to write to',
                        default=None,
                        metavar="<output_folder>")
    parser.add_argument('hsifilename',
                        help='Hyperspectral bil file to classify',
                        default=None,
                        metavar="<hsi_filename>")
    parser.add_argument('--refspectra',
                        help='ASCII file containing reference spectra as space separated columns. First column should be "wavelength" all others should be the spectra intensity.',
                        default=os.path.join(os.path.dirname(__file__),"ref_spectra/ref_spectra.txt"),
                        metavar="<filename>")
    args = parser.parse_args()


    run(output_folder=args.output_folder,hsi_filename=args.hsifilename,refspectra=args.refspectra)
