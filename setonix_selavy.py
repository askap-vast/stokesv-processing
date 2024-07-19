### write selavy batch files for running ###
### the data is downloaded from casda ###

import argparse
from astropy.io.fits.hdu import image
import pandas as pd
import glob
import logging
from pathlib import Path

### logger
def _setlogger_(level=logging.INFO):
    logger = logging.getLogger('makeparset')
    logger.setLevel(level)
    ### set formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ### stream handler
    sh = logging.StreamHandler()
    # sh.setLevel(level)
    sh.setFormatter(formatter)
    ### add handler
    if not logger.handlers:
        logger.addHandler(sh)

### function for writing selavy parset
def write_selavy_parset(image, weight_image, taylor1_image, sbid, invert, outdir="."):
    outdir = Path(outdir)
    no_fits_image = image.split("/")[-1].replace(".fits", "")
    findspectral = 'false' if taylor1_image == '' else 'true'
    if invert:
        invertflag = 'n'
    else:
        invertflag = ''
    
    resultsfile = str(outdir / f"selavy-{invertflag}{no_fits_image}.txt")
    thresh_img_path = str(outdir / f"detThresh.{invertflag}{no_fits_image}")
    noise_img_path = str(outdir / f"noiseMap.{invertflag}{no_fits_image}")
    mean_img_path = str(outdir / f"meanMap.{invertflag}{no_fits_image}") 
    snr_img_path = str(outdir / f"snrMap.{invertflag}{no_fits_image}")
    ann_path = str(outdir / "selavy-SubimageLocations.{invertflag}{no_fits_image}.ann")
    
    selavy_template = f"""
Selavy.image                                    = {image}
Selavy.sbid                                     = {sbid}
Selavy.sourceIdBase                             = SB{sbid}
Selavy.imageHistory                             = ["Produced with ASKAPsoft 1.10.0.a on Setonix"]
Selavy.imagetype                                = fits
#
Selavy.spectralTerms.thresholdSNR               = 50.
Selavy.spectralTermsFromTaylor                  = true
Selavy.findSpectralTerms                        = [{findspectral}, false]
Selavy.spectralTermImages                       = [{taylor1_image},]
Selavy.nsubx                                    = 5
Selavy.nsuby                                    = 4
Selavy.overlapx                                 = 0
Selavy.overlapy                                 = 0
Selavy.subimageAnnotationFile                   = ""
#
Selavy.resultsFile                              = {resultsfile}
#
# Detection threshold
Selavy.snrCut                                   = 5
Selavy.flagGrowth                               = true
Selavy.growthThreshold                          = 3
#
Selavy.VariableThreshold                        = true
Selavy.VariableThreshold.reuse                  = false
Selavy.VariableThreshold.boxSize                = 50
Selavy.VariableThreshold.ThresholdImageName     = {thresh_img_path}
Selavy.VariableThreshold.NoiseImageName         = {noise_img_path}
Selavy.VariableThreshold.AverageImageName       = {mean_img_path}
Selavy.VariableThreshold.SNRimageName           = {snr_img_path}
Selavy.Weights.weightsImage                     = {weight_image}
Selavy.Weights.weightsCutoff                    = 0.04
#
Selavy.Fitter.doFit                             = true
Selavy.Fitter.fitTypes                          = [full]
Selavy.Fitter.numGaussFromGuess                 = true
Selavy.Fitter.maxReducedChisq                   = 10.
Selavy.Fitter.imagetype                         = fits
Selavy.Fitter.writeComponentMap                 = false
#
Selavy.threshSpatial                            = 5
Selavy.flagAdjacent                             = true
Selavy.flagNegative                             = {invert}
#
Selavy.minPix                                   = 3
Selavy.minVoxels                                = 3
Selavy.minChannels                              = 1
Selavy.sortingParam                             = -pflux
Selavy.precFlux                                 = 6
Selavy.precSNR                                  = 3
#
# Not performing RM Synthesis for this case
Selavy.RMSynthesis                              = false

# No spectral extraction being performed
Selavy.Components.extractSpectra                = false
Selavy.Components.extractNoiseSpectra           = false
"""

    parset_name="selavy.{}{}.in".format(invertflag, no_fits_image)
    with open(parset_name, "w") as f:
        f.write(selavy_template)

### for files
def _makeparset(imagepath, invert, outdir="."):
    '''
    make selavy parset for one image

    we will read message from image name directly
    '''

    logger = logging.getLogger('makeparset.run')
    ### extract file name
    imagefname = imagepath.split('/')[-1]
    imagedir = '/'.join(imagepath.split('/')[:-1])
    fnamesplit = imagefname.split('.')
    # extracting messages
    # example: image.v.FRB190711_beam15.SB31377.cont.taylor.0.restored.conv.fits
    pol = fnamesplit[1]
    field = fnamesplit[2] if any([s in fnamesplit[2] for s in ['-', '+']]) else ''
    sbid = fnamesplit[3][2:]

    logger.info(f'writing selavy parset for SBID - {sbid}')

    ### create weight image name
    # example: weights.v.NGC6744.SB31349.cont.taylor.0.fits
    weightpattern = f'weights.{pol}.*{field}*.SB{sbid}.*.taylor.0.fits'
    weightfiles = glob.glob(f'{imagedir}/{weightpattern}')
    if len(weightfiles) == 0: raise ValueError(f'No weights image found with pattern {weightpattern}!')
    if len(weightfiles) != 1: logger.warning('{} weights files found!'.format(len(weightfiles)))
    weight_image = weightfiles[0]

    ### create taylor1 image name
    if pol == 'i': raise NotImplemented
    else: taylor1_image = ''

    write_selavy_parset(imagepath, weight_image, taylor1_image, sbid, invert, outdir=outdir)

def makeparsets(pathpattern, invert, outdir="."):
    '''write parsets for a list of images'''

    logger = logging.getLogger('makeparset.run')

    ### get data
    images = glob.glob(pathpattern)
    logger.info('{} images found...'.format(len(images)))

    for imagepath in images:
        _makeparset(imagepath, invert, outdir=outdir)

    logger.info('done!')

def _write_sbatch(job_name,
                  imagepath,
                  invert,
                  walltime,
                  ntasks,
                  ntasks_per_node,
                  memory,
                  project_code
                  ):
    '''write sbatch files'''
    logger = logging.getLogger('makeparset.sbatch')

    if invert:
        invertflag = 'n'
    else:
        invertflag = ''
    
    no_fits_image = imagepath.split("/")[-1].replace(".fits", "")
    parset_name="selavy.{}{}.in".format(invertflag, no_fits_image)
    sbatch_name = 'selavy.{}{}.sbatch'.format(invertflag, no_fits_image)

    logger.debug(f'writing sbatch file to {sbatch_name}')

    with open(sbatch_name, 'w') as fp:
        fp.write(f'''#!/bin/bash
#SBATCH --job-name {job_name}
#SBATCH --time={walltime}
#SBATCH --ntasks={ntasks}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --mem={memory}
#SBATCH --account={project_code}

module use /software/projects/ja3/modulefiles
module load singularity/3.11.4-slurm
module load askapsoft/1.10.0.a

srun selavy -c {parset_name}        
''')

    return sbatch_name

def writebatch(job_name,
               pathpattern,
               invert,
               walltime = '01:30:00',
               ntasks = '21',
               ntasks_per_node = '21',
               memory = '110G',
               project_code='ja3'
               ):
    '''write the final .sh file for submission'''
    logger = logging.getLogger('writebatch')

    ### get data
    images = glob.glob(pathpattern)
    logger.info('{} images found...'.format(len(images)))

    sbatch_names = []
    for imagepath in images:
        sbatch_name = _write_sbatch(job_name,
                                    imagepath,
                                    invert,
                                    walltime,
                                    ntasks,
                                    ntasks_per_node,
                                    memory,
                                    project_code
                                    )
        sbatch_names.append(sbatch_name)
        logger.info(f"Written {sbatch_name}")

    shfile = ''
    for sbatch_name in sbatch_names:
        shfile += 'sbatch {}\n'.format(sbatch_name)
    
    with open('selavybatch.sh', 'w') as fp:
        fp.write(shfile)

    logger.info('please run `sh selavybatch.sh` to submit jobs')

if __name__ == "__main__":
    _setlogger_()

    parser = argparse.ArgumentParser(description='writing selavy configuration files')
    parser.add_argument('-f', '--file', type=str, required=True, help='Pattern of your images')
    parser.add_argument('-j', '--jobname', type=str, required=True, help='Name of job')
    parser.add_argument('-n', '--invert', dest='invert', required=False, action='store_true', help='Flag to invert image')
    parser.add_argument('--out-dir', required=False, default=".", help='Selavy output directory')
    
    args = parser.parse_args()

    invert = args.invert
    
    makeparsets(pathpattern=args.file, invert=invert, outdir=args.out_dir)
    writebatch(job_name=args.jobname, pathpattern=args.file, invert=invert)
