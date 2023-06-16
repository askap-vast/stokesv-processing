import argparse
import glob
import logging
import sys
from astropy.io import fits
from pathlib import Path
import setonix_selavy
import subprocess

def _setlogger_(level=logging.INFO):
    logger = logging.getLogger()
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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('imagedir',
                        type=str,
                        help='Path to directory containing Stokes V images'
                        )
                        
    parser.add_argument('--out-dir',
                        type=str,
                        help='Path to directory containing the finished data products'
                        )
    parser.add_argument('--wall-time',
                        type=str,
                        default='01:30:00',
                        help='Wall time for each slurm job'
                        )
    parser.add_argument('--mem-request',
                        type=str,
                        default='110G',
                        help='Memory request for each slurm job'
                        )
    parser.add_argument('--ntasks',
                        type=str,
                        default='21',
                        help='Number of tasks for each slurm job'
                        )
    parser.add_argument('--ntasks-per-node',
                        type=str,
                        default='21',
                        help='Number of tasks per node for each slurm job'
                        )
    parser.add_argument('--submit-jobs',
                        action='store_true',
                        help='Submit the jobs'
                        )
    
    return parser.parse_args()

def fix_stokesaxis(imagedir):
    imagedir = Path(imagedir)
    images = imagedir.glob('image*.fits')

    for image in images:
       with fits.open(image, mode='update') as hdul:
           header = hdul[0].header
           header['CRVAL4'] = 1.0
    return

def generate_files(filename, jobname, args, invert=False, outdir="."):
    logger = logging.getLogger('run_selavy.generate_files')
    logger.info("Generating sbatch with: ")
    logger.info(f"filename: {filename}")
    logger.info(f"jobname: {jobname}")
    logger.info(f"invert: {invert}")
    setonix_selavy.makeparsets(pathpattern=filename,
                               invert=invert,
                               outdir=outdir
                               )
    setonix_selavy.writebatch(job_name=jobname,
                              pathpattern=filename,
                              invert=invert,
                              walltime=args.wall_time,
                              ntasks=args.ntasks,
                              ntasks_per_node=args.ntasks_per_node,
                              memory=args.mem_request,
                              )
    return


def _remove_regex(dir, glob_str, dry_run=True):
    logger = logging.getLogger('run_selavy._remove_regex')
    for p in dir.glob(glob_str):
        if dry_run:
            logger.info(f"Will remove {p}")
        else:
            logger.info(f"Removing {p}")
            p.unlink()
    return

def remove_products(dir, sbid, invert, dry_run=True):
    dir = Path(dir)
    pol = 'image'
    if invert:
        pol='nimage'
    
    _remove_regex(dir, f"selavy-{pol}*{sbid}*", dry_run=dry_run)
    _remove_regex(dir, f"*.{pol}*{sbid}*.fits", dry_run=dry_run)
    return

def main(args):
    logger = logging.getLogger('run_selavy.main')
    image_dir = Path(args.imagedir).resolve()
    out_dir = Path(args.out_dir).resolve()
    
    if not out_dir.is_dir():
        out_dir.mkdir(parents=True)
    if not image_dir.is_dir():
        print("{image_dir} does not exist!")
        exit()
    
    fix_stokesaxis(image_dir)
    
    num_jobs = 0
    num_images = len(list(image_dir.glob("image.*")))
    
    for image_path in image_dir.glob("image.*"):
        sbid = image_path.name.split(".")[3]
        selavy_search_str = f"selavy-image.*{sbid}*.components.xml"
        nselavy_search_str = f"selavy-nimage.*{sbid}*.components.xml"
        
        # check if the positive sourcefinding has already been run
        invert=False
        if len(list(out_dir.glob(selavy_search_str))) == 0:
            # if not, run it
            logger.info(f"{image_path.name} (pos) has not been run.")
            generate_files(str(image_path),
                           f"selavy-{sbid}",
                           args,
                           invert=invert,
                           outdir=str(out_dir)
                           )
            
            if args.submit_jobs:
                logger.info("Removing old products")
                remove_products(out_dir, sbid, invert=invert, dry_run=False)
                logger.info(f"Submitting sbatch")
                subprocess.run("sh selavybatch.sh", shell=True)
            else:
                remove_products(out_dir, sbid, invert=invert, dry_run=True)
            
            num_jobs += 1
        else:
            logger.info(f"{image_path.name} (pos) has already been run - skipping.")
           
        # check if the negative sourcefinding has already been run
        invert=True
        if len(list(out_dir.glob(nselavy_search_str))) == 0:
            # if not, run it
            logger.info(f"{image_path.name} (neg) has not been run.")
            generate_files(str(image_path),
                           f"nselavy-{sbid}",
                           args,
                           invert=invert,
                           outdir=str(out_dir)
                           )
            
            if args.submit_jobs:
                logger.info("Removing old products")
                remove_products(out_dir, sbid, invert=invert, dry_run=False)
                logger.info(f"Submitting sbatch")
                subprocess.run("sh selavybatch.sh", shell=True)
            else:
                remove_products(out_dir, sbid, invert=invert, dry_run=True)
            
            num_jobs += 1
        else:
            logger.info(f"{image_path.name} (neg) has already been run - skipping.")
           
    if args.submit_jobs:
        logger.info(f"Submitted {num_jobs} jobs for {num_images} images")
    else:
        logger.info(f"Will submit {num_jobs} jobs for {num_images} images")

if __name__ == '__main__':
    args = parse_args()
    _setlogger_()
    main(args)
