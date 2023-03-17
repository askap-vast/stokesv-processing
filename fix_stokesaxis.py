import sys
import glob
from astropy.io import fits

imagedir = sys.argv[1]
images = glob.glob(f'{imagedir}/image*.fits')

for image in images:
   with fits.open(image, mode='update') as hdul:
       header = hdul[0].header
       print(header['CRVAL4'])
       header['CRVAL4'] = 1.0
