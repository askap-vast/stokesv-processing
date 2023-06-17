indir=$1
outdir=$2

mkdir $outdir

mv $indir/selavy-*.xml $outdir/
mv $indir/noiseMap.image.*.fits $outdir/
mv $indir/meanMap.image*.fits $outdir/
