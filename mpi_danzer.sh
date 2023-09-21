#!/bin/sh
#PBS -V
#PBS -N prism
#PBS -q exclusive
#PBS -A etc
#PBS -l select=5:ncpus=1:mpiprocs=5:ompthreads=1
#PBS -l walltime=04:00:00
#PBS -m abe
#PBS -M sylee2519@naver.com
#PBS -W sandbox=PRIVATE

cd $PBS_O_WORKDIR
module purge
module load craype-x86-skylake gcc/8.3.0 openmpi/3.1.0
module load forge/18.1.2

mpirun -np 25 ./danzer_obj f 4 -s 4096 -m 1 -i /home01/sample_data/nurion_stripe/abaqus/ -o ./ghi -t 2 1>stdout 2>stderr
#mpirun -np 25 ./danzer_obj f 4 -s 4096 -m 1 -i /home01/sample_data/nurion/dup_sample/abaqus/ -o ./my 1>stdout_my 2>stderr
