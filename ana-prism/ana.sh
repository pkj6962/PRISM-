#!/bin/sh
#PBS -V
#PBS -N ana-abaqus
#PBS -q exclusive
#PBS -A etc
#PBS -l select=1:ncpus=68
#PBS -l walltime=04:00:00
#PBS -m abe
#PBS -M pjh6962@naver.com

cd $PBS_O_WORKDIR

module purge
module load gcc/8.3.0


./ana_prism -i /scratch/s5104a20/mpi_danzer/danzer/abc.txt -o abaqus_res.txt -n 24
