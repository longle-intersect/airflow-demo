import sys
import os
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def create_slurm_script(script_name, script_stage, local_path):

    script_content = f"""#!/bin/bash
#SBATCH --job-name={script_name}
#SBATCH --output=/home/lelong/log_airflow_slurm/stdout/{script_name}.log
#SBATCH --error=/home/lelong/log_airflow_slurm/stderr/{script_name}.error
#SBATCH -n 1
#SBATCH --mem=5120M
#SBATCH -t 01:00:00

# Load modules and specify the work
module load sdc_testing
if [ $? -ne 0 ]; then
    echo "Failed to load sdc_testing module."
    exit 1
fi

# Load necessary modules
module load satimport usgslandsat autoslats overnightbatch aarnet maint_tools jrsrp-landsat
if [ $? -ne 0 ]; then
    echo "Failed to load modules."
    exit 1
fi

# Specify the work to be done
cd $FILESTORE_PATH/tmp_data/landsat
{script_stage}
"""

    #{self.stage_script}
    #print(local_path)
    #print(script_name)
    script_path = local_path + script_name
    with open(script_path, 'w') as file:
        file.write(script_content)

    return script_path


def generate_script_stage(date, stage):

    if stage == "1":
        script_stage = f"""
# Execute incidence processing
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)

# Check if file exist
if [ -z "$fileda2" ]; then
    echo "Failed at stage 4: Required input files not found."
    exit 1
fi

makeincidenceangles.py --anglesfile $fileda2
if [ $? -ne 0 ]; then
    echo "Failed at stage 1: Incidence."
    exit 1
fi
"""    
# {date}_ab0m5.img
    elif stage == "2":
        script_stage = f"""
# toa_brdf_reflectance
fileda1=$(ls {date}_da1*.img 2>/dev/null | head -n 1)
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)

# Check if file exist
if [ -z "$fileda1" ] || [ -z "$fileda2" ]; then
    echo "Failed at stage 2: Required input files not found."
    exit 1
fi

usgs2stdtoaref.py --infile $fileda1
if [ $? -ne 0 ]; then
    echo "Failed at stage 2: toa_brdf_reflectance."
    exit 1
fi
"""
# {date}_ab0m5.img
    elif stage == "3":
        script_stage = f"""
# topocorrectref
filedb3=$(ls {date}_db3*.img 2>/dev/null | head -n 1)
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)
fileda3=$(ls {date}_da3*.img 2>/dev/null | head -n 1)

# Check if file exist
if [ -z "$filedb3" ] || [ -z "$fileda2" ] || [ -z "$fileda3" ]; then
    echo "Failed at stage 3: Required input files not found."
    exit 1
fi

topocorrect.py --reffile $filedb3 --limitovercorrection
if [ $? -ne 0 ]; then
    echo "Failed at stage 2: topocorrectref."
    exit 1
fi

"""
# 
    elif stage == "4":
        script_stage = f"""
# incidencemask
fileda3=$(ls {date}_da3*.img 2>/dev/null | head -n 1)

# Check if both files exist
if [ -z "$fileda3" ]; then
    echo "Failed at stage 4: Required input files not found."
    exit 1
fi

fileddc=${fileda3/da3/ddc}

incidencemask.py --incidfile $fileda3 --outfile $fileddc
if [ $? -ne 0 ]; then
    echo "Failed at stage 4: incidencemask."
    exit 1
fi    
"""        
    elif stage == "5":
        script_stage =f"""
# fpc_topocorrected
filedb8=$(ls {date}_db8*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedb8" ]; then
    echo "Failed at stage 5: Required input files not found."
    exit 1
fi

qv_fpc_index.py --in $filedb8
if [ $? -ne 0 ]; then
    echo "Failed at stage 5: fpc_topocorrected.
fi    
"""
,
    return script_stage

