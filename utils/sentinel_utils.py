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
#SBATCH --mem=8192M
#SBATCH -t 01:00:00

# Load modules and specify the work
module load sdc_testing
if [ $? -ne 0 ]; then
    echo "Failed to load sdc_testing module."
    exit 1
fi

# Load necessary modules
module load cloud fractionalcover
if [ $? -ne 0 ]; then
    echo "Failed to load cloud fractionalcover module."
    exit 1
fi

# Specify the work to be done
cd $FILESTORE_PATH/tmp_data/
{script_stage}
"""

    #{self.stage_script}
    print(local_path)
    print(script_name)
    script_path = local_path + script_name + ".slurm"
    with open(script_path, 'w') as file:
        file.write(script_content)

    return script_path


def generate_script_stage(date, stage):

    if stage == "1":
        script_stage = f"""
# Execute cloud fmask processing
qv_sentinel2cloud_fmask.py --toaref10 cemsre_t55hdv_{date}_ab0m5.img
if [ $? -ne 0 ]; then
echo "Failed at stage 1: Cloud fmask processing."
exit 1
fi
"""    
    elif stage == "2":
        script_stage = f"""
qv_sentinel2topomasks.py --toaref10 cemsre_t55hdv_{date}_ab0m5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 2: Topo masks processing."
    exit 1
fi
"""
    elif stage == "3":
        script_stage = f"""
doSfcRefSentinel2.py --toaref cemsre_t55hdv_{date}_ab0m5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 3: Surface reflectance processing."
    exit 1
fi    
"""
    elif stage == "4":
        script_stage = f"""
qv_water_index2015.py cemsre_t55hdv_{date}_abam5.img cemsre_t55hdv_{date}_abbm5.img --omitothermasks
if [ $? -ne 0 ]; then
    echo "Failed at stage 4: Water index processing."
    exit 1
fi    
"""        
    elif stage == "5":
        script_stage =f"""
qv_fractionalcover_sentinel2.py cemsre_t55hdv_{date}_abam5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 5: Fractional cover processing."
    exit 1
fi    
"""

    return script_stage

