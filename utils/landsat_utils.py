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
module load brdf drr irs-atmos groundcover cloud fractionalcover
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

    ## STAGE 1
    if stage == "1":
        script_stage = f"""
# Execute incidence processing
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)

# Check if file exist
if [ -z "$fileda2" ]; then
    echo "Failed at stage 1: Required input files not found."
    exit 1
fi

makeincidenceangles.py --anglesfile $fileda2
if [ $? -ne 0 ]; then
    echo "Failed at stage 1: Incidence."
    exit 1
fi
"""    
# {date}_ab0m5.img
    ## STAGE 2
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
    ## STAGE 3
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
    echo "Failed at stage 3: topocorrectref."
    exit 1
fi

"""
    ## STAGE 4
    elif stage == "4":
        script_stage = f"""
# incidencemask
fileda3=$(ls {date}_da3*.img 2>/dev/null | head -n 1)

# Check if both files exist
if [ -z "$fileda3" ]; then
    echo "Failed at stage 4: Required input files not found."
    exit 1
fi

fileddc=${{fileda3/da3/ddc}}

incidencemask.py --incidfile $fileda3 --outfile $fileddc
if [ $? -ne 0 ]; then
    echo "Failed at stage 4: incidencemask."
    exit 1
fi    
"""  
    ## STAGE 5
    elif stage == "5":
        script_stage =f"""
# castshadowmask
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$fileda2" ]; then
    echo "Failed at stage 5: Required input files not found."
    exit 1
fi

fileddb=${{fileda2/da2/ddb}}

toposhadowmask.py --anglesImage $fileda2 --outImage $fileddb
if [ $? -ne 0 ]; then
    echo "Failed at stage 5: castshadowmask."
fi    
"""   
    ## STAGE 6            
    elif stage == "6":
        script_stage =f"""
# fpc_topocorrected
filedb8=$(ls {date}_db8*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedb8" ]; then
    echo "Failed at stage 6: Required input files not found."
    exit 1
fi

qv_fpc_index.py --in $filedb8
if [ $? -ne 0 ]; then
    echo "Failed at stage 6: fpc_topocorrected."
fi    
"""
    # STAGE 7
    elif stage == "7":
        script_stage =f"""
# temperature
converted_product="${{date/re/th}}"  # Replace 'l9olre' with 'l9olth'
fileda1=$(ls ${{converted_product}}_da1*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$fileda1" ]; then
    echo "Failed at stage 7: Required input files not found."
    exit 1
fi

filedbh_re=${{fileda1/da1/ddh}}
filedbh=${{filedbh_re/th/re}}

landsattemperature.py -r $fileda1 -t $filedbh
if [ $? -ne 0 ]; then
    echo "Failed at stage 7: temperature."
fi    
"""
    # STAGE 8
    elif stage == "8":
        script_stage =f"""
# watermask_topocorrected
filedb8=$(ls {date}_db8*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedb8" ]; then
    echo "Failed at stage 8: Required input files not found."
    exit 1
fi

qv_water_index.py --toaref $filedb8
if [ $? -ne 0 ]; then
    echo "Failed at stage 8: watermask_topocorrected."
fi    
"""        
    # STAGE 9
    elif stage == "9":
        script_stage =f"""
# surfacereflectance
fileda1=$(ls {date}_da1*.img 2>/dev/null | head -n 1)
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)
fileda3=$(ls {date}_da3*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$fileda1" ] || [ -z "$fileda2" ] || [ -z "$fileda3" ]; then
    echo "Failed at stage 9: Required input files not found."
    exit 1
fi

filedbg=${{fileda1/da1/dbg}}

doRadiomCorrection.py --radiance $fileda1 --angles $fileda2 --incid $fileda3 --flattenedref $filedbg

if [ $? -ne 0 ]; then
    echo "Failed at stage 9: surfacereflectance."
fi    
""" 
    # STAGE 10
    elif stage == "10":
        script_stage =f"""
# ndvi
filedbg=$(ls {date}_dbg*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedbg" ]; then
    echo "Failed at stage 10: Required input files not found."
    exit 1
fi

qv_landsatndvi.py -i $filedbg

if [ $? -ne 0 ]; then
    echo "Failed at stage 10: ndvi."
fi    
"""
    # STAGE 11
    elif stage == "11":
        script_stage =f"""
# fmaskcloud
fileda1=$(ls {date}_da1*.img 2>/dev/null | head -n 1)
fileda2=$(ls {date}_da2*.img 2>/dev/null | head -n 1)
converted_product="${{date/re/th}}"  # Replace 'l9olre' with 'l9olth'
filedbh=$(ls ${{converted_product}}_dbh*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$fileda1" ] || [ -z "$fileda2" ] || [ -z "$filedbh" ]; then
    echo "Failed at stage 11: Required input files not found."
    exit 1
fi

qv_landsatcloud_fmask.py --radiancefile $fileda1 --updatedatabase

if [ $? -ne 0 ]; then
    echo "Failed at stage 11: fmaskcloud."
fi    
"""              
    # STAGE 12
    elif stage == "12":
        script_stage =f"""
# fractionalcover_sfcref
filedbg=$(ls {date}_dbg*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedbg" ]; then
    echo "Failed at stage 12: Required input files not found."
    exit 1
fi

filedil=${{filedbg/dbg/dil}}

compute_fractionalcover.py -i $filedbg -o $filedil

if [ $? -ne 0 ]; then
    echo "Failed at stage 12: fractionalcover_sfcref."
fi    
"""
    # STAGE 13
    elif stage == "13":
        script_stage =f"""
# binarywatermask
filedd6=$(ls {date}_dd6*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedd6" ]; then
    echo "Failed at stage 13: Required input files not found."
    exit 1
fi

qv_binarywatermask.py --waterindex $filedd6 --omitothermasks

if [ $? -ne 0 ]; then
    echo "Failed at stage 13: binarywatermask."
fi    
"""
    # STAGE 14
    elif stage == "14":
        script_stage =f"""
# waterindex_2015
filedbg=$(ls {date}_dbg*.img 2>/dev/null | head -n 1)

# Check if files exist
if [ -z "$filedbg" ]; then
    echo "Failed at stage 14: Required input files not found."
    exit 1
fi

fileddi=${{filedbg/dbg/ddi}}
fileddj=${{filedbg/dbg/ddj}}

qv_water_index2015.py --outindex $fileddi --outmask $fileddj $filedbg --omitothermasks

if [ $? -ne 0 ]; then
    echo "Failed at stage 14: waterindex_2015."
fi    
"""           
    return script_stage

