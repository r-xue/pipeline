#!/Applications/CASA.app/Contents/MacOS/python3
# -*- coding: utf-8 -*-

import argparse
import pickle
import time as systime
from pathlib import Path

import numpy as np

import pipeline
from pipeline import Context
from pipeline.domain import DataType
from . import qa

def_output_path = "./pipe-1770"

memlim = 8.0
applycalQAversion ='32p'

##Default Parameter dictionary##
def_param = { \
    'ampslopesiglimit': 25.0,
    'ampscalesiglimit': 53.0,
    'amptol': 0.0,
    'phaseslopesiglimit': 40.0,
    'phaseoffsetsiglimit': 60.5,
    'phasetol': 0.0*np.pi/180.0,
    'chi2limit': 100000.0,
    'psdplimit': 12.5,
    'thresh_mad': 5.0,
    'thresh_frac': 0.1,
    'thresh_off': 0.84,
    'thresh_var': 1.6,
    'verbose': False}

outlier_score = 0.5


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Calculate QA2 scores for ALMA MOUSS files. '
                    'MUST be run from a directory containing a Pipeline context.'
    )
    parser.add_argument('-o','--output', metavar="output",help='Directory to store QA2 scores', required=False,
                        default=def_output_path)
    parser.add_argument('-p','--params', metavar="parameters",help='Filename with optional parameters to calculate scores', 
                        default=def_param, required=False)  
    parser.add_argument('-m','--memlim', metavar="memory_limit",help='memory limit for calculations', 
                        default=memlim, required=False)
    
    args = parser.parse_args()

    output_dir = Path(args.output)
    print(f"Output directory: {args.output}")

    # create output and databuffer directories if they don't already exist
    Path(args.output).mkdir(parents=True, exist_ok=True)
    Path(output_dir / 'databuffer').mkdir(parents=True, exist_ok=True)

    if args.params != def_param:
        try:
            paramfile = open(args.params, 'r')
            print('Reading in parameters from '+args.params)
            params = def_param
            for line in paramfile:
                splitline = line.split()
                if (len(splitline) > 0) and (not line[0] == '#') and (len(splitline) == 2) and (splitline[0] in def_param.keys()):
                    print('Using custom parameter: '+line)
                    params[splitline[0]] = eval(splitline[1])
            paramfile.close()
            args.params=params
        except:
            print("Could not find parameters file: "+args.params+", See -h for help. Using default parameters instead.")

    return args

def save_txt_pkl(filename: Path, scores):
    #pickle file
    with open(f'{filename}.pickle','wb') as f:
        pickle.dump(scores,f)
        f.close()
    #plain txt
    f = open(f'{filename}.txt', "a")
    f.write(str(scores))
    f.close()

def main():
    args = parse_arguments()
    # #Get timestamp for output folders and files
    aux = systime.asctime().split()
    timestamp = (aux[4]+aux[1]+aux[2]+'T'+aux[3]).replace(':','_')

    output_path = Path(args.output)
    memlim = float(args.memlim)

    # The Pipeline context stores MS paths relative to the pipeline working
    # directory, hence we need to always run from that directory. A benefit
    # of running from the pipeline working directory is that we can simply
    # call context='last' to get the most recent pipeline context.
    ctx: Context = pipeline.Pipeline(context='last').context
    raw_mses = ctx.observing_run.get_measurement_sets_of_type([DataType.RAW])

    for ms in raw_mses:
        (all_scores, final_scores, qaevalf) = qa.get_qa_scores(
            ms.basename,
            output_path=output_path,
            memory_gb=memlim,
            applycalQAversion=applycalQAversion,
            timestamp=timestamp
        )

        #Write QA score Eval Function For testing
        fname_qascoref = output_path / f'{ms.basename}.{timestamp}.qascoref.pipe'
        with open(f'{fname_qascoref}.pickle','wb') as f:
            pickle.dump(qaevalf, f)

        fname_final_scores = output_path / f'{ms.basename}.{timestamp}.final_scores.pipe'
        save_txt_pkl(fname_final_scores, final_scores)

        fname_all_scores = output_path / f'{ms.basename}.{timestamp}.all_scores.pipe'
        save_txt_pkl(fname_all_scores, all_scores)


if __name__ == '__main__':
    main()
