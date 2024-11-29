#!/Applications/CASA.app/Contents/MacOS/python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os
import pickle
import sys
import time as systime

import numpy as np

from . import qa

#def_input_path = "/mnt/jaosco/data/hfrancke/QAscores"
#def_input_path = "/jaopost_spool/QAscoresMOUSs"
def_input_path = "/Users/haroldfrancke/QAscores_tests"
#def_output_path = "/mnt/jaosco/data/hfrancke/QAscores/test"
#def_output_path = "/jaopost_spool/QAscoresResults_v31"
def_output_path = "/Users/haroldfrancke/QAscores_test_results"

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
    parser = argparse.ArgumentParser(description='Calculate QA2 scores for ALMA MOUSS files')
    parser.add_argument('input', help='Directory to get .m files from',type=str)
    parser.add_argument('-o','--output', metavar="output",help='Directory to store QA2 scores', required=False,
                        default=def_output_path)
    parser.add_argument('-p','--params', metavar="parameters",help='Filename with optional parameters to calculate scores', 
                        default=def_param, required=False)  
    parser.add_argument('-m','--memlim', metavar="memory_limit",help='memory limit for calculations', 
                        default=memlim, required=False)
    
    args = parser.parse_args()

    input_folder_name=args.input

    if os.path.exists(args.input):
        print("Input directory: "+args.input)
        input_folder_name=args.input.split("/")[-1]
    elif os.path.exists(def_input_path+"/"+args.input):
        args.input = def_input_path+"/"+args.input
        print("Input directory: "+args.input)
    else:
        print('Could not find input folder '+def_input_path+"/"+args.input+". See -h for help")
        sys.exit()

    if not os.path.exists(args.output):
        os.system('mkdir '+args.output)

    if not os.path.exists(args.output+'/'+input_folder_name):
        os.system('mkdir '+args.output+'/'+input_folder_name)
    
    if not os.path.exists(args.output+'/'+input_folder_name+'/databuffer'):
        os.system('mkdir '+args.output+'/'+input_folder_name+'/databuffer')
        
    args.output = args.output+'/'+input_folder_name
    print("Output directory: "+args.output)

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

def save_txt_pkl(filename, scores):
    #pickle file
    with open(filename+'.pickle','wb') as f:
        pickle.dump(scores,f)
        f.close()
    #plain txt
    f = open(filename+'.txt', "a")
    f.write(str(scores))
    f.close()

def main():
    args = parse_arguments()
    # #Get timestamp for output folders and files
    aux = systime.asctime().split()
    timestamp = (aux[4]+aux[1]+aux[2]+'T'+aux[3]).replace(':','_')

    input_path = args.input
    output_path = args.output
    memlim = float(args.memlim)

    #get list of ms
    mslist = glob.glob(input_path+'/S*/G*/M*/working/*.ms')
    excludedstr = ['_targets.ms','_target.ms','_line.ms','_cont.ms']
    mslist = [ms for ms in mslist if all([not item in ms for item in excludedstr])]
    justmslist = [ms.split('/')[-1] for ms in mslist]

    #Get an idetifier for the containing folder of the dataset
    if '/' in input_path:
        plfolder = [item for item in input_path.split('/') if not item == ''][-1]
    else:
        plfolder = input_path

    for idx, ms in enumerate(mslist):
        (all_scores, final_scores, \
         qaevalf) = qa.get_qa_scores(ms, output_path=output_path,
                                     memory_gb=memlim,
                                     applycalQAversion=applycalQAversion,
                                     timestamp=timestamp)

        #Write QA score Eval Function For testing
        fname_qascoref = output_path+'/'+justmslist[idx]+'.'+timestamp+'.qascoref.pipe'
        with open(fname_qascoref+'.pickle','wb') as f:
            pickle.dump(qaevalf, f)
            f.close()

        fname_final_scores = output_path+'/'+justmslist[idx]+'.'+timestamp+'.final_scores.pipe'
        save_txt_pkl(fname_final_scores, final_scores)

        fname_all_scores = output_path+'/'+justmslist[idx]+'.'+timestamp+'.all_scores.pipe'
        save_txt_pkl(fname_all_scores, all_scores)


if __name__ == '__main__':
    main()
