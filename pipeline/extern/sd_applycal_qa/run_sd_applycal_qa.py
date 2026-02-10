#!/Applications/CASA.app/Contents/MacOS/python3
# -*- coding: utf-8 -*-

#shebang cluster
#!/opt/pipelinedriver/2024OCT/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/python3

import os, sys
import argparse
import glob
import time as systime
import numpy as np

from . import sd_applycal_qa
from . import sd_qa_reports


#def_input_path = "/Users/haroldfrancke/QAscoresMOUSs_SD"
#def_input_path = "/jaopost_spool/"
#def_output_path = ""
#def_output_path = ""
def_input_path = "/jaopost_spool/sd-qascores/PIPEREQ-176"
def_output_path = ""

def parse_arguments():
    parser = argparse.ArgumentParser(description='Calculate QA scores for ALMA SD PL applycal stage')
    parser.add_argument('input', help='Directory to get .ms files from',type=str)
    parser.add_argument('-o','--output', metavar="output",help='Directory to store copy of QA2 score results',
                        required=False, default=def_output_path)
    parser.add_argument('-e','--pereb', help='Do QA evaluation per-EB? T/F', required=False, action='store_true')
    parser.add_argument('-b','--bufferdata', help='Buffer read MS data T/F', required=False, action='store_true')

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

    if len(args.output) > 0:
        if not os.path.exists(args.output):
            os.system('mkdir '+args.output)

        if not os.path.exists(args.output+'/'+input_folder_name):
            os.system('mkdir '+args.output+'/'+input_folder_name)
                
        args.output = args.output+'/'+input_folder_name
        print("Output copy directory: "+args.output)
    else:
        args.output = ""
        print("No Output copy directory, leaving output in the same input directory...")

    return args

def main():

    #Start time
    tstart = systime.time()

    #Get command line arguments
    args = parse_arguments()
    input_path = args.input
    output_path = args.output

    #Do QA assessment per EB?
    pereb = args.pereb

    #Buffer MS data read to disk?
    buffer_data = args.bufferdata

    #get list of ms, create relevant lists filtering out non-relevant MSs
    mslist = glob.glob(input_path+'/S*/G*/M*/working/*.ms')
    excludedstr = ['atmcor']
    mslist = [ms for ms in mslist if all([not item in ms for item in excludedstr])]
    justmslist = [ms.split('/')[-1] for ms in mslist]
    working_folder = mslist[0].replace(justmslist[0], '')

    #Get an identifier for the containing folder of the dataset
    if '/' in input_path:
        plfolder = [item for item in input_path.split('/') if not item == ''][-1]
    else:
        plfolder = input_path

    #Set output path
    if output_path == '':
        output_path = working_folder
    else:
        output_path = plfolder

    #Create output paths, if they don't exist yet
    if (len(output_path) > 0) and (not os.path.exists(output_path)):
        os.system('mkdir '+output_path)

    print('Working PL folder:'+working_folder)
    print('Requesting plot output to be in '+output_path+'sd_applycal_output')
    #Run sd_applycal main method to get QA scores
    if pereb:
        #Option to run the QA per EB
        qascore_list = []
        plots_fnames = []
        qascore_per_scan_list = []
        for ms in mslist:
            (qascore_list_pereb, plots_fnames_pereb, qascore_per_scan_list_pereb) = \
                sd_applycal_qa.get_ms_applycal_qascores([ms], plot_output_path = output_path+'sd_applycal_output',
                                                        weblog_output_path = output_path+'sd_applycal_output',
                                                        sciline_det = True, buffer_data = False)
            qascore_list.extend(qascore_list_pereb)
            plots_fnames.extend(plots_fnames_pereb)
            qascore_per_scan_list.extend(qascore_per_scan_list_pereb)
    else:
        #Option to run the QA with all EBs combined
        (qascore_list, plots_fnames, qascore_per_scan_list) = \
            sd_applycal_qa.get_ms_applycal_qascores(mslist, plot_output_path = output_path+'sd_applycal_output',
                                                    weblog_output_path = output_path+'sd_applycal_output',
                                                    sciline_det = True, buffer_data = buffer_data)

    #End time
    tend = systime.time()
    dtime_min = (tend-tstart)/60.0

    #Write reports used for testing
    sd_qa_reports.addFLSLentry(qascore_list, output_file = output_path+'/prototype_qa_score.csv', dtime_min = dtime_min)
    sd_qa_reports.makeSummaryTable(qascore_list, plots_fnames, plfolder, output_file = output_path+'/sd_applycal_output/qascore_summary.csv', working_folder = working_folder, weblog_adress = 'http://jaopost-web.sco.alma.cl/spool/sd-qascores/PIPEREQ-176/')
    sd_qa_reports.makeQAmsgTable(qascore_per_scan_list, plfolder, output_file = output_path+'/sd_applycal_output/qascores_details.csv')
 
    return

if __name__ == '__main__':
    main()

