import os
import sys
import optparse
import pprint
import time
import datetime
import json
import graph

def convert_to_unixtime(input_time_str):
    x = time.strptime(input_time_str, "%Y-%m-%dT%H:%M:%SZ")
    return int(time.mktime(x))

def process_payments(inputFile, outputFile):
    txGraph = graph.TxGraph()
    with open(outputFile, 'w') as ofile:
        with open(inputFile, 'r') as ifile:
            for line in ifile.readlines():
                try:
                    parsedPayment = json.loads(line)
                    tstamp = convert_to_unixtime(parsedPayment['created_time'])
                    txGraph.process_transaction(tstamp, parsedPayment['target'], parsedPayment['actor'])
                    ofile.write("%.2f\n" %txGraph.median)       
                except Exception as e:
                    pprint.pprint("Error in line('%s'): '%s'" %(line, str(e)))

def main():
    parser = optparse.OptionParser(usage='\n%prog [options]' +
                                         '\nexample: ' +
                                         '\n\trolling_median.py -i <input-file> -o <output-file>')
    parser.disable_interspersed_args()
    
    parser.add_option('-i', '--inputfile',
                    default=None,
                    help='input file to read payments from')

    parser.add_option('-o', '--outputfile',
                    default=None,
                    help='output file to write the median value')
    
    (options, args) = parser.parse_args()
   
    if not options.inputfile:
        parser.error("no valid input file")

    if not options.outputfile:
        parser.error("no valid output file")

    process_payments(options.inputfile, options.outputfile)

if __name__ == "__main__":
    main()
