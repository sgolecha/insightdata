import random
import string
import pprint
import optparse
import json

def randomword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def gen_test_with_same_timestamp(outputFile, tstamp, count):
    data = {}
    data['created_time'] = tstamp

    with open(outputFile, 'w') as ofile:
        for x in range(count):
            source = randomword(12)
            data['target'] = source
            actor = randomword(12)
            data['actor'] = actor
            ofile.write("%s\n" %json.dumps(data))


def main():
    parser = optparse.OptionParser(usage='\n%prog [options]' +
                                         '\nexample: ' +
                                         '\n\ttest_gen.py <output-file>')
    parser.disable_interspersed_args()
    
    (options, args) = parser.parse_args()
  
    if len(args) == 0:
        parser.error("no output file specified")

    gen_test_with_same_timestamp(args[0], "2016-03-28T23:23:12Z", 1000000)
    
if __name__ == "__main__":
    main()
