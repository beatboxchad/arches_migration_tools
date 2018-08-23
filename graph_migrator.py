# coding: utf-8

import csv
import json
import argparse
import os
import logging

from zipfile import ZipFile
from fuzzywuzzy import process
from string import capwords
from datetime import datetime

parser = argparse.ArgumentParser()

parser.add_argument("v3_data")
parser.add_argument("-o", "--output",
                    help="The directory to output CSV and mapping files")
parser.add_argument("-m", "--mappings",
                    help="The directory your .mapping zip files are in")
parser.add_argument("-v", "--verbose", action="store_true",
                    help="Turns on debug logging and prints to console")

args = parser.parse_args()

class DTFixer:
    def __init__(self):

        def fix_string(data):
            """string - Strings need not be single-quoted unless they contain a
            comma or contain HTML tags (as in the case strings
            display/collected with the rich text editor widget).
            """
            return "'" + data + "'"

        def fix_number(data):
            """number - Numbers don’t need quotes.
            """
            return data

        def fix_date(data):
            """date - All dates must be formatted as YYYY-MM-DD. Dates that are
            not four digits must be zero padded (01-02-1999). No
            quotes.
            """
            # This is brittle, developed for one specific installation
            if data == '':
                return data
            return datetime.strptime(data,
                                     "%Y-%m-%dT%H:%M:%S").date().isoformat()

        def fix_geojson(data):
            """geojson-feature-collection - All geometry must be formatted in
            “well-known text” (WKT), and all coordinates stored as WGS
            84 (EPSG:4326) decimal degrees. Multi geometries must be
            single-quoted.
            """
            # All the WKT looks valid other than the same error we
            # encounter with Elasticsearch's pedantry elsewhere; when
            # we figure out how to fix that, the logic will go here.
            return data

        def fix_concept(data):
            """concept - If the values in your concept collection are
            unique you can use the label (prefLabel) for a concept. If
            not, you will get an error during import and you must use
            UUIDs instead of labels (if this happens, see Concepts
            File below). If a prefLabel has a comma in it, it must be
            triple-quoted:
            """
            # V3 JSON holds the Preflabel in "Label" and the UUID in
            # "Value" which we pass anyway, so no action is necessary
            return data

        def fix_list(data):
            """
            concept-list - This must be a single-quoted list of
            prefLabels (or UUIDs if necessary): "Slate,Thatch". If a
            prefLabel contains a comma, then that prefLabel must have
            double-quotes: "Slate,""Shingles, original"",Thatch".
            """

            split_data = data.split()
            if len(split_data) > 1:
                return "'{}'".format(data)
            return data

        self.fixers = {
            'string': fix_string,
            'number': fix_number,
            'date': fix_date,
            'geojson-feature-collection': fix_geojson,
            'concept': fix_concept,
            'concept-list': fix_list,
            'domain-value': fix_concept,
            'domain-value-list': fix_list,
            'file-list': fix_list
        }

        self.mappings = {}
        self.p_uuids = {}
        self.graphdiffs = {}
        self.names_n_dts = {}

        # load mappings
        for mapping_file in os.listdir(args.mappings):
            with ZipFile(os.path.join(args.mappings,mapping_file)) as zip:
                mapping = json.load(
                    zip.open(mapping_file.replace('.zip',
                                                  '.mapping')))
                resource_name = mapping['resource_model_name']
                self.names_n_dts[resource_name] = {node['arches_node_name']:
                                                   node['data_type'] for node in
                                                   mapping['nodes']}
                concepts = json.load(
                    zip.open(mapping_file.replace('.zip',
                                                  '_concepts.json')))

            for ctype in concepts.items():
                if str(type(ctype[1])) == "<type 'unicode'>":
                    print(ctype[1])
                else:
                    for concept in ctype[1].items():
                        self.p_uuids[concept[1]] = concept[0]

            self.mappings[resource_name] = mapping

            # load graph name changes from clojure tool
            graphdiff_filename = process.extractOne(
                resource_name,
                os.listdir('./resources/graphdiffs/')
            )[0]

            with open('./resources/graphdiffs/' + graphdiff_filename) as gdiff:
                self.graphdiffs[resource_name] = json.load(gdiff)

        # This is a little messy, but I don't wanna manually unzip the
        # mapfiles and I've got 'em in memory anyway. So go ahead and
        # write out the mapping file JSON for easy import
            writeout_path = os.path.join(args.output,resource_name+'.mapping')
            with open(writeout_path,'w') as outfile:
                json.dump(mapping, outfile, indent=4, sort_keys=True)

    def convert_v3_rname(self, resource_name):
        return process.extractOne(
            capwords(resource_name.split('.')[0]
                      .replace('_', ' ')),
            self.mappings.keys())[0]

    def get_v4_fieldname(self, resource_name, field_name):
        # two-tier check, look at the graphdiffs from the clojure tool
        # and look at the mapping file
        mapping_fieldnames = [node['arches_node_name'] for node in
                              self.mappings[resource_name]['nodes']]

        if self.graphdiffs[resource_name][field_name] is None:
            return process.extractOne(capwords(field_name
                                               .split('.')[0]
                                               .replace("_", " ")),
                                      mapping_fieldnames)[0]
        else:
            return process.extractOne(
                self.graphdiffs[resource_name][field_name],
                mapping_fieldnames)[0]

    def fix_datatype(self, resource_name, field_name, data):
        dt = self.names_n_dts[resource_name][field_name]
        return self.fixers[dt](data)
        
def get_logger(level='info'):

    logFormatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s",
        datefmt='%m-%d-%y %H:%M:%S')
    logger = logging.getLogger()
    

    fileHandler = logging.FileHandler("{0}/{1}.log".format('logs', 'business_data_conversion'))
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    if level == "debug":
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        logger.addHandler(consoleHandler)
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    return logger
    
class Migrator:
    
    def __init__(self,v3_json_file=''):
        
        self.v3_json_file = v3_json_file
        self.v3_json = self.parse_v3_json(self.v3_json_file)
        self.fixer = DTFixer()
        
        self.v4_data = {}
        self.resource_fieldnames = {}

    def parse_v3_json(self,json_file_path):
        if json_file_path == "":
            return ""
        with open(json_file_path, 'rb') as opendata:
            v3_data = json.load(opendata)
            
        v3_sorted = {}
        for r in v3_data['resources']:
            if not r['entitytypeid'] in v3_sorted.keys():
                v3_sorted[r['entitytypeid']] = [r]
            else:
                v3_sorted[r['entitytypeid']].append(r)

        logger.info('v3 business data loaded')
        logger.debug(str(len(v3_data['resources'])) + ' resources')
        
        return v3_sorted

    def process_children(self, children, resource_name, ruuid):
        # recursively process children
        for child in children:
            children = child['child_entities']

            v4_field_name = self.fixer.get_v4_fieldname(resource_name,
                                                   child['entitytypeid'])

            if len(children) > 0:

                self.process_children(children, resource_name, ruuid)

            v4_field_data = child['value']
            if v4_field_data == '94d3bc4e-a4b6-492d-bf7e-a62e4a077c9d':
                v4_field_data = 'e4be3321-a3e5-42f3-b397-93fcb1401342'

            fixed_field_data = self.fixer.fix_datatype(resource_name,
                                                  v4_field_name,
                                                  v4_field_data)

            # don't attempt to migrate semantic nodes
            if (v4_field_name is not None and child['businesstablename'] != ""):
                if ruuid not in self.v4_data[resource_name]:
                    # an array of dictionaries so that duplicate
                    # fieldnames with different values may be imported
                    self.v4_data[resource_name][ruuid] = []

                self.v4_data[resource_name][ruuid].append({
                    'ResourceID': ruuid,
                    v4_field_name: fixed_field_data})

                self.resource_fieldnames[resource_name].append(v4_field_name)

    def migrate_data(self,outdir='',mapping_dir=''):

        # create a dictionary of v3 and their corresponding v4 resource model names
        resource_model_names = {rname: self.fixer.convert_v3_rname(rname)
                                for rname in self.v3_json.keys()}

        self.resource_fieldnames = {rname: ["ResourceID"]
                               for rname in resource_model_names.values()}

        for resource_type,resources in self.v3_json.iteritems():

            rm_name = resource_model_names[resource_type]
            self.v4_data[rm_name] = {}
            
            logger.info("processing {} resources ({})".format(
                resource_type,len(resources)))
            for resource in resources:

                ruuid = resource['entityid']
                self.process_children(resource['child_entities'], rm_name, ruuid)
            logger.info("{} resources processed".format(len(self.v4_data[rm_name])))
            filename = os.path.join(outdir, rm_name + '.csv')
            logger.debug("writing...".format(filename))
            with open(filename, 'w') as csvfile:
                fieldnames = [field for field in
                              set(self.resource_fieldnames[rm_name])]
                fieldnames.remove("ResourceID")
                fieldnames.insert(0, "ResourceID")

                writer = csv.DictWriter(csvfile,
                                        fieldnames=fieldnames)
                writer.writeheader()
                for resource in self.v4_data[rm_name].values():
                    for row in resource:
                        writer.writerow({k: v.encode('utf8') for k, v in row.items()})
            del self.v4_data[rm_name]
            logger.info("written to {}".format(filename))

if __name__ == "__main__":

    if args.verbose:
        lvl = "debug"
    else:
        lvl = "info"
        
    logger = get_logger(lvl)
        
    migrator = Migrator(v3_json_file=args.v3_data)
    migrator.migrate_data(
        outdir=args.output,
        mapping_dir=args.mappings,
    )
