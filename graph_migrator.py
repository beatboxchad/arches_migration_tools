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
parser.add_argument("--process-model", default="<all>",
                    help="Allows you to pass the name of a single resource "
                    "model to process")

args = parser.parse_args()


class DTFixer:
    def __init__(self, mappings_dir, output_dir):

        def fix_string(data):
            """string - Strings need not be single-quoted unless they contain a
            comma or contain HTML tags (as in the case strings
            display/collected with the rich text editor widget).
            """
            # don't manually quote strings for now.
            return data

        def fix_number(data):
            """number - Numbers don’t need quotes.
            """
            output = str(data).replace(",", "")
            return output

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

        def fix_filepath(data):
            """
            file paths may need to be modified upon upload
            """
            data = data.split('/')
            return data[-1]

        self.fixers = {
            'string': fix_string,
            'number': fix_number,
            'date': fix_date,
            'geojson-feature-collection': fix_geojson,
            'concept': fix_concept,
            'concept-list': fix_list,
            'domain-value': fix_concept,
            'domain-value-list': fix_list,
            'file-list': fix_filepath
        }

        self.mappings = {}
        self.p_uuids = {}
        self.graphdiffs = {}
        self.names_n_dts = {}

        # load mappings
        for mapping_file in os.listdir(mappings_dir):
            if not mapping_file.endswith(".zip"):
                continue
            with ZipFile(os.path.join(mappings_dir, mapping_file)) as zip:
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
        # It would be good to further refactor this so that the output dir
        # does not need to be passed to this class.
            writeout_path = os.path.join(output_dir, resource_name+'.mapping')
            with open(writeout_path, 'w') as outfile:
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

        # avoids possible key errors below
        if not field_name in self.graphdiffs[resource_name]:
            return None

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


class Migrator:

    def __init__(self, v3_json_file, mappings_dir, output_dir):

        self.v3_json_file = v3_json_file
        self.v3_json = self.parse_v3_json(self.v3_json_file)
        self.fixer = DTFixer(mappings_dir, output_dir)
        self.output_dir = output_dir
        self.mappings_dir = mappings_dir

        self.v4_data = {}
        self.resource_fieldnames = {}

    def parse_v3_json(self, json_file_path):
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

            if v4_field_name is None:
                continue

            v4_field_data = child['value']
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

    def create_resource_rows(self, resource):
        """condenses the input resource data into a set of rows that can be
        written to the csv."""

        outrows = []

        while len(resource) > 0:
            logger.debug("-"*80)
            newrow = {}
            added = []

            for index, row in enumerate(resource):
                for node_name, value in row.iteritems():
                    if not node_name in newrow.keys():
                        added.append(index)
                        # logger.debug("adding {} to new row".format(node_name))
                        newrow[node_name] = value

                        # encode to ascii only for logging
                        v = value.encode('ascii', 'ignore')
                        if len(v) > 50:
                            v = v[:50]
                        logger.debug("{}: {}".format(node_name, v))

            outrows.append(newrow)

            # strip out nodes that were just added and run the loop again
            resource = [v for i, v in enumerate(resource) if not i in added]

        return outrows

    def migrate_data(self, process_model='<all>'):

        # create a dictionary of v3 and their corresponding v4 resource model names
        resource_model_names = {rname: self.fixer.convert_v3_rname(rname)
                                for rname in self.v3_json.keys()}

        self.resource_fieldnames = {rname: ["ResourceID"]
                                    for rname in resource_model_names.values()}

        for resource_type, resources in self.v3_json.iteritems():

            rm_name = resource_model_names[resource_type]

            if process_model != rm_name and process_model != '<all>':
                continue

            self.v4_data[rm_name] = {}

            logger.info("processing {} resources ({})".format(
                resource_type, len(resources)))
            for resource in resources:

                ruuid = resource['entityid']
                self.process_children(resource['child_entities'], rm_name, ruuid)
            logger.info("{} resources processed".format(len(self.v4_data[rm_name])))
            filename = os.path.join(self.output_dir, rm_name + '.csv')
            logger.debug("writing...".format(filename))

            with open(filename, 'wb') as csvfile:
                fieldnames = [field for field in
                              set(self.resource_fieldnames[rm_name])]
                fieldnames.remove("ResourceID")
                fieldnames.insert(0, "ResourceID")

                writer = csv.DictWriter(csvfile,
                                        fieldnames=fieldnames)
                writer.writeheader()
                for resource in self.v4_data[rm_name].values():
                    rows = self.create_resource_rows(resource)
                    for row in rows:
                        writer.writerow({k: v.encode('utf8') for k, v in row.items()})

            # this line could be commented out if it's desirable to have the entire
            # v4_data object persist. currently, memory cleanup is prioritized.
            del self.v4_data[rm_name]

            logger.info("written to {}".format(filename))


def get_logger(level='info'):

    logFormatter = logging.Formatter(u"%(asctime)s [%(levelname)s]  %(message)s",
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


if __name__ == "__main__":

    if args.verbose:
        lvl = "debug"
    else:
        lvl = "info"

    logger = get_logger(lvl)

    # it would be better to not have to instantiate the migrator calss with
    # the output directory, but right now it is necessary because that dir
    # should be passed to DTFixer when it is instantiated.
    migrator = Migrator(args.v3_data, args.mappings, args.output)
    migrator.migrate_data(process_model=args.process_model)
