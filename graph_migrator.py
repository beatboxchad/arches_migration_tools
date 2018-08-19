# coding: utf-8

import csv
import json
import argparse
import os

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

args = parser.parse_args()


class DTFixer:
    def __init__(self):

        def fix_string(data):
            """string - Strings need not be single-quoted unless they contain a
            comma or contain HTML tags (as in the case strings
            display/collected with the rich text editor widget).
            """
            return '"""' + data + '"""'

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
            with ZipFile(args.mappings + mapping_file) as zip:
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
            json.dump(mapping,
                      open(args.output +
                           mapping['resource_model_name'] +
                           '.mapping', 'w'),
                      indent=4, sort_keys=True)

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


v3_graphs = json.load(open(args.v3_data))


fixer = DTFixer()

# create a dictionary of v3 and their corresponding v4 resource model names
resource_model_names = {rname: fixer.convert_v3_rname(rname)
                        for rname in
                        set([r['entitytypeid']
                             for r in v3_graphs['resources']])}


v4_data = {rname: {} for rname in resource_model_names.values()}
resource_fieldnames = {rname: ["ResourceID"]
                       for rname in resource_model_names.values()}


def process_children(children, resource_name, ruuid):
    # recursively process children
    for child in children:
        children = child['child_entities']

        v4_field_name = fixer.get_v4_fieldname(resource_name,
                                               child['entitytypeid'])

        if len(children) > 0:

            process_children(children, resource_name, ruuid)

        v4_field_data = child['value']

        fixed_field_data = fixer.fix_datatype(resource_name,
                                              v4_field_name,
                                              v4_field_data)

        # don't attempt to migrate semantic nodes
        if (v4_field_name is not None and child['businesstablename'] != ""):
            if ruuid not in v4_data[resource_name]:
                # an array of dictionaries so that duplicate
                # fieldnames with different values may be imported
                v4_data[resource_name][ruuid] = []

            v4_data[resource_name][ruuid].append({
                'ResourceID': ruuid,
                v4_field_name: fixed_field_data})

            resource_fieldnames[resource_name].append(v4_field_name)


for resource in v3_graphs['resources']:
    resource_name = fixer.convert_v3_rname(resource['entitytypeid'])
    ruuid = resource['entityid']
    process_children(resource['child_entities'], resource_name, ruuid)


for resource_model in v4_data.items():
    filename = args.output + resource_model[0] + '.csv'
    with open(filename, 'w') as csvfile:
        fieldnames = [field for field in
                      set(resource_fieldnames[resource_model[0]])]
        fieldnames.remove("ResourceID")
        fieldnames.insert(0, "ResourceID")

        writer = csv.DictWriter(csvfile,
                                fieldnames=fieldnames)
        writer.writeheader()
        for resource in resource_model[1].items():
            resource[1].append({"ResourceID": resource[0]})

            for row in resource[1]:
                writer.writerow({k: v.encode('utf8') for k, v in row.items()})