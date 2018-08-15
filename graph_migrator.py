# coding: utf-8

import csv
import json
import argparse
import os
import uuid

from zipfile import ZipFile
from fuzzywuzzy import fuzz, process
from string import capwords
from datetime import datetime
from lxml import etree
parser = argparse.ArgumentParser()

parser.add_argument("v3_json")
parser.add_argument("-o", "--output-dir",
                    help="The directory to output CSV and mapping files")
parser.add_argument("-m", "--mapping-dir",
                    help="The directory your .mapping zip files are in")

args = parser.parse_args()


class DTFixer:
    def __init__(self):

        def fix_string(data):
            """string - Strings need not be single-quoted unless they contain a
            comma or contain HTML tags (as in the case strings
            display/collected with the rich text editor widget).
            """

            return data

        def fix_number(data):
            """number - Numbers don’t need quotes.
            """
            return data

        def fix_date(data):
            """date - All dates must be formatted as YYYY-MM-DD. Dates that are
            not four digits must be zero padded (01-02-1999). No
            quotes.
            """
            return datetime.strptime(data, "%Y-%m-%dT%H:%M:%S").date().isoformat()

        def fix_geojson(data):
            """geojson-feature-collection - All geometry must be formatted in
            “well-known text” (WKT), and all coordinates stored as WGS
            84 (EPSG:4326) decimal degrees. Multi geometries must be
            single-quoted.
            """
            return data

        def fix_concept(data):
            """concept - If the values in your concept collection are
            unique you can use the label (prefLabel) for a concept. If
            not, you will get an error during import and you must use
            UUIDs instead of labels (if this happens, see Concepts
            File below). If a prefLabel has a comma in it, it must be
            triple-quoted:

            concept-list - This must be a single-quoted list of
            prefLabels (or UUIDs if necessary): "Slate,Thatch". If a
            prefLabel contains a comma, then that prefLabel must have
            double-quotes: "Slate,""Shingles, original"",Thatch".
            """

            return data

        def fix_domain(data):
            return data

        self.fixers = {
            'string': fix_string,
            'number': fix_number,
            'date': fix_date,
            'geojson-feature-collection': fix_geojson,
            'concept': fix_concept,  # accounts for concept lists as well
            'domain': fix_domain
        }

        self.businesstable_datatypes = {
            "dates": 'date',
            "files": 'string',
            "geometries": 'geojson-feature-collection',
            "strings": 'string',
            "domains": 'concept'

        }

        self.mappings = {}
        self.p_uuids = {}

        for mapping_file in os.listdir(args.mapping_dir):

            with ZipFile(args.mapping_dir + mapping_file) as zip:
                mapping = json.load(
                    zip.open(mapping_file.replace('.zip',
                                                  '.mapping')))
                concepts = json.load(
                    zip.open(mapping_file.replace('.zip',
                                                  '_concepts.json')))
            for ctype in concepts.items():

                if str(type(ctype[1])) == "<type 'unicode'>":
                    print(ctype[1])
                else:
                    for concept in ctype[1].items():
                        self.p_uuids[concept[1]] = concept[0]

            self.mappings[mapping['resource_model_name']] = mapping

    def convert_v3_rname(self, resource_name):
        return process.extractOne(
            capwords(resource_name.split('.')[0]
                      .replace('_', ' ')),
            self.mappings.keys())[0]

    def fix_datatype(self, businesstable, data):
        dt = self.businesstable_datatypes[businesstable]
        return self.fixers[dt](data)


v3_graphs = json.load(open(args.v3_json))

graphdiff_files = os.listdir('./resources/graphdiffs/')

fixer = DTFixer()

# create a dictionary of v3 and their corresponding v4 resource model names
resource_model_names = {rname: fixer.convert_v3_rname(rname)
                        for rname in
                        set([r['entitytypeid']
                             for r in v3_graphs['resources']])}


# load graph name changes from clojure tool into dictionaries
graphdiffs = {}
for name in resource_model_names.keys():
    with open('./resources/graphdiffs/' + process.extractOne(
            name, graphdiff_files)[0]) as graphdiff:
        graphdiffs[fixer.convert_v3_rname(name)] = json.load(graphdiff)

v4_data = {rname: {} for rname in resource_model_names.values()}
resource_fieldnames = {rname: ["ResourceID"]
                       for rname in resource_model_names.values()}


def process_children(children, resource_name, ruuid):

    # recursively process children
    for child in children:
        children = child['child_entities']
        v4_fieldname = graphdiffs[resource_name][child['entitytypeid']]

        if len(children) > 0:
            process_children(children, resource_name, ruuid)

        v4_field_data = child['value']

        # don't attempt to migrate semantic nodes
        if (v4_fieldname is not None and child['businesstablename'] != ""):
            if ruuid not in v4_data[resource_name]:
                # an array of dictionaries so that duplicate
                # fieldnames with different values may be imported
                v4_data[resource_name][ruuid] = []

            fixed_field_data = fixer.fix_datatype(
                child['businesstablename'], v4_field_data)

            v4_data[resource_name][ruuid].append({
                'ResourceID': ruuid,
                v4_fieldname: fixed_field_data})

            resource_fieldnames[resource_name].append(v4_fieldname)


for resource in v3_graphs['resources']:
    resource_name = fixer.convert_v3_rname(resource['entitytypeid'])
    ruuid = str(uuid.uuid4())
    process_children(resource['child_entities'], resource_name, ruuid)


for resource_model in v4_data.keys():
    filename = args.output_dir + resource_model + '.csv'
    with open(filename, 'w') as csvfile:
        fieldnames = [field for field in
                      set(resource_fieldnames[resource_model])]
        fieldnames.remove("ResourceID")
        fieldnames.insert(0, "ResourceID")

        writer = csv.DictWriter(csvfile,
                                fieldnames=fieldnames)
        writer.writeheader()
        resources = v4_data[resource_model].values()
        for resource in resources:
            for row in resource:
                writer.writerow({k: v.encode('utf8') for k, v in row.items()})
