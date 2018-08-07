import csv
import json
import argparse
import os
import uuid
from zipfile import ZipFile
from fuzzywuzzy import fuzz, process
from string import capwords

parser = argparse.ArgumentParser()

parser.add_argument("v3_json")
parser.add_argument("-o", "--output-dir",
                    help="The directory to output CSV and mapping files")
parser.add_argument("-m", "--mapping-dir",
                    help="The directory your .mapping zip files are in")

args = parser.parse_args()


v3_graphs = json.load(open(args.v3_json))
mapping_files = os.listdir(args.mapping_dir)
graphdiff_files = os.listdir('./resources/graphdiffs/')

# load mappings
mappings = {}

for mapping_file in mapping_files:
    mapping = json.load(ZipFile(args.mapping_dir + mapping_file)
                        .open(mapping_file.replace( '.zip', '.mapping')))
    mappings[mapping['resource_model_name']] = mapping


def convert_v3_rname(resource_name):
    return process.extractOne(
        capwords(resource_name.split( '.')[0]
                 .replace( '_', ' ')),
        mappings.keys())[0]


# create a dictionary of v3 and their corresponding v4 resource model names
resource_model_names = {rname: convert_v3_rname(rname)
                        for rname in
                        set([r['entitytypeid']
                             for r in v3_graphs['resources']])}


# load graph name changes from clojure tool into dictionaries
graphdiffs = {}
for name in resource_model_names.keys():
    with open('./resources/graphdiffs/' + process.extractOne(
            name, graphdiff_files)[0]) as graphdiff:
        graphdiffs[convert_v3_rname(name)] = json.load(graphdiff)

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

        if child['businesstablename'] == "domains":
            v4_field_data = child['label']
        else:
            v4_field_data = child['value']

        # don't attempt to migrate semantic nodes
        if (v4_fieldname is not None and child['businesstablename'] != ""):
            if ruuid not in v4_data[resource_name]:
                v4_data[resource_name][ruuid] = {}

            v4_data[resource_name][ruuid][v4_fieldname] = v4_field_data
            v4_data[resource_name][ruuid]['ResourceID'] = ruuid
            resource_fieldnames[resource_name].append(v4_fieldname)


for resource in v3_graphs['resources']:
    resource_name = convert_v3_rname(resource['entitytypeid'])
    ruuid = str(uuid.uuid4())
    process_children(resource['child_entities'], resource_name, ruuid)



#print(json.dumps(v4_data, indent=2, sort_keys=True))

for resource_model in v4_data.keys():
    filename = args.output_dir + resource_model + '.csv'
    with open(filename, 'w') as csvfile:
        fieldnames = ["ResourceID"]
        fieldnames += [field for field in set(resource_fieldnames[resource_model])]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        rows = v4_data[resource_model].values()
        for row in rows:
            writer.writerow({k:v.encode('utf8') for k, v in row.items()})
