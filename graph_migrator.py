# coding: utf-8
import unicodecsv as csv
import json
import argparse
import os
import logging

from zipfile import ZipFile
from fuzzywuzzy import process
from string import capwords
from datetime import datetime


class DTFixer:
    def __init__(self):
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

            return data

        def fix_filepath(data):
            """
            file paths may need to be modified upon upload
            """

            return os.path.basename(data)

        self._fixers = {
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

    def fix_datatype(self, datatype, data):

        return self._fixers[datatype](data)


class GraphDiff:
    def __init__(self, resource_name, path):

        with open(path, 'r') as gdiff:
            self._name = resource_name
            self._data = json.load(gdiff)

    @property
    def data(self):
        return self._data

    @property
    def name(self):
        return self._name


class Mapping:
    def __init__(self, path):
        self._node_datatypes = {}
        self._preflabel_uuids = {}
#        self._logger = logging.getLogger('graph_migrator')
        self._dir, self._filename = os.path.split(path)
        with ZipFile(path) as mzip:
            mapping = json.load(
                mzip.open(self._filename.replace('.zip',
                                                 '.mapping')))
            self._data = mapping
            self._resource_name = mapping['resource_model_name']

            self._node_datatypes = {node['arches_node_name']:
                                    node['data_type'] for node in
                                    mapping['nodes']}
            concepts = json.load(
                mzip.open(self._filename.replace('.zip',
                                                 '_concepts.json')))

            for ctype in concepts.items():
                if str(type(ctype[1])) == "<type 'unicode'>":
                    # self._logger.debug(ctype[1])
                    pass
                else:
                    for concept in ctype[1].items():
                        self._preflabel_uuids[concept[1]] = concept[0]

        self._fieldnames = [node['arches_node_name'] for node in
                            self.data['nodes']]

    @property
    def data(self):
        return self._data

    @property
    def dir(self):
        return self._dir

    def write(self, output_dir):
        writeout_path = os.path.join(output_dir,
                                     self._resource_name+'.mapping')
        with open(writeout_path, 'w') as outfile:
            json.dump(self._data,
                      outfile,
                      indent=4,
                      sort_keys=True)


class DataConverter:
    """A DataConverter encasulates a v4 Resource Model Mapping and a
     Legion-generated graphdiff , and provides some calculated data
     based on these two sources to translate node names and data from
     v3 to v4.
    """

    def __init__(self, mapping, graphdiff):
        self._graphdiff = graphdiff
        self._mapping = mapping

    @property
    def mapping(self):
        return self._mapping

    @property
    def v4_fieldnames(self):
        return [u"ResourceID"] + self.mapping._fieldnames

    @property
    def resource_name(self):
        return self.mapping._resource_name

    @property
    def graphdiff(self):
        return self._graphdiff

    def get_datatype(self, node_name):
        return self.mapping._node_datatypes[node_name]

    def convert_v3_fieldname(self, field_name):

        if field_name in self.graphdiff.data:
            return process.extractOne(
                self.graphdiff.data[field_name],
                self.v4_fieldnames)[0]

            # return self.graphdiff.data[field_name]
        else:
            return process.extractOne(capwords(field_name
                                               .split('.')[0]
                                               .replace("_", " ")),
                                      self.mapping.v4_fieldnames)[0]


class ResourceModelMigrator:

    # Migrates all resources in a Resource Model

    def __init__(self, name, converter):

        self._converter = converter
        self._resources = []
        self._fixer = DTFixer()

    @property
    def v4_name(self):
        return self.converter.resource_name

    @property
    def converter(self):
        return self._converter

    @property
    def fixer(self):
        return self._fixer

    @property
    def resources(self):
        return self._resources

    def add_resource(self, resource):
        self._resources.append(resource)

    def get_v4_rows(self, v4_nodes, resource_id):
        """condenses the input resource data into a set of rows that can be
            written to the csv."""

        resource = list(v4_nodes)
        outrows = []
        newrow = {u"ResourceID": resource_id}

        while len(resource) > 0:

            node = resource.pop()  # This modifies in-place
            if node[0] in newrow.keys():
                outrows.append(newrow)
                newrow = {u"ResourceID": resource_id,
                          node[0]: node[1]}

            else:
                newrow[node[0]] = node[1]
        outrows.append(newrow)
        return outrows

    def convert_v3_rows(self, v3_nodes):
        v4_nodes = []
        for node in v3_nodes:
            v4_name = self.converter.convert_v3_fieldname(node[0])
            datatype = self.converter.get_datatype(v4_name)

            v4_value = self.fixer.fix_datatype(datatype, node[1])

            v4_nodes.append((v4_name, v4_value))
        return v4_nodes

    def migrate(self):
        rows = []
        for resource in self.resources:
            nodes = resource.nodes
            rows += self.get_v4_rows(self.convert_v3_rows(nodes),
                                     resource.resource_id)
        return rows


class Resource:
    # knows its data and ID
    def __init__(self, data):
        self._uuid = data['entityid']
        self._v3data = [data]

        def process_children(children=self._v3data, processed=[]):

            for child in children:
                grandchildren = child['child_entities']

                field_name = child['entitytypeid']

                if len(grandchildren) > 0:
                    process_children(grandchildren,
                                     processed)

                field_data = child['value']

                # don't attempt to migrate semantic nodes
                if (field_name is not None and
                        child['businesstablename'] != ""):

                    processed.append((
                        field_name, field_data))

            return processed
        self._nodes = process_children()

    @property
    def resource_id(self):
        return self._uuid

    @property
    def nodes(self):
        return self._nodes


class Migration:
    # this class handles details like the resource/output locations,
    # configuration, IO, and worker spawning

    def __init__(self, v3_file, mappings_dir, output_dir, models_to_use,
                 config=".migrator_config.json"):
        self._output_dir = output_dir
        self._resource_models = {}
        self._models_to_use = models_to_use

        with open(config, 'r') as config:
            self._config = json.load(config)

        self.import_v3_resources(v3_file, mappings_dir)

    @property
    def resource_models(self):
        return self._resource_models
        
    @property
    def models_to_use(self):
        return self._models_to_use

    def import_v3_resources(self, v3_file, mappings_dir):

        v3_sorted = {}
        namediffs = self._config['namediffs']
        graphdiffs = self._config['graphdiffs']
        graphdiff_path = self._config['graphdiff_path']

        with open(v3_file, 'rb') as opendata:
            v3_data = json.load(opendata)

            for r in v3_data['resources']:

                entitytypeid = r['entitytypeid']

                if not "<all>" in self.models_to_use and not\
                    entitytypeid in self.models_to_use:
                    continue

                if not entitytypeid in v3_sorted.keys():
                    v3_sorted[entitytypeid] = [r]
                else:
                    v3_sorted[entitytypeid].append(r)

        for name, resources in v3_sorted.iteritems():
            mapping_path = mappings_dir + namediffs[name] + ".zip"

            graphdiff = GraphDiff(name, graphdiff_path +
                                  graphdiffs[name])
            mapping = Mapping(mapping_path)
            converter = DataConverter(mapping, graphdiff)

            model = ResourceModelMigrator(name, converter)

            for resource in resources:
                model.add_resource(Resource(resource))

            self._resource_models[model.v4_name] = model

    def migrate_data(self, process_model='<all>'):
        for rm_name, migrator in self._resource_models.iteritems():

            filename = os.path.join(self._output_dir, rm_name + '.csv')
            migrator.converter.mapping.write(self._output_dir)

            with open(filename, 'wb') as csvfile:
                fieldnames = migrator.converter.v4_fieldnames
                writer = csv.DictWriter(csvfile,
                                        fieldnames=fieldnames,
                                        encoding='utf-8-sig')
                writer.writeheader()
                rows = migrator.migrate()
                for row in rows:
                    writer.writerow(row)


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

    parser = argparse.ArgumentParser()

    parser.add_argument("v3_data")
    parser.add_argument("-o", "--output",
                        help="The directory to output CSV and mapping files")
    parser.add_argument("-m", "--mappings",
                        help="The directory your .mapping zip files are in")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Turns on debug logging and prints to console")
    parser.add_argument("--process-model", nargs="+", default=["<all>"],
                        help="Allows you to pass the name of a single resource "
                        "model to process")

    args = parser.parse_args()

    if args.verbose:
        lvl = "debug"
    else:
        lvl = "info"

    logger = get_logger(lvl)

    migrator = Migration(args.v3_data, args.mappings, args.output, args.process_model)

    migrator.migrate_data()
