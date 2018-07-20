"""
This utility converts an Arches 3 SKOS-formatted scheme file into
an Arches 4 thesaurus file and collections file for import
"""

from lxml import etree
from rdflib import Graph
import os
import json
import uuid
import argparse


parser = argparse.ArgumentParser()

parser.add_argument("skosfile",
                    help="The Arches V3 Scheme you've exported from the Reference Data Manager")

parser.add_argument("-u", "--uri",
                    nargs="?",
                    default="http://www.archesproject.org/",
                    help="(Recommended) The root URI of your Arches installation's RDM (https://www.example.com/rdm/)"
                    )
parser.add_argument("-d", "--directory",
                    nargs="?",
                    default="Arches",
                    help="The name of the directory to save data files in")


args = parser.parse_args()


def update_arches_namespace(xml_str):
    "substitute www.archesproject.org with the installation domain"
    return xml_str.replace("http://www.archesproject.org/", args.uri)


def prepare_export(namespaces, nodes):
    """
    return a graph with the desired node type for writing out to XML,
    with cleaned-up namespaces
    """

    output_graph = Graph()
    [output_graph.bind(k, v) for k, v in namespaces.items()]

    [output_graph.parse(
        data=etree.tostring(node),
        nsmap=namespaces)
     for node in nodes]

    return output_graph


def export(filename, content):
    directory = args.directory
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(directory + '/' + filename, 'w') as fh:
        fh.write(content)
        fh.close


with open(args.skosfile, 'r') as incoming_skos:

    uuid_file = 'collection_uuids.json'

    with open(uuid_file, 'r') as uuid_store:
        uuids = json.load(uuid_store)

    def new_or_existing_uuid(preflabel):
        """
        otake a topConcept's prefLabel node, parse the JSON within, and
        return a new or existing UUID for the collection based on
        whether we already have one for the JSON's `value` key

        """
        preflabel_val = json.loads(preflabel.text)['value']

        if preflabel_val in uuids:
            return uuids[preflabel_val]
        else:
            new_uuid = str(uuid.uuid4())
            uuids[preflabel_val] = new_uuid
            return new_uuid

    def new_preflabel_uuid(preflabel):
        """
        Concept Values in Arches have a UUID (stored in the embedded JSON)
        and a 1-1 mapping with their Concept; if a Collection shares a
        prefLabel with a Concept, it needs a new ID to avoid
        clobbering the same attribute on the existing Concept
        """
        working = json.loads(preflabel.text)
        working['id'] = unicode(uuid.uuid4())
        preflabel.text = json.dumps(working)

    fixed_ns_raw = update_arches_namespace(incoming_skos.read())
    skos_xml = etree.fromstring(fixed_ns_raw)
    namespaces = skos_xml.nsmap

    # retrieve concepts and collections from skos with regular ol' xpath
    concepts = skos_xml.xpath("./skos:Concept", namespaces=namespaces)
    collections = skos_xml.xpath("./skos:Collection", namespaces=namespaces)
    top_concepts = skos_xml.xpath(".//skos:hasTopConcept", namespaces=namespaces)

    top_concept_uris = [
        concept.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
        for concept in top_concepts]

    for concept in concepts:
        uri = concept.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
        if uri in top_concept_uris:
            # create a new top level Collection based on the topConcept
            collection = etree.Element(
                "{http://www.w3.org/2004/02/skos/core#}Collection",
                nsmap=namespaces)

            # migrate nested concepts and attributes into new
            # Collection
            #################################################

            # hacky-deepcopy the TopConcept to avoid mutating original
            # (still needed for thesaurus export)
            working_concept = etree.fromstring(etree.tostring(concept))

            # give the collection a UUID based on the prefLabel
            col_preflabel = working_concept.find('./skos:prefLabel',
                                                 namespaces=namespaces)

            new_preflabel_uuid(col_preflabel)

            col_uuid = new_or_existing_uuid(col_preflabel)
            fq_uuid = namespaces['arches'] + col_uuid
            collection.set("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about",
                           fq_uuid)

            # change skos:narrower into valid skos:member tags in the
            # Concept's first-level children
            for narrower in working_concept.xpath("skos:narrower",
                                                  namespaces=namespaces):
                narrower.tag = "{http://www.w3.org/2004/02/skos/core#}member"

            # Append the child concepts to the new Collection
            [collection.append(child) for child in
             working_concept.getchildren()]

            collections.append(collection)

    # prepare raw XML
    thesaurus_skos = prepare_export(namespaces,
                                    concepts).serialize(format='pretty-xml')
    collections_skos = prepare_export(namespaces,
                                      collections).serialize(format='pretty-xml')

    # export files
    export('thesaurus.xml', thesaurus_skos)
    export('collections.xml', collections_skos)

    # write UUID's
    with open(uuid_file, 'w') as uuid_store:
        uuid_store.write(json.dumps(uuids, indent=4, sort_keys=True))
        uuid_store.close()
