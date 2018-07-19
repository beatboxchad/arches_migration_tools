"""
This utility converts an Arches 3 SKOS-formatted scheme file into
an Arches 4 thesaurus file and collections file for import
"""

from lxml import etree
from rdflib import Graph
import json
import argparse
import os

parser = argparse.ArgumentParser()

parser.add_argument("skosfile",
                    help="The Arches V3 Scheme you've exported from the Reference Data Manager")

parser.add_argument("-u", "--uri",
                    nargs="?",
                    default="http://www.archesproject.org/",
                    help="(Recommended) The root URI of your Arches installation (https://www.example.com/rdm/)"
                    )
parser.add_argument("-d", "--directory",
                    nargs="?",
                    default="Arches",
                    help="The name of the directory to save data files in")


args = parser.parse_args()

# Need to make Collections from each TopConcept.
# Need to investigate the Arches built-ins.
# The UUID's need to be generated for the new Collections.


def update_arches_namespace(xml_str):
    "substitute www.archesproject.org with the installation domain"
    return xml_str.replace("http://www.archesproject.org", args.uri)


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

    thesaurus_skos = prepare_export(namespaces, concepts).serialize(format='pretty-xml')

    for concept in concepts:
        uri = concept.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
        if uri in top_concept_uris:
            # create a new top level Collection to hold the concepts
            collection = etree.Element(
                "{http://www.w3.org/2004/02/skos/core#}Collection",
                nsmap=namespaces)
            # Copy the TopConcept's attributes to it
            [collection.set(attr[0], attr[1]) for attr in concept.attrib.items()]

            # Change skos:narrower into valid skos:member tags in the Concept's children

            for narrower in concept.xpath("skos:narrower", namespaces=namespaces):
                narrower.tag = "{http://www.w3.org/2004/02/skos/core#}member"

            # Append the child concepts to the new Collection
            [collection.append(child) for child in concept.getchildren()]

            collections.append(collection)

    # prepare raw XML

    collections_skos = prepare_export(namespaces, collections).serialize(format='pretty-xml')

    export('thesaurus.xml', thesaurus_skos)

    export('collections.xml', collections_skos)
