"""
This utility converts an Arches 3 SKOS-formatted scheme file into
an Arches 4 thesaurus file and collections file for import
"""

from lxml import etree
from rdflib import Graph
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("skosfile",
                    help="The Arches V3 Scheme you've exported from the Reference Data Manager")

parser.add_argument("-u", "--uri",
                    nargs="?",
                    default="http://www.archesproject.org/",
                    help="(Recommended) The root URI of your Arches installation (https://www.example.com/rdm/)"
                    )


args = parser.parse_args()


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

    return output_graph.serialize(format='pretty-xml')


def export(filename, content):
    with open(filename, 'w') as fh:
        fh.write(content)
        fh.close


with open(args.skosfile, 'r') as incoming_skos:
    incoming_skos = update_arches_namespace(incoming_skos.read())
    skos_xml = etree.fromstring(incoming_skos)
    namespaces = skos_xml.nsmap

    # retrieve concepts and collections from skos with regular ol' xpath
    concepts = skos_xml.findall("./{http://www.w3.org/2004/02/skos/core#}Concept")
    collections = skos_xml.findall("./{http://www.w3.org/2004/02/skos/core#}Collection")

    export('thesaurus.xml',
           prepare_export(namespaces, concepts))

    export('collections.xml',
           prepare_export(namespaces, collections))
