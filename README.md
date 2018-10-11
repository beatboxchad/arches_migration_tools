### Arches v3 to v4 Migration Tools

#### graph_migrator.py

python migrator.py v3data.json [options]

-o/--output path to output directory

-m/--mappings path to directory with all mapping zip files

-v/--verbose sets logging level to debug and prints to console

--process-model names of the resource models to process. by default, all are
processed. these must be the **v3** names, separated by a space. For example:

    --process-model HERITAGE_RESOURCE.E18 ACTOR.E39
    
would only process resources in those two categories.