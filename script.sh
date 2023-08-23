#!/bin/bash

search_dir=src/main/resources/www.liquibase.org/xml/ns/neo4j
          filenames=`ls -1 $search_dir`
          mkdir index-file
          cp $search_dir/index.htm index-file/
          for entry in $filenames
          do
            if [[ "$entry" != "liquibase-neo4j-latest.xsd" ]] && [[ "$entry" != "index.htm" ]] ;then
              sed -ie "s/<\/ul>/  <li><a href=\"\/xml\/ns\/neo4j\/${entry}\">${entry}<\/a><\/li>\n<\/ul>/" index-file/index.htm
            fi
          done