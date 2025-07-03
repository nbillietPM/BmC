#!/bin/bash

#The script should be executed in a way that the following things are provided
#a json object containing the query

if [ $# -lt 1 ]; then #check if 1 argument is passed along with the script along execution
    echo "Usage: ./cubeGeneration.sh <sqlQuery.json>"
    exit 1 #code 1 terminates the script indicating an error
fi

queryFile=$1

if [[ "$queryFile" != *.json ]]; then
   echo "The provided query file should be stored in a .json file"
   exit 1
fi 

#the curl commands assumes that your GBIF credentials are stored in the ~/.bashrc config
echo "Submitting query to GBIF"
curl --include --user $GBIF_USERNAME:$GBIF_PASSWORD --header "Content-Type: application/json" --data @$queryFile https://api.gbif.org/v1/occurrence/download/request