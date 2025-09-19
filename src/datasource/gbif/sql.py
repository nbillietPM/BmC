import pygbif
import pandas as pd
import os
import itertools
import shapely
import json

def read_species_names(inpFile, inpPath="", sep=","):
    """
    A function to read in the names of different species of interest that are stored in a csv file.
    This function provides automatic formatting

    Args:
        inp_file (str): The name for the file containing all the species names
        inp_path (str, optional): Path where the input file is stored. The default is empty and will be read from the working directory
        sep (str, optional): The separator character used in between species names. The standard used is ',', i.e. comma separated values
    Returns:
        species_names (pd.Dataframe): A dataframe containing all the relevant information stored in the GBIF taxonomic backbone
    """
    with open(os.path.join(inpPath, inpFile), "r") as f:
        fileLines = f.readlines()
    #Read lines up until second last character to drop '\n' new line command
    #Split the lines based on the used separator in the file
    species_names = [line[:-2].split(sep) for line in fileLines]
    #Convert list of lists into a single list
    species_names = list(itertools.chain.from_iterable(species_names))
    #Trim whitespace from the names
    species_names = [name.strip() for name in species_names]
    #Remove the empty elements from the list
    species_names = list(filter(None, species_names))
    #Remove duplicates and return list of unique species
    return list(set(species_names))

def fetch_taxon_info(inp_file, inp_path="", out_file="", out_path="", sep=",", mismatch_file= "", keep_higherrank=False):
    """
    A function that reads in a list of species names and than retrieves all the relevant information from the GBIF taxonomic backbone. 
    The dataframe containing all the taxonomic data is than formatted to store the taxonKey that is used for the most general name within the column 'acceptedUsageKey'.

    Args:
        inp_file (str): The name for the file containing all the species names
        inp_path (str, optional): Path where the input file is stored. The default is empty and will be read from the working directory
        out_file (str): The name for the file to which the taxonomic information will be written to. If nothing is provided than the result will not be saved
        out_path (str, optional): Path where the output file will be stored. The default is empty and will be written to the working directory
        sep (str, optional): Separator used in the input file in between species names
        mismatch_file (str, optional): File where all the mismatched names should be written to together with the note of the name_backbone function
        keep_higherrank (bool, optional): Boolean option that allows the removal of higherrank matches in the GBIF taxonomic backbone
    Returns:
        taxonomic_df, mismatch_df (pd.Dataframe): A pair of dataframes containing all the relevant information stored in the GBIF taxonomic backbone. The first dataframe is the dataframe with valid matches whereas the second one contains all erroneous matches
    """
    #Retrieve names from the file
    species_names = read_species_names(inp_file, inp_path, sep=sep)
    #Retrieve information from the GBIF taxonomic backbone
    taxonomic_info = list(map(pygbif.species.name_backbone, species_names))
    #Convert the list of dictionaries to dataframe
    taxonomic_df = pd.DataFrame(taxonomic_info)
    taxonomic_df["lookupNames"] = species_names
    #Extract rows that are None matches and if enabled higherrank matches
    mismatch_df = None
    if keep_higherrank == True:
        #Lookup 'NONE' matches in the df and get the indices
        mismatch_indices = taxonomic_df.index[taxonomic_df['matchType'] == "NONE"].tolist()
        #Copy mismatched to separate df
        mismatch_df = taxonomic_df.iloc[mismatch_indices] 
        #Remove mismatches from original df
        taxonomic_df.drop(mismatch_indices, inplace = True)
        warning_msg = "'NONE' matches encountered while searching through the GBIF taxonomic backbone:\nThe following lookup names ("
        warning_msg += ", ".join(mismatch_df["lookupNames"].tolist())
        warning_msg += ") resulted in 'NONE' type match. Potential reasons can be found in the mismatch_df under the key 'note'"
        print(warning_msg)
    else:
        mismatch_indices = taxonomic_df.index[(taxonomic_df['matchType'] == "NONE") | (taxonomic_df['matchType'] == "HIGHERRANK")].tolist()
        mismatch_df = taxonomic_df.iloc[mismatch_indices] 
        taxonomic_df.drop(mismatch_indices, inplace = True)
        warning_msg = "'NONE' and 'HIGHERRANK' matches encountered while searching through the GBIF taxonomic backbone:\nThe following lookup names ("
        warning_msg += ", ".join(mismatch_df["lookupNames"].tolist())
        warning_msg += ") resulted in 'NONE' or 'HIGHERRANK' type match. Potential reasons can be found in the mismatch_df under the key 'note'"
        print(warning_msg)
    #Assert that usageKeys are cast as integers
    taxonomic_df["acceptedUsageKey"].fillna(taxonomic_df["usageKey"], inplace=True)
    # Assure that the keys are stored and represented as integers
    taxonomic_df["acceptedUsageKey"] = taxonomic_df["acceptedUsageKey"].astype(int)
    #If an out_file is specified than the taxonomic info will be written to a file of said name
    if out_file != "":
        taxonomic_df.to_csv(os.path.join(out_path, out_file), index=False)
    if mismatch_file != "":
        mismatch_df[["matchType", "note", "scientificName", "lookupNames"]].to_csv(os.path.join(out_path, mismatch_file), index=False)
    return taxonomic_df, mismatch_df


def extract_keys_dwc(inp_file, inp_path="", sep="\t", encoding="utf-8"):
    """
    Extract the usagekeys from a Darwin Core archive

    Args
        inp_file (str): the file containing the taxonomic information 
        inp_path (str, optional): The path towards the darwin core taxon file. Standard value is the current working directory
        sep (str, optional): Separator used in the inp_file. Standard separator used in the tab value 
        encoding (str, optional): Encoding used within the file. Standard encoding is utf-8
    Returns
        usageKeys (list<str>): A list containing the usageKeys described within the archive. These keys are multidigit strings
    """
    #Read in the DwC file immediately as a DF with a tab separator and utf8 encoding
    dwc_df = pd.read_csv(os.path.join(inp_path, inp_file), sep="\t", encoding="utf8")
    #Extract the usageKey from the taxonID field in the dataframe
    usageKeys = [key.split("/")[-1] for key in dwc_df["taxonID"]]
    return usageKeys

def bbox2polygon_wkt(bbox):
    """
    Convert a bbox to a wkt style polygon to use within the GBIF SQL query API

    Args
        bbox (tuple<float>): A tuple of floats corresponding to (longitude_min, latitude_min, longitude_max, latitude_max)
    Returns
        polygon.wkt (str): The WKT representation of the bbox in polygon format 
    """
    #Read the tuple as a rectangular geometry
    polygon = shapely.geometry.box(*bbox)
    return polygon.wkt

"""
taxon_ranks = ["species", "genus", "family", "order", "class", "phylum", "kingdom"]

class gbif_sql_query():
    def __init__(self):
        self.taxon_ranks = ["species", "genus", "family", "order", "class", "phylum", "kingdom"]

    def col_selection(self, col_names):

def generate_sql_query
"""

def generate_json_query(usageKeys, bbox, begin_year, end_year, out_file="gbif_query.json", out_path="", sendNotification='true', notificationAddress=None):
    """
    A function used in the generation of a SQL query which can be used to query data from the GBIF SQL API. The resulting GBIF query will be stored in a JSON object.

    Args
        usageKeys (list<int>): A list of keys that signify the ID's of the species of interest. These keys are retrieved from the GBIF taxonomic backbone
        bbox (tuple<float>): A tuple of floats corresponding to (longitude_min, latitude_min, longitude_max, latitude_max) 
        out_file (str, optional): The file name where the JSON query should be written to. The standard name is 'gbif_query.json'
        out_path (str, optional): The path where the file should be saved. The standard path is the working directory i.e. ''
        sendNotification (str, optional): A string containing either 'true' or 'false' to enable the GBIF to send email notifications when the download is ready
        notification (list<str>, optional): A list containing the email adresses to which notifications should be sent
    Returns
        None: The query is written to the target JSON file
    """
    wkt_str = bbox2polygon_wkt(bbox)
    json_query = {
        "sendNotification": sendNotification,
        "notificationAddresses": notificationAddress,
        "format": "SQL_TSV_ZIP"
    }
    #quotes around order keyword to avoid error with SQL, ORDER is a reserved word in SQL so we need to quote it
    sql_query = """
    SELECT 
        "year", 
        "month",
        GBIF_EEARGCode(1000, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS eeaCellCode,
        speciesKey,
        species,
        genusKey,
        genus,
        familyKey,
        family,
        orderKey,
        "order",
        classKey,
        class,
        COUNT(*) AS occurrences, 
        COUNT(DISTINCT recordedBy) AS distinctObservers
    FROM
        occurrence
    WHERE 
        GBIF_Within('{wkt}', decimalLatitude, decimalLongitude) = TRUE AND
        hasCoordinate = TRUE AND 
        occurrenceStatus = 'PRESENT' AND
        NOT ARRAY_CONTAINS(issue, 'ZERO_COORDINATE') AND 
        NOT ARRAY_CONTAINS(issue, 'COORDINATE_OUT_OF_RANGE') AND 
        NOT ARRAY_CONTAINS(issue, 'COORDINATE_INVALID') AND 
        NOT ARRAY_CONTAINS(issue, 'COUNTRY_COORDINATE_MISMATCH') AND 
        "year" IS NOT NULL AND 
        "month" IS NOT NULL AND 
        "year" >= {begin_year} AND "year" <= {end_year} AND
        taxonKey IN ({keys})
    GROUP BY
        species,
        speciesKey,
        eeaCellCode,
        "year",
        "month",
        genusKey,
        genus,
        familyKey,
        family,
        orderKey,
        "order",
        classKey,
        class
    ORDER BY 
        "year" ASC,
        "month" ASC,
        speciesKey ASC
    """.format(
        wkt=wkt_str,
        keys=",".join(map(str, usageKeys)),
        begin_year=begin_year,
        end_year=end_year
    ).strip()

    # Add to JSON query
    json_query["sql"] = sql_query

    # Write to file
    with open(os.path.join(out_path, out_file), "w") as json_file:
        json.dump(json_query, json_file)
    return None