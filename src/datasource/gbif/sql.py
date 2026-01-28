from concurrent.futures import ThreadPoolExecutor, as_completed
import pygbif
import pandas as pd
import os
import itertools
from functools import lru_cache
import shapely
import json
from utils import credentials
import time 

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

#Decorator to allow the function to cache its result
@lru_cache(maxsize=None)
def lookup_backbone_single(name: str):
    """Cached single-name GBIF backbone lookup."""
    return pygbif.species.name_backbone(name)

def lookup_backbone(name_list, max_workers=8):
    """Parallel backbone lookup with caching."""
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        return list(ex.map(lookup_backbone_single, name_list))
        
def fetch_taxon_info(inp_file, 
                     inp_path="", 
                     out_file="", out_path="", 
                     sep=",", 
                     mismatch_file= "", 
                     keep_higherrank=False,
                     keep_fuzzy = False,
                     max_workers = 8):
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
    taxonomic_info = lookup_backbone(species_names, max_workers=max_workers)
    #Convert the list of dictionaries to dataframe
    taxonomic_df = pd.DataFrame(taxonomic_info)
    taxonomic_df["lookupNames"] = species_names
    #Extract rows that are None matches and if enabled higherrank matches
    mismatchTypes = ["NONE",
                     *(["FUZZY"] if not keep_fuzzy else []),
                     *(["HIGHERRANK"] if not keep_higherrank else [])]
    mismatch_indices = taxonomic_df["matchType"].isin(mismatchTypes)
    if set(mismatch_indices)=={False, True}:
        print(f"Mismatches encountered of type {set(taxonomic_df['matchType'])}"
              f"{''.join(f'\nCount {mt}: {sum(taxonomic_df['matchType'] == mt)}' for mt in set(taxonomic_df['matchType']))}")
    mismatch_df = taxonomic_df[mismatch_indices]
    #Invert mismatch indices to extract correct matches
    taxonomic_df = taxonomic_df[~mismatch_indices]
    #Assert that usageKeys are cast as integers
    taxonomic_df["acceptedUsageKey"] = taxonomic_df["acceptedUsageKey"].fillna(taxonomic_df["usageKey"])
    # Assure that the keys are stored and represented as integers
    taxonomic_df["acceptedUsageKey"] = taxonomic_df["acceptedUsageKey"].astype(int)
    #If an out_file is specified than the taxonomic info will be written to a file of said name
    if out_file != "":
        taxonomic_df.to_csv(os.path.join(out_path, out_file), index=False)
    if mismatch_file != "":
        mismatch_df[["matchType", "note", "scientificName", "lookupNames"]].to_csv(os.path.join(out_path, mismatch_file), index=False)
    return taxonomic_df, mismatch_df

GBIF_COLUMNS = [
    "gbifid", "accessrights", "bibliographiccitation", "language", "license",
    "modified", "publisher", "references", "rightsholder", "type",
    "institutionid", "collectionid", "datasetid", "institutioncode", "collectioncode",
    "datasetname", "ownerinstitutioncode", "basisofrecord", "informationwithheld",
    "datageneralizations", "dynamicproperties", "occurrenceid", "catalognumber",
    "recordnumber", "recordedby", "recordedbyid", "individualcount",
    "organismquantity", "organismquantitytype", "sex", "lifestage",
    "reproductivecondition", "caste", "behavior", "vitality",
    "establishmentmeans", "degreeofestablishment", "pathway",
    "georeferenceverificationstatus", "occurrencestatus", "preparations",
    "disposition", "associatedoccurrences", "associatedreferences",
    "associatedsequences", "associatedtaxa", "othercatalognumbers",
    "occurrenceremarks", "organismid", "organismname", "organismscope",
    "associatedorganisms", "previousidentifications", "organismremarks",
    "materialentityid", "materialentityremarks", "verbatimlabel",
    "materialsampleid", "eventid", "parenteventid", "eventtype",
    "fieldnumber", "eventdate", "eventtime", "startdayofyear", "enddayofyear",
    "year", "month", "day", "verbatimeventdate", "habitat", "samplingprotocol",
    "samplesizevalue", "samplesizeunit", "samplingeffort", "fieldnotes",
    "eventremarks", "locationid", "highergeographyid", "highergeography",
    "continent", "waterbody", "islandgroup", "island", "countrycode",
    "stateprovince", "county", "municipality", "locality", "verbatimlocality",
    "verbatimelevation", "verticaldatum", "verbatimdepth",
    "minimumdistanceabovesurfaceinmeters", "maximumdistanceabovesurfaceinmeters",
    "locationaccordingto", "locationremarks", "decimallatitude",
    "decimallongitude", "coordinateuncertaintyinmeters", "coordinateprecision",
    "pointradiusspatialfit", "verbatimcoordinatesystem", "verbatimsrs",
    "footprintwkt", "footprintsrs", "footprintspatialfit", "georeferencedby",
    "georeferenceddate", "georeferenceprotocol", "georeferencesources",
    "georeferenceremarks", "geologicalcontextid", "earliesteonorlowesteonothem",
    "latesteonorhighesteonothem", "earliesteraorlowesterathem",
    "latesteraorhighesterathem", "earliestperiodorlowestsystem",
    "latestperiodorhighestsystem", "earliestepochorlowestseries",
    "latestepochorhighestseries", "earliestageorloweststage",
    "latestageorhigheststage", "lowestbiostratigraphiczone",
    "highestbiostratigraphiczone", "lithostratigraphicterms", "group",
    "formation", "member", "bed", "identificationid", "verbatimidentification",
    "identificationqualifier", "typestatus", "identifiedby", "identifiedbyid",
    "dateidentified", "identificationreferences", "identificationverificationstatus",
    "identificationremarks", "taxonid", "scientificnameid", "acceptednameusageid",
    "parentnameusageid", "originalnameusageid", "nameaccordingtoid",
    "namepublishedinid", "taxonconceptid", "scientificname",
    "acceptednameusage", "parentnameusage", "originalnameusage",
    "nameaccordingto", "namepublishedin", "namepublishedinyear",
    "higherclassification", "kingdom", "phylum", "class", "order", "superfamily",
    "family", "subfamily", "tribe", "subtribe", "genus", "genericname",
    "subgenus", "infragenericepithet", "specificepithet",
    "infraspecificepithet", "cultivarepithet", "taxonrank",
    "verbatimtaxonrank", "vernacularname", "nomenclaturalcode",
    "taxonomicstatus", "nomenclaturalstatus", "taxonremarks", "datasetKey",
    "publishingcountry", "lastinterpreted", "elevation", "elevationaccuracy",
    "depth", "depthaccuracy", "distancefromcentroidinmeters", "issue",
    "taxonomicissue", "nontaxonomicissue", "mediatype", "hascoordinate",
    "hasgeospatialissues", "taxonKey", "acceptedtaxonKey", "kingdomKey",
    "phylumKey", "classKey", "orderKey", "familyKey", "genusKey",
    "subgenusKey", "speciesKey", "species", "acceptedscientificname",
    "typifiedname", "protocol", "lastparsed", "lastcrawled", "isinvasive",
    "repatriated", "relativeorganismquantity", "projectid", "issequenced",
    "gbifregion", "publishedbygbifregion", "level0gid", "level0name",
    "level1gid", "level1name", "level2gid", "level2name", "level3gid",
    "level3name", "iucnredlistcategory", "publishingorgKey", "installationKey",
    "institutionKey", "collectionKey", "programmeacronym",
    "hostingorganizationKey", "isincluster", "dwcaextension", "eventdategte",
    "eventdatelte"
]

GBIF_GRIDS = ["EEA", "EQDG", "DMSG", "ISEA3H", "MGRS"]
GBIF_GRID_FUNCTIONS = ["GBIF_EEARGCode", "GBIF_EQDGCode", "GBIF_ISEA3HCode","GBIF_DMSGCode", "GBIF_MGRSCode"]
GBIF_GRID_RESOLUTIONS = [[25, 100, 250, 1000, 10000, 50000,100000],
                    list(range(0,8)),
                    [3600, 1800, 900, 600, 300, 150, 60, 30],
                    list(range(1,23)),
                    [0, 1, 10, 100, 1000, 10000, 100000]]

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

def generate_query(taxonKeys, columns, record_type, wkt_polygon,
                   year_range=None,
                   aggregate=False, include_distinct_observers = True,
                   grid=False, grid_resolution=None, coordinateUncertainty=1000,
                   includeUnknownStatus=True,
                   include_uncertainty=True,
                   issue_flags=["hasCoordinate = TRUE", 
                                "NOT ARRAY_CONTAINS(issue, 'ZERO_COORDINATE')",
                                "NOT ARRAY_CONTAINS(issue, 'COORDINATE_OUT_OF_RANGE')",
                                "NOT ARRAY_CONTAINS(issue, 'COORDINATE_INVALID')",
                                "NOT ARRAY_CONTAINS(issue, 'COUNTRY_COORDINATE_MISMATCH')"]):
    #----VALIDITY CHECK----
    #check if all the selected columns are valid columns that can be selected from the occurrence table
    if not set(columns).issubset(GBIF_COLUMNS):
        raise ValueError(f"The following column(s) ({set(columns)-set(GBIF_COLUMNS)}) are not present in the GBIF data table") 
    #check if the requested record type(s) is(are) valid
    if record_type.lower() not in ["occurrence", "absence", "mixed"]:
        raise ValueError(f"Chosen record type {record_type} is invalid. Please choose either 'occurrence' or 'absence'")
    #Some columns require that we use quotes in order to use due to conflict with reserved Keywords and functions in sql
    reserved_columns = ["group", "order", "type", "references", "class", "language", "year", "month", "day"]
    #Format the column names so they are usable in the query
    quoted_columns = [f'"{col}"' if col in reserved_columns else col for col in columns]

    #----ISSUES AND CONDITIONS----
    #--TIME--
    time_columns = " AND ".join([f'"{col}" IS NOT NULL' for col in columns if col in ["year", "month", "day"]])
    #If a year range is given check if it formatted in a valid way
    if year_range:
        if year_range[0]>year_range[1]:
            raise ValueError(f"Year range invalid (start_year > end_year). Please give a list [start_year, end_year] where (start_year < end_year)")
        year_range_str = f'"year" >= {year_range[0]} AND "year" <= {year_range[1]} AND'
    else:
        year_range_str = ""
    #--ISSUES--
    #Join the issue flags together 
    if issue_flags:
        issue_str = " AND ".join(issue_flags)
    else:
        issue_str = ""
    #--OCCURRENCE STATUS--
    status_map = {"occurrence": ["'PRESENT'"],
                  "absence": ["'ABSENT'"],
                  "mixed": ["'PRESENT'", "'ABSENT'"]}
    status_values = status_map[record_type.lower()].copy()
    if includeUnknownStatus:
        status_values.append("'UNKNOWN'")
    status_clause = "occurrenceStatus IN ({})".format(", ".join(status_values))
    status_str_map = {"occurrence": "occurrences",
                      "absence": "absences",
                      "mixed": "frequency"}
    #----GRID SECTION----
    if grid:
        #Check if grid is valid
        if grid.upper() not in GBIF_GRIDS:
            raise ValueError(f"The specified grid '{grid}' is not a supported grid. Please choose one of the following {GBIF_GRIDS}")
        #Extract the corresponding idx
        grid_idx = GBIF_GRIDS.index(grid.upper())
        #Check if resolution is valid
        if grid_resolution not in GBIF_GRID_RESOLUTIONS[grid_idx]:
            raise ValueError(f"The specified resolution '{grid_resolution}' is not a valid option for the selected grid. Please use one of the following option ({GBIF_GRID_RESOLUTIONS[grid_idx]})")
        #Construct gridding function string 
        gridding_str = f"""{GBIF_GRID_FUNCTIONS[grid_idx]}({grid_resolution}, 
                           decimalLatitude, 
                           decimalLongitude, 
                           COALESCE(coordinateUncertaintyInMeters, {coordinateUncertainty})) AS {grid.lower()}CellCode"""

    #----AGGREGATION----
    #Return aggregated records based on the selected columns, grid cell code and occurrencestatus
    if aggregate:
        #Group based on the columns that where requested, grid cell code and occurrence status
        group_statement = f'GROUP BY {",".join(quoted_columns)}{","+grid.lower()+"CellCode" if grid else ""}, occurrenceStatus'
        aggr_statement = f'COUNT(*) AS {status_str_map[record_type]}'
    distinct_obs_clause = ""
    if include_distinct_observers and aggregate:
        distinct_obs_clause = ", COUNT(DISTINCT recordedBy) as distinctObservers"
    select_uncertainty = include_uncertainty and not aggregate
    uncertainty_string = f", COALESCE(coordinateUncertaintyInMeters, {coordinateUncertainty}) as coordinateUncertaintyInMeters"
    #----SQL QUERY BUILD----
    select_statement = f'''SELECT {",".join(quoted_columns)}
                           {",decimalLatitude, decimalLongitude" if not aggregate else ""}{uncertainty_string if select_uncertainty else ""}
                           {","+aggr_statement if aggregate else ""}
                           {distinct_obs_clause}
                           {"," +gridding_str if grid else ""} FROM occurrence'''
    #Filter conditions for the records
    filter_statement = f"""WHERE GBIF_Within('{wkt_polygon}', decimalLatitude, decimalLongitude) = TRUE
                           AND {time_columns} 
                           AND {year_range_str} {issue_str} 
                           AND taxonKey IN ({','.join(map(str, taxonKeys))}) 
                           AND {status_clause}"""
    return f"{select_statement} {filter_statement} {group_statement if aggregate else ""}"

def download_query(gbif_query,
                   target_dir= "",
                   max_time=3600,
                   sleep_time= 30):
    """
    Submit a GBIF SQL download request, poll for completion, and save the ZIP file.
    Automatically creates the target directory if it does not exist.
    """
    # ---- Directory existence check and auto-create --------------------------
    if target_dir:
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as e:
            raise RuntimeError(
                f"Could not create target directory '{target_dir}': {e}"
            )
    else:
        target_dir = "."

    # ---- Credentials --------------------------------------------------------
    creds = credentials.verify_gbif_credentials()

    # ---- Submit GBIF download ----------------------------------------------
    try:
        download_key = pygbif.occurrences.download_sql(
            gbif_query,
            user=creds["GBIF_USER"],
            pwd=creds["GBIF_PWD"]
        )
    except Exception as e:
        raise RuntimeError(f"Failed to submit GBIF download: {e}")

    print(f"GBIF download key: {download_key}")

    # ---- Poll download status ----------------------------------------------
    start = time.time()
    while True:
        try:
            metadata = pygbif.occurrences.download_meta(download_key)
        except Exception as e:
            print(f"Warning: metadata fetch failed temporarily: {e}")
            metadata = {"status": "UNKNOWN"}

        status = metadata.get("status", "UNKNOWN")

        if status == "SUCCEEDED":
            print("Download succeeded")
            break
        elif status in ("FAILED", "KILLED"):
            raise RuntimeError(f"GBIF download failed with status: {status}")

        if time.time() - start >= max_time:
            raise TimeoutError(
                f"GBIF download did not finish within {max_time} seconds. "
                f"Last status: {status}"
            )

        time.sleep(sleep_time)

    # ---- Download file to target directory ---------------------------------
    try:
        filepath = pygbif.occurrences.download_get(download_key, path=target_dir)
    except Exception as e:
        raise RuntimeError(f"Failed to download GBIF file: {e}")
    return filepath