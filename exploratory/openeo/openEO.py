import openeo

connection = openeo.connect("openeo.vito.be")

#List all available collections throught the openeo
print(connection.list_collection_ids())


#Retrieve metadata information from a specific collection
print(connection.describe_collection("SENTINEL2_L2A"))