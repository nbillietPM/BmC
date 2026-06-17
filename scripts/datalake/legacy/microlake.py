import getpass
import os
from hda import Client
from bmc.cube import wekeo

# 1. Securely capture credentials
print("--- WEkEO HDA Authentication ---")
wekeo_user = input("Username (User ID): ")
wekeo_pass = getpass.getpass("Password (API Key): ")

# 2. Inject them into the environment so the Client can find them
# The 'hda' library specifically looks for these keys
os.environ['HDA_USER'] = wekeo_user
os.environ['HDA_PASSWORD'] = wekeo_pass

# 3. Initialize the Client (it will now find the credentials in the environment)
c = Client()

# 3. Proceed with the Data Lake Ingestion
wekeo_cube_engine = wekeo.wekeo_cube()

# Generate the recipe (now respecting your YAML configuration)
microlake_recipe = wekeo_cube_engine.generate_cube_recipe("../../config/level2/micro_lake.yaml")

# Trigger the ingestion pipeline
generated_cogs = wekeo_cube_engine.build_wekeo_datalake(
    recipe=microlake_recipe, 
    wekeo_client=c
)

print(f"\nIngestion complete. {len(generated_cogs)} layers added to the lake.")