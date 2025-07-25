# Biodiversity meets Cubes
A repository aimed at the development of the cubing engine that combines biodiversity data with abiotic data to produce data cubes ready for use within the VRE's of the BmD project

## Running the code

The source code for this project is built upon the user specifying the parameters of interest within the `param.yaml` file within the `/config` directory. This yaml file will be read by the data cube constructor functions which will subsequently retrieve all relevant and desired data.

Required input data and output data will be stored within the prespecified data directory on the highest level of the directory. If the user wishes to change these paths, the `config.yaml` file within the `/config` directory needs to be modified to reflect this.