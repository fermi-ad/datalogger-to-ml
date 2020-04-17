# Data Logger to ML
Scripts for requesting AD Controls data logger data and transforming those to the desired ML output format and destination.

## ToDos

- Using https://github.com/fermilab-accelerator-ai/workflow/ as a template, use DPM to request data logger data and transform them to hdf5 files.
  - https://cdcvs.fnal.gov/redmine/projects/acsys-dpm/wiki/Python_DPM_API
  - https://portal.hdfgroup.org/display/HDF5/HDF5
- Find an optimal rate to offload data from the data logger.
- Find an optimal rate to write to the desired ML output.
- Explore No-SQL options.
