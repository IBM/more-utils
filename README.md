# MoreUtils

MoreUtils is a Python package to support MORE applications. With moreutils, you can retrieve de-compressed time series / data models from [**ModelarDB**](https://github.com/ModelarData/ModelarDB), save and load models to the cloud object storage, upload timeseries forecast and more.

## Current State:

The release tag v1.0.0 is the first integrated version that works with other MORE applications.

## Installation

It is strongly recommended that a virtual environment (like venv or conda) is created to run MoreUtils to avoid dependency conflicts with already
installed packages.

### Python Version

The package is developed using a python version of atleast 3.8.

### Run ModelarDB instance

1. Clone ModelarDB repository by running

```shell
git clone https://github.com/ModelarData/ModelarDB.git
```

2. Follow the installation steps in the Readme (`ModelarDB/README.md`). It is recommended to use `modelardb.interface` as arrow.

3. Start the instance and load your dataset into the ModelarDB.

**Note:** The sample configuration file is present at `ModelarDB/modelardb`.conf

### To use MoreUtils as a python package in your library

MoreUtils can be installed via pip. The github repo can be cloned to the local machine and in the root directory where setup.py is located, the following command needs to be run.

```shell
pip install .
```

This command will install MoreUtils along with its dependencies.

### To install MoreUtils for development purpose, run following

```shell
pip install -e .
```

**Note:** The -e flag specifies the editable mode. Packages install in editable mode do not need to re-install before the changes come into effect. This is useful if you are working on a moreutils package.

## Testing

Unit tests are contained in the [**tests**](https://github.com/IBM/more-utils/tree/main/tests) directory.

## Usage

A series of [**examples**](https://github.ibm.com/Dublin-Research-Lab/more-utils/tree/main/examples) in the repository shows how to use various functions of MoreUtils.

## Acknowledgment

This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 957345 for MORE project.
