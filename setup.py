from setuptools import setup, find_packages
import more_utils

setup(
    name=more_utils.__package_name__,
    version=more_utils.__version__,
    description="Python Util package for MORE applications",
    url="https://github.ibm.com/Dublin-Research-Lab/more-utils",
    author="Dhaval Salwala",
    author_email="dhaval.vinodbhai.salwala@ibm.com",
    license="IBM",
    packages=find_packages("."),
    python_requires=">=3.8",
    install_requires=[
        "pycloudmessenger @ http://github.com/IBM/pycloudmessenger/archive/v0.7.3.tar.gz",
        "pika==0.13.0",
        "pandas>=2.0",
        "pyspark==3.3.0",
        "cassandra-driver==3.25.0",
        "logzero",
        "ipykernel",
        "PyModelarDB @ git+https://github.com/ModelarData/PyModelarDB.git"
    ],
    extras_require={
        "test": ["black", "pytest", "pytest-mock"],
    },
    zip_safe=False,
)
