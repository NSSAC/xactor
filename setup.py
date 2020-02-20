"""Setup."""

from setuptools import setup

package_name = "xactor"
description = "An MPI based Actor framework"

with open("README.rst", "r") as fh:
    long_description = fh.read()

classifiers = """
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: MIT License
Operating System :: POSIX :: Linux
Programming Language :: C
Programming Language :: Python :: 3.7
Topic :: Scientific/Engineering
Topic :: System :: Distributed Computing
""".strip().split("\n")

setup(
    name=package_name,
    description=description,

    author="Parantapa Bhattacharya",
    author_email="pb+pypi@parantapa.net",

    long_description=long_description,
    long_description_content_type="text/x-rst",

    packages=[package_name],

    use_scm_version=True,
    setup_requires=['setuptools_scm'],

    install_requires=[
        "click",
        "click_completion",
        "mpi4py",
    ],

    url="http://github.com/parantapa/xactor",
    classifiers=classifiers
)
