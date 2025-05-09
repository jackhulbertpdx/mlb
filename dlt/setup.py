#!/usr/bin/env python
"""Setup script for MLB Roster Pipeline."""

from setuptools import setup, find_packages

setup(
    name="mlb-roster-pipeline",
    version="1.0.0",
    description="Pipeline to fetch MLB team rosters and load into databases using dlt",
    author="Jack Hulbert ",
    author_email="jackhulbertpdx@gmail.com",
    packages=find_packages(),
    install_requires=[
        "requests>=2.28.1",
        "pandas>=1.5.0",
        "dlt>=0.3.0",
    ],
    python_requires=">=3.7",
)