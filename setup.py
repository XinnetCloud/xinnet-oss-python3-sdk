#!/usr/bin/env python
#-*- coding:utf-8 -*-
 
from setuptools import setup, find_packages
 
setup(
    name = "xinnet",
    version = "1.0",
    description='Xinnet OSS (Object Storage Service) SDK',
    install_requires=['requests>=2.6.0'],
    packages = find_packages(),
)
