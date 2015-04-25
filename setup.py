#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name="sandsnake",
    license='Apache License 2.0',
    version="0.1.3",
    description="Sorted indexes backed by redis.",
    long_description=open('README.rst', 'r').read(),
    author="Numan Sachwani",
    author_email="numan856@gmail.com",
    url="https://github.com/numan/sandsnake",
    packages=find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=[
        'nydus==0.11.0',
        'redis>=2.7.2',
        'python-dateutil==1.5',
    ],
    tests_require=[
        'nose>=1.0',
    ],
    classifiers=[
        "Intended Audience :: Developers",
        'Intended Audience :: System Administrators',
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
)
