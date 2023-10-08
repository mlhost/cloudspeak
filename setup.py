import sys
from setuptools import setup, setuptools

from cloudspeak import __version__


def readme():
    with open('README.md', encoding="UTF-8") as f:
        return f.read()


if sys.version_info < (3, 4, 1):
    sys.exit('Python < 3.4.1 is not supported!')


setup(
    name='cloudspeak',
    version=__version__,
    description='Set of python tools that eases integration of cloud services in Python projects',
    long_description_content_type="text/markdown",
    long_description=readme(),
    packages=setuptools.find_packages(exclude=["tests.*", "tests"]),
    url='https://github.com/mlhost/cloudspeak',
    install_requires=[
        "tqdm==4.64.1",
        "joblib==1.2.0",
        "azure-storage-blob==12.14.1",
        "azure-storage-queue==12.3.0",
        "pandas==1.5.2",
        "lz4==4.0.0",
    ],
    classifiers=[],
    test_suite='nose.collector',
    tests_require=['nose'],
    include_package_data=True,
    keywords="python cloud remote dict abstraction azure storage blob".split(" "),
    zip_safe=False,
)
