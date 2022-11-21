<img style="display: block; margin: auto; width:400px; height:250px;" src="https://raw.githubusercontent.com/mlhost/cloudspeak/v0.0.1/docs/images/logo.png">

[![PyPI version](https://badge.fury.io/py/cloudspeak.svg)](https://badge.fury.io/py/cloudspeak)

# Introduction

*CloudSpeak* is a set of python tools that eases integration of cloud services in Python projects. 

It provides a set of tools that enhances communication and provides abstraction to developers so that they do 
not require to study the backend API -- this package does that for them. 

# Docs

* [Getting started](https://cloudspeak.readthedocs.io/en/v0.0.1/).
 

# Examples

* Accessing Azure Blob Storage service:

```python
from cloudspeak.api.azure import AzureService

service = AzureService(connection_string="...")
container = service['container']
file = container['path/to/blob']

# To download:
file.download()
content = file.data

# To upload:
file.data = content
file.upload(overwrite=True)
```

* Transferring with progress:
<img style="display: block; margin: auto;" src="https://raw.githubusercontent.com/mlhost/cloudspeak/v0.0.1/docs/images/jupyter_download.gif">
