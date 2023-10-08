<img style="display: block; margin: auto; width:400px; height:250px;" src="https://raw.githubusercontent.com/mlhost/cloudspeak/v0.0.4/docs/images/logo.png">

[![PyPI version](https://badge.fury.io/py/cloudspeak.svg)](https://badge.fury.io/py/cloudspeak)
[![Documentation Status](https://readthedocs.org/projects/cloudspeak/badge/?version=latest)](https://cloudspeak.readthedocs.io/en/latest/?badge=latest)


# CloudSpeak - Azure Backend Abstractions for Python

CloudSpeak is a Python package that simplifies and abstracts the usage of Azure as a backend for specific Python data structures. It provides easy-to-use classes and methods for working with Azure Blob Storage and Azure Queue Storage, allowing you to seamlessly integrate Azure services into your Python applications.

## Features

CloudSpeak offers the following key features:

- **Azure Blob Storage Interaction:** Easily iterate through containers, create and delete them, and access individual blob files.
  
- **File Upload and Download:** Seamlessly upload and download files to and from Azure Blob Storage with built-in tracking of upload and download progress.
  
- **Azure Blob Storage Lock Management:** Efficiently manage locks offered by Azure Blob Storage to synchronize multiple processes.

- **Remote Dictionaries:** Create and manage dictionaries hosted entirely in Azure Blob Storage. You can choose between indexed dictionaries (with a maintained index for key retrieval) and non-indexed dictionaries for faster access to dictionary keys.

- **Remote Queues:** Create, send, and receive messages using a user-friendly interface, with Azure Storage as the backend.

## Getting Started

To get started with CloudSpeak, follow these simple steps:

1. Install CloudSpeak using pip:

   ```bash
   pip install cloudspeak
   ```

2. Import the necessary modules and set up Azure credentials:

   ```python
   from cloudspeak.azure import AzureCredentials
   from cloudspeak.azure import AzureFactory

   credentials = AzureCredentials()
   credentials.connection_string_storage = "YOUR_CONNECTION_STRING"

   factory = AzureFactory(credentials=credentials)
   ```

3. Use CloudSpeak to interact with Azure services. For example, to access blob storage:

   ```python
   service = factory.service_storage
   container = service.containers["example"]

   # Uploading content to a blob file named "file"
   file = container["file"]
   file.data = b"test"
   progress = file.upload()
   progress.join()

   # Downloading content from a blob file named "file"
   file = container["file"]
   progress = file.download()
   progress.join()
   downloaded_data = file.data
   ```

4. Create and use remote queues:

   ```python
   queue = factory.queue("example", create=True)
   queue.push("hello")

   message = queue.pop(wait_time=-1)

   # Automatically release the message from the queue when exiting the 'with' block.
   with message:
       print(message.content)
   ```

5. Access remote dictionaries:

   ```python
   factory = AzureFactory(credentials=credentials)

   # Create a non-indexed remote dictionary
   ad = factory.dictionary('container', 'example', create_container=False, indexed=False)

   ad["key"] = "value"
   print(ad["key"])
   ```

* Transferring with progress:
<img style="display: block; margin: auto;" src="https://raw.githubusercontent.com/mlhost/cloudspeak/v0.0.4/docs/images/jupyter_download.gif">

## Abstractions Overview

CloudSpeak provides two main abstractions:

| Abstraction           | Description                                                |
|-----------------------|------------------------------------------------------------|
| Remote Dictionaries   | Dictionaries hosted entirely in Azure Blob Storage. Choose between indexed or non-indexed versions for various use cases. |
| Remote Queues         | User-friendly interface for managing queues with Azure Storage as the backend. |

## Documentation

For detailed documentation and examples, please refer to the [CloudSpeak Documentation](https://cloudspeak.readthedocs.io/).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
