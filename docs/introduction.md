# Introduction to CloudSpeak

CloudSpeak is a Python library designed to provide abstractions that simplify working with cloud services, making them feel like native Python data structures. This project aims to bridge the gap between cloud platforms and Python developers, offering a high-level interface to interact with cloud services effortlessly.

## Supported Clouds

At the core of CloudSpeak is its support for various cloud providers. Currently, CloudSpeak supports the following cloud services:

| Cloud Service  | Features Supported        |
| ---------------|---------------------------|
| Azure Storage  | - Blob Storage            |
|                | - Queue Service           |
| Google Cloud   | (Future Support Planned)  |
| AWS S3         | (Future Support Planned)  |
| ...           | (More to come)           |

Please note that while CloudSpeak currently focuses on Azure Storage, we have plans to extend support for other cloud providers in future releases.

## Motivation

The motivation behind CloudSpeak is to simplify cloud service interactions for Python developers. Traditional cloud SDKs often come with a steep learning curve and require in-depth knowledge of cloud-specific APIs. CloudSpeak abstracts away the complexity and provides a consistent and intuitive API, allowing developers to work with cloud resources as if they were native Python data structures.

## Core Functionality

At its core, CloudSpeak offers classes and abstractions that enable seamless interaction with cloud services. For example, it provides easy-to-use classes for accessing Azure Storage services using a connection string. These classes make it simple to perform operations on blobs and queues without delving into the intricacies of Azure's SDK.

## Requirements and Installation

To get started with CloudSpeak, it can be installed using pip. Open a terminal or command prompt and run the following command:

```shell
pip install cloudspeak
```

### Azure Storage Connection String

Before you can start using CloudSpeak for Azure Storage, you'll need to create an Azure Storage account and obtain a connection string. Follow these steps to set up your Azure Storage account and retrieve the connection string:

1. **Create an Azure Storage Account**: If you don't have an Azure account, you can sign up for a free trial [here](https://azure.com/free). Once you have an Azure account, log in to the [Azure Portal](https://portal.azure.com).

2. **Create a Storage Account**: In the Azure Portal, click on "Create a resource," then search for "Storage Account" and select it. Follow the wizard to create a new storage account, specifying a unique name, region, and other settings as needed.

3. **Retrieve the Connection String**:
   - After creating the storage account, go to its "Settings" section.
   - Under the "Settings" section, select "Access keys."
   - You'll find two connection strings (primary and secondary). Either one can be used. Click the copy button next to one of them to copy the connection string to your clipboard.

Ensure that you store the Azure Storage connection string securely, as it provides access to your Azure Storage resources. You'll need this connection string to configure CloudSpeak for Azure Storage.

With Python, CloudSpeak, and the Azure Storage connection string in place, you are ready to start using CloudSpeak to interact with Azure Storage services seamlessly.


## Documentation Structure

This documentation is organized into several sections to help you get the most out of CloudSpeak:

1. [Getting Started](link-to-getting-started): A guide to quickly start using CloudSpeak.
2. [Tutorials](link-to-tutorials): Step-by-step tutorials on using CloudSpeak for common tasks.
3. [API Reference](link-to-api-reference): Detailed information about the CloudSpeak API.
4. [Examples](link-to-examples): Code examples showcasing various use cases.

Feel free to explore the documentation to make the most of CloudSpeak's capabilities and simplify your cloud-based projects.

If you have any questions or encounter issues while using CloudSpeak, please refer to the [Support](link-to-support) section for assistance.
