# Access to the storage service

In this section, we will explore how to access the Azure Storage service using CloudSpeak's `AzureFactory`. This class provides access to Azure Storage, allowing you to perform operations such as creating containers, accessing blobs, uploading, downloading, and more. Additionally, CloudSpeak offers abstractions for remote dictionaries and queues, making it easy to work with cloud data.

## Importing and Setting Up Credentials

To begin working with Azure, you need to import the factory and provide the necessary credentials using the `AzureCredentials` class:

```python
from cloudspeak.azure import AzureCredentials, AzureFactory

# Create AzureCredentials and set the Azure Storage connection string
credentials = AzureCredentials()
credentials.connection_string_storage = CONNECTION_STRING

# Create an AzureFactory instance with the provided credentials
factory = AzureFactory(credentials=credentials)
```

## Accessing Azure Storage at a Low Level

Once you have set up the factory object, you can access Azure Storage at a low level. For example, you can create a container, access a blob, and perform various operations:

```python
# Access the Azure Storage service
service = factory.service_storage

# Access the "foo" container (create if it doesn't exist)
container = service.containers["foo"]
container.create(exists_ok=True)

# Access a blob within the container
blob = container["path/to/blob"]

# Upload data to the blob
blob.data = b"test"
progress = blob.upload()

# Download the blob data (wait for completion)
blob = container["path/to/blob"]
progress = blob.download()
progress.join()
data = blob.data
```

## Navigating Blobs in a Specific Path

Container's `get_files` method allows you to retrieve a list of files located in a specific path within your Azure Storage container. This can be useful when you want to work with a specific subset of blobs. Below are some examples of how to use this method effectively.

### Example 1: Retrieve All Files in a Path

To retrieve all files within a specified path, you can call the `get_files` method with the desired `prefix`:

```python
# Access the desired container
container = factory.service_storage.containers["your_container"]

# Specify the prefix (path) you want to navigate
prefix = "path/to/files/"

# Get a list of all files in the specified path
file_list = list(container.get_files(prefix=prefix))

# Now, 'file_list' contains references to all files within 'path/to/files/'
```

### Example 2: Retrieve Files with a Specific Delimiter

If you have used a delimiter to structure your blob storage, you can retrieve files that match a specific pattern. For example, if you want to get all files with a ".txt" extension:

```python
# Specify the prefix (path) and delimiter
prefix = "documents/"
delimiter = ".txt"

# Get a list of all files with the ".txt" extension
txt_files = list(container.get_files(prefix=prefix, delimiter=delimiter))

# 'txt_files' now contains references to all files with ".txt" extension within 'documents/' path
```

By using the `get_files` method with the appropriate `prefix` and `delimiter`, you can efficiently navigate and retrieve specific files within your Azure Storage container.

Note that listing files in this manner is a highly efficient and convenient solution for listing blobs in containers with numerous elements.

## Querying Blobs by Tags

CloudSpeak provides a powerful feature that allows you to tag your blobs with custom key-value pairs and then query those blobs based on the tags. This enables you to categorize and filter blobs based on your specific criteria. Below, we'll show you how to tag blobs and perform tag-based queries.

### Tagging a Blob

To tag a blob with key-value pairs, you can use the `tags` method on the blob object:

```python
# Access a specific blob within the container
blob = container["path/to/blob"]

# Tag the blob with a key-value pair
blob.tags({"tag_key": "tag_value"})
```

### Querying Blobs by Tags

After tagging blobs, you can query the blobs within a container based on specific tag criteria. The query allows you to filter blobs that match the specified tag conditions. Here's how you can perform a tag-based query:

```python
# Perform a tag-based query to retrieve blobs with a specific tag condition
tag_key = "tag_key"
tag_value = "tag_value"
files = list(container.query(f'"{tag_key}" = \'{tag_value}\''))

# 'files' now contains references to blobs that match the tag condition
```

In the code above, we query the container to retrieve blobs that have a tag with the key `"tag_key"` and the value `"tag_value"`. You can customize the tag conditions as needed to filter and retrieve the blobs that meet your specific criteria.

Tagging and querying blobs by tags provides a flexible and efficient way to organize and search for your data within your storage containers.

## Handling Concurrency and Locking

To handle synchronization of processes and concurrency, CloudSpeak provides the ability to lock a blob:

```python
# Lock the blob for a specified amount of time (seconds)
blob.lock()
```

A blob can be locked at the instance level (with context="instance") or at the application level (with context="app" or custom context). When locked at the application level, all applications compete for the lock, regardless of their physical location. Locks can have a specific timeout or be indefinite, lasting until the locking application is closed.

Only the application that initiated the lock can unlock the blob:

```python
# Unlock the blob
blob.unlock()
```
If multiple processes are competing for the lock, only one application can lock it, and the others will wait. When the lock is released, another application may acquire it, and the process repeats.

This provides a way to control access and ensure data consistency when multiple processes or applications are working with the same blob.

By using these features in CloudSpeak, you can effectively manage access to Azure Storage resources and handle concurrency in your cloud-based applications.
