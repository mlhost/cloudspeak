# Abstraction of Remote Dictionaries

Remote dictionaries in CloudSpeak are classes that mimic Python dictionaries but have their content hosted entirely in the cloud, specifically in Azure Storage. There are two types of remote dictionaries: indexed and non-indexed. Each type has its advantages and disadvantages, which are listed below:

## Indexed Dictionary

An indexed dictionary has a special index file that contains key-value pairs associating dictionary keys with blob paths. It offers quick listing of existing keys but comes with penalties as it grows larger. Multiple writes require index synchronization, which can impact performance in scenarios with many actors. Indexed dictionaries are ideal when you need to know the keys beforehand and have many reads and no concurrent writes.

## Non-Indexed Dictionary

A non-indexed dictionary doesn't have an index file, making it hard to determine the number of keys it contains. Iteration is more complex in this type. However, both read and write access to a key is direct and unaffected by the number of keys or actors acting on the dictionary. Key names are subject to Azure Storage naming restrictions, and keys referencing directories (ending with "/") generate new remote dictionaries pointing to that directory, allowing interaction as if it were a tree.

## Examples

### Creating an Indexed Dictionary

To create an indexed dictionary, use the following code:

```python
# Create an indexed dictionary
ad = factory.dictionary('container', 'example', create_container=False, indexed=True)
```

### Creating a Non-Indexed Dictionary

To create a non-indexed dictionary, use the following code:

```python
# Create a non-indexed dictionary
ad = factory.dictionary('container', 'example', create_container=False, indexed=False)
```

### Adding Entries to a Dictionary

You can add entries to a dictionary like this:

```python
ad['key'] = 4
ad['key2'] = b'binary'
ad['key3'] = pd.DataFrame({'foo': ["bar"]})
```

### Accessing Dictionary Entries

You can access dictionary entries as follows:

```python
print(ad['key'])
```

### Locking and Unlocking Dictionary Entries

Remote dictionaries inherit the locking system defined in the low-level core, allowing you to lock and unlock dictionary entries:

```python
# Lock an entry (freezing all processes attempting to lock it)
ad['key'].lock()

# Once a given key is locked, only the process that locked the key can alter its content

# Unlock the entry (allowing another process to use it)
ad['key'].unlock()
```

By using remote dictionaries in CloudSpeak, you can efficiently work with key-value data stored in Azure Storage, whether you need an indexed or non-indexed approach.
