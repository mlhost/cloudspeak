# Remote Queues Abstraction

Remote queues in CloudSpeak are classes that behave like Python queues but are hosted in the cloud. You can create, send, and receive messages from these queues easily.

## Creating a Remote Queue

To create a remote queue, use the following code:

```python
queue = factory.queue(queue_name="example")  # "example" is the name of the queue
```

## Sending Messages

You can send messages to the queue using the `push` method. You can send a single message or multiple messages as follows:

```python
# Send a single message
queue.push("message")

# Send multiple messages
queue.push(["message1", "message2"])
```

## Receiving Messages

To retrieve messages from the queue, you can use the `pop` method. You can specify the number of messages to retrieve, the wait time for receiving messages, and the duration for which the message is considered valid:

```python
# Retrieve a single message (blocking indefinitely)
message1 = queue.pop(count=1, wait_time=-1, message_duration_seconds=30)

# Retrieve another single message (blocking indefinitely)
message2 = queue.pop(count=1, wait_time=-1, message_duration_seconds=30)
```

If `count` is set to a value greater than 1, the method will attempt to retrieve as many messages as specified in the `count` parameter at once.

## Accessing Message Content

You can access the content of a message by using a context manager to work with the message:

```python
with message1:
    content = message1.content
```

When you exit the context manager, the message is automatically removed from the queue. Alternatively, you can manually delete the message:

```python
content = message1.content
message1.delete()
```

## Message Duration and Renewal

Each message has a specified duration for which it is considered valid (default is 30 seconds). If the processing of a message takes longer than the specified duration, you can renew it using the `update` method to extend its validity:

```python
# Renew the message's validity for another 30 seconds (or the specified duration)
message1.update(message_duration_seconds=30)
```

## Timestamp of Message Receipt

All messages include a timestamp indicating when they were received. You can access this timestamp using `message.date`.

Using remote queues in CloudSpeak, you can efficiently manage message-based communication in your cloud-based applications.
