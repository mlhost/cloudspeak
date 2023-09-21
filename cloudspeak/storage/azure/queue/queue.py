import time

from cloudspeak.storage.azure.queue.message import AzureQueueMessage
from cloudspeak.storage.interface.queue import Queue


class AzureQueue(Queue):
    """
    AzureQueue is a class that represents an Azure Queue Storage queue and provides methods for queue management
    and message handling.

    Args:
        service_storage (AzureService): An instance of AzureService used for Azure Queue Storage operations.
        queue_name (str): The name of the Azure Queue Storage queue.

    Properties:
        client_raw: Provides access to the raw Azure QueueClient instance.
        name (str): The name of the Azure Queue Storage queue.
    """

    def __init__(self, service_storage, queue_name):
        """
        Initializes an instance of AzureQueue.

        Args:
            service_storage (AzureService): An instance of AzureService used for Azure Queue Storage operations.
            queue_name (str): The name of the Azure Queue Storage queue.

        Returns:
            None
        """
        super().__init__(service_storage)

        self._queue_name = queue_name
        service_raw = service_storage.service_raw_queue
        self._client = service_raw.get_queue_client(queue_name)

    @property
    def client_raw(self):
        """
        Provides access to the raw Azure QueueClient instance.

        Returns:
            QueueClient: The Azure QueueClient instance.

        Raises:
            None
        """
        return self._client

    @property
    def name(self):
        """
        Gets the name of the Azure Queue Storage queue.

        Returns:
            str: The name of the queue.

        Raises:
            None
        """
        return self._queue_name

    def create(self):
        """
        Creates the Azure Queue Storage queue.

        Returns:
            None

        Raises:
            None
        """
        self._client.create_queue()
    
    def delete(self):
        """
        Deletes the Azure Queue Storage queue.

        Returns:
            None

        Raises:
            None
        """
        self._client.delete_queue()

    def push(self, messages):
        """
        Pushes one or more messages into the Azure Queue Storage queue.

        Args:
            messages (list or AzureQueueMessage): A single message or a list of messages to be pushed into the queue.

        Returns:
            None

        Raises:
            None
        """

        if type(messages) is not list:
            messages = [messages]

        for message in messages:
            self._client.send_message(message)

    def pop(self, count=1, wait_time=0, ensure_count=False, autorenew_messages=False, message_duration_seconds=30, check_interval=0.1) -> AzureQueueMessage:
        """
        Retrieves and returns one or more messages from the Azure Queue Storage queue.

        Args:
            count (int, optional): The number of messages to retrieve (default is 1).
            wait_time (float, optional): The maximum time to wait for messages to be available (default is 0).
            ensure_count (bool, optional): If True, ensures that the specified number of messages is retrieved (default is False).
            autorenew_messages (bool, optional): If True, automatically renews the messages to extend their visibility timeout (default is False).
            message_duration_seconds (int, optional): The visibility timeout for retrieved messages (default is 30 seconds).
            check_interval (float, optional): The interval at which to check for available messages (default is 0.1 seconds).

        Returns:
            AzureQueueMessage or list: The retrieved message(s) wrapped in an AzureQueueMessage instance or a list of messages.

        Raises:
            None
        """
        def elapsed(it):
            return time.time() - it

        init_time = time.time()

        messages = []

        while (elapsed(init_time) < wait_time or wait_time == -1) and ((not ensure_count and len(messages) == 0) or
               (ensure_count and len(messages) != count)):

            messages = self._client.receive_messages(number_of_messages=count,
                                                     visibility_timeout=message_duration_seconds)

            messages = [AzureQueueMessage(self, message) for message, _ in zip(messages, range(count))]
            time.sleep(check_interval)

        if autorenew_messages:
            updates_handler = self.service_storage.updates

            for m in messages:
                updates_handler.add_message(m)

        if count == 1 and len(messages) > 0:
            messages = messages[0]

        return messages

    def __len__(self):
        """
        Gets the approximate number of messages in the Azure Queue Storage queue.

        Returns:
            int: The approximate number of messages in the queue.

        Raises:
            None
        """
        return self._client.get_queue_properties().approximate_message_count

    def __str__(self):
        """
        Returns a string representation of the AzureQueue.

        Returns:
            str: A string describing the AzureQueue, including its name and associated AzureService.

        Raises:
            None
        """
        return f"[Queue '{self.name}' from {self.service_storage}]"

    def __repr__(self):
        """
        Returns a string representation of the AzureQueue.

        Returns:
            str: A string describing the AzureQueue, including its name and associated AzureService.

        Raises:
            None
        """
        return str(self)
