from cloudspeak.utils.time import now


class AzureQueueMessage:

    def __init__(self, queue, message):
        """
        Initializes an instance of AzureQueueMessage.

        Args:
            queue (AzureQueue): The AzureQueue instance to which the message belongs.
            message: The underlying message object.

        Returns:
            None
        """
        self._queue = queue
        self._message = message
        self._date = now()
        self._deleted = False

    @property
    def queue(self):
        return self._queue

    @property
    def content(self):
        """
        Gets the content of the message.

        Returns:
            str: The content of the message.

        Raises:
            None
        """
        return self._message.content

    def update(self, visibility_timeout=30):
        """
        Updates the message with a new visibility timeout.

        Args:
            visibility_timeout (int, optional): The new visibility timeout value in seconds (default is 30).

        Returns:
            None

        Raises:
            None
        """
        client = self.queue.client_raw
        self._message = client.update_message(self._message, visibility_timeout=visibility_timeout)

    @property
    def id(self):
        """
        Gets the unique identifier of the message.

        Returns:
            str: The unique identifier of the message.

        Raises:
            None
        """
        return self._message.pop_receipt

    @property
    def date(self):
        """
        Gets the date and time when the message was created (the read instance, not the message stored in the queue).

        Returns:
            datetime: The date and time when the message was created.

        Raises:
            None
        """
        return self._date

    def __len__(self):
        """
        Returns the length (number of characters) of the message content.

        Returns:
            int: The length of the message content.

        Raises:
            None
        """
        return len(self._message.content)

    def __str__(self):
        """
        Returns a string representation of the message content.

        Returns:
            str: The content of the message as a string.

        Raises:
            None
        """
        return self._message.content

    def __repr__(self):
        """
        Returns a string representation of the AzureQueueMessage.

        Returns:
            str: A string describing the AzureQueueMessage, including its queue, length, and creation date.

        Raises:
            None
        """
        deleted_str = "[DELETED]" if self._deleted else ""
        return f"{deleted_str}[{self.queue} message ({len(self)} characters; date: {self.date}]"

    def __enter__(self):
        """
        Provides support for using the AzureQueueMessage in a 'with' statement.

        Returns:
            AzureQueueMessage: The AzureQueueMessage instance.

        Raises:
            None
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Handles the exit of a 'with' statement for the AzureQueueMessage.

        Args:
            exc_type: The type of exception, if any.
            exc_value: The exception instance, if any.
            traceback: The traceback information, if any.

        Returns:
            None

        Raises:
            None
        """
        if exc_type is None:
            self.delete()

    def delete(self):
        """
        Deletes the message from the Azure Queue Storage queue.

        Returns:
            None

        Raises:
            None
        """
        if self._deleted:
            return

        client = self.queue.client_raw
        client.delete_message(self._message)
        self._deleted = True

        # We also remove it from the updates handler
        service = self.queue.service_storage
        service.updates.remove_message(self)

