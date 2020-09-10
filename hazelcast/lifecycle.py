import logging
import uuid

from hazelcast import six
from hazelcast.util import create_git_info, with_reserved_items


@with_reserved_items
class LifecycleState(object):
    """
    Lifecycle states.
    """

    STARTING = "STARTING"
    """
    The client is starting.
    """

    STARTED = "STARTED"
    """
    The client has started.
    """

    CONNECTED = "CONNECTED"
    """
    The client connected to a member.
    """

    SHUTTING_DOWN = "SHUTTING_DOWN"
    """
    The client is shutting down.
    """

    DISCONNECTED = "DISCONNECTED"
    """
    The client disconnected from a member.
    """

    SHUTDOWN = "SHUTDOWN"
    """
    The client has shutdown.
    """


class LifecycleService(object):
    """
    LifecycleService allows you to shutdown, terminate, and listen to LifecycleEvent's on HazelcastInstances.
    """
    logger = logging.getLogger("HazelcastClient.LifecycleService")

    def __init__(self, lifecycle_listeners, logger_extras):
        self.running = False
        self._listeners = {}
        self._logger_extras = logger_extras

        if lifecycle_listeners:
            for listener in lifecycle_listeners:
                self.add_listener(listener)

        self._git_info = create_git_info()

    def add_listener(self, on_state_change):
        """
        Add a listener object to listen for lifecycle events.

        :param on_state_change: (Function), function to be called when LifeCycle state is changed.
        :return: (str), id of the listener.
        """
        listener_id = str(uuid.uuid4())
        self._listeners[listener_id] = on_state_change
        return listener_id

    def remove_listener(self, registration_id):
        """
        Removes a lifecycle listener.

        :param registration_id: (str), the id of the listener to be removed.
        :return: (bool), ``true`` if the listener is removed successfully, ``false`` otherwise.
        """
        try:
            self._listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    def fire_lifecycle_event(self, new_state):
        """
        Called when instance's state changes.

        :param new_state: (Lifecycle State), the new state of the instance.
        """
        self.logger.info(self._git_info + "HazelcastClient is %s", new_state, extra=self._logger_extras)
        for on_state_change in six.itervalues(self._listeners):
            if on_state_change:
                try:
                    on_state_change(new_state)
                except:
                    self.logger.exception("Exception in lifecycle listener", extra=self._logger_extras)

    def start(self):
        if self.running:
            return

        self.fire_lifecycle_event(LifecycleState.STARTING)
        self.running = True
        self.fire_lifecycle_event(LifecycleState.STARTED)

    def shutdown(self):
        self.running = False
