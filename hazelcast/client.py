import logging
import logging.config
import threading

from hazelcast import six
from hazelcast.cluster import ClusterService
from hazelcast.config import _Config
from hazelcast.connection import ConnectionManager, DefaultAddressProvider
from hazelcast.core import DistributedObjectInfo
from hazelcast.invocation import InvocationService, Invocation
from hazelcast.listener import ListenerService, ClusterViewListenerService
from hazelcast.lifecycle import LifecycleService, LifecycleState
from hazelcast.partition import PartitionService
from hazelcast.protocol.codec import client_get_distributed_objects_codec
from hazelcast.proxy import ProxyManager, MAP_SERVICE, QUEUE_SERVICE, LIST_SERVICE, SET_SERVICE, MULTI_MAP_SERVICE, \
    REPLICATED_MAP_SERVICE, RINGBUFFER_SERVICE, \
    TOPIC_SERVICE, RELIABLE_TOPIC_SERVICE, \
    EXECUTOR_SERVICE, PN_COUNTER_SERVICE, FLAKE_ID_GENERATOR_SERVICE
from hazelcast.near_cache import NearCacheManager
from hazelcast.reactor import AsyncoreReactor
from hazelcast.serialization import SerializationServiceV1
from hazelcast.statistics import Statistics
from hazelcast.transaction import TWO_PHASE, TransactionManager
from hazelcast.util import AtomicInteger, DEFAULT_LOGGING, RoundRobinLB
from hazelcast.discovery import HazelcastCloudAddressProvider
from hazelcast.errors import IllegalStateError


class HazelcastClient(object):
    """
    Hazelcast Client.
    """
    _CLIENT_ID = AtomicInteger()
    logger = logging.getLogger("HazelcastClient")

    def __init__(self, cluster_members=None, cluster_name=None, **kwargs):
        kwargs.update({
            "cluster_members": cluster_members,
            "cluster_name": cluster_name
        })
        config = _Config.from_dict(kwargs)
        self.config = config
        client_id = HazelcastClient._CLIENT_ID.get_and_increment()
        self.name = self._create_client_name(client_id)
        self._init_logger()
        self._logger_extras = {
            "client_name": self.name,
            "cluster_name": config.cluster_name
        }
        self.lifecycle_service = LifecycleService(config.lifecycle_listeners, self._logger_extras)
        self.reactor = AsyncoreReactor(self._logger_extras)
        self._address_provider = self._create_address_provider()
        self.connection_manager = ConnectionManager(self, self.reactor.connection_factory, self._address_provider)
        self.cluster_service = ClusterService(self, self._logger_extras)
        self.load_balancer = self._init_load_balancer(config)
        self.partition_service = PartitionService(self._logger_extras)
        self.listener_service = ListenerService(self, self._logger_extras)
        self.invocation_service = InvocationService(self, self._logger_extras)
        self.proxy_manager = ProxyManager(self)
        self.serialization_service = SerializationServiceV1(config)
        self._transaction_manager = TransactionManager(self, self._logger_extras)
        self.lock_reference_id_generator = AtomicInteger(1)
        self.near_cache_manager = NearCacheManager(self)
        self._statistics = Statistics(self, self._logger_extras)
        self._cluster_view_listener = ClusterViewListenerService(self)
        self._shutdown_lock = threading.RLock()
        self._start()

    def _start(self):
        self.reactor.start()
        try:
            self.lifecycle_service.start()
            self.load_balancer.init(self.cluster_service)
            membership_listeners = self.config.membership_listeners
            self.cluster_service.start(membership_listeners)
            self._cluster_view_listener.start()
            self.connection_manager.start()
            if not self.config.async_start:
                self.cluster_service.wait_initial_member_list_fetched()
                self.connection_manager.connect_to_all_cluster_members()

            self.listener_service.start()
            self._statistics.start()
        except:
            self.shutdown()
            raise
        self.logger.info("Client started.", extra=self._logger_extras)

    def get_executor(self, name):
        """
        Creates cluster-wide :class:`~hazelcast.proxy.executor.Executor`.

        :param name: (str), name of the Executor proxy.
        :return: (:class:`~hazelcast.proxy.executor.Executor`), Executor proxy for the given name.
        """
        return self.proxy_manager.get_or_create(EXECUTOR_SERVICE, name)

    def get_flake_id_generator(self, name):
        """
        Creates or returns a cluster-wide :class:`~hazelcast.proxy.flake_id_generator.FlakeIdGenerator`.

        :param name: (str), name of the FlakeIdGenerator proxy.
        :return: (:class:`~hazelcast.proxy.flake_id_generator.FlakeIdGenerator`), FlakeIdGenerator proxy for the given name
        """
        return self.proxy_manager.get_or_create(FLAKE_ID_GENERATOR_SERVICE, name)

    def get_queue(self, name):
        """
        Returns the distributed queue instance with the specified name.

        :param name: (str), name of the distributed queue.
        :return: (:class:`~hazelcast.proxy.queue.Queue`), distributed queue instance with the specified name.
        """
        return self.proxy_manager.get_or_create(QUEUE_SERVICE, name)

    def get_list(self, name):
        """
        Returns the distributed list instance with the specified name.

        :param name: (str), name of the distributed list.
        :return: (:class:`~hazelcast.proxy.list.List`), distributed list instance with the specified name.
        """
        return self.proxy_manager.get_or_create(LIST_SERVICE, name)

    def get_map(self, name):
        """
        Returns the distributed map instance with the specified name.

        :param name: (str), name of the distributed map.
        :return: (:class:`~hazelcast.proxy.map.Map`), distributed map instance with the specified name.
        """
        return self.proxy_manager.get_or_create(MAP_SERVICE, name)

    def get_multi_map(self, name):
        """
        Returns the distributed MultiMap instance with the specified name.

        :param name: (str), name of the distributed MultiMap.
        :return: (:class:`~hazelcast.proxy.multi_map.MultiMap`), distributed MultiMap instance with the specified name.
        """
        return self.proxy_manager.get_or_create(MULTI_MAP_SERVICE, name)

    def get_pn_counter(self, name):
        """
        Returns the PN Counter instance with the specified name.

        :param name: (str), name of the PN Counter.
        :return: (:class:`~hazelcast.proxy.pn_counter.PNCounter`), the PN Counter.
        """
        return self.proxy_manager.get_or_create(PN_COUNTER_SERVICE, name)

    def get_reliable_topic(self, name):
        """
        Returns the :class:`~hazelcast.proxy.reliable_topic.ReliableTopic` instance with the specified name.

        :param name: (str), name of the ReliableTopic.
        :return: (:class:`~hazelcast.proxy.reliable_topic.ReliableTopic`), the ReliableTopic.
        """
        return self.proxy_manager.get_or_create(RELIABLE_TOPIC_SERVICE, name)

    def get_replicated_map(self, name):
        """
        Returns the distributed ReplicatedMap instance with the specified name.

        :param name: (str), name of the distributed ReplicatedMap.
        :return: (:class:`~hazelcast.proxy.replicated_map.ReplicatedMap`), distributed ReplicatedMap instance with the specified name.
        """
        return self.proxy_manager.get_or_create(REPLICATED_MAP_SERVICE, name)

    def get_ringbuffer(self, name):
        """
        Returns the distributed RingBuffer instance with the specified name.

        :param name: (str), name of the distributed RingBuffer.
        :return: (:class:`~hazelcast.proxy.ringbuffer.RingBuffer`), distributed RingBuffer instance with the specified name.
        """

        return self.proxy_manager.get_or_create(RINGBUFFER_SERVICE, name)

    def get_set(self, name):
        """
        Returns the distributed Set instance with the specified name.

        :param name: (str), name of the distributed Set.
        :return: (:class:`~hazelcast.proxy.set.Set`), distributed Set instance with the specified name.
        """
        return self.proxy_manager.get_or_create(SET_SERVICE, name)

    def get_topic(self, name):
        """
        Returns the :class:`~hazelcast.proxy.topic.Topic` instance with the specified name.

        :param name: (str), name of the Topic.
        :return: (:class:`~hazelcast.proxy.topic.Topic`), the Topic.
        """
        return self.proxy_manager.get_or_create(TOPIC_SERVICE, name)

    def new_transaction(self, timeout=120, durability=1, type=TWO_PHASE):
        """
        Creates a new :class:`~hazelcast.transaction.Transaction` associated with the current thread using default or given options.

        :param timeout: (long), the timeout in seconds determines the maximum lifespan of a transaction. So if a
            transaction is configured with a timeout of 2 minutes, then it will automatically rollback if it hasn't
            committed yet.
        :param durability: (int), the durability is the number of machines that can take over if a member fails during a
        transaction commit or rollback
        :param type: (Transaction Type), the transaction type which can be :const:`~hazelcast.transaction.TWO_PHASE` or :const:`~hazelcast.transaction.ONE_PHASE`
        :return: (:class:`~hazelcast.transaction.Transaction`), new Transaction associated with the current thread.
        """
        return self._transaction_manager.new_transaction(timeout, durability, type)

    def add_distributed_object_listener(self, listener_func):
        """
        Adds a listener which will be notified when a
        new distributed object is created or destroyed.
        :param listener_func: Function to be called when a distributed object is created or destroyed.
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        return self.proxy_manager.add_distributed_object_listener(listener_func)

    def remove_distributed_object_listener(self, registration_id):
        """
        Removes the specified distributed object listener. Returns silently if there is no such listener added before.
        :param registration_id: (str), id of registered listener.
        :return: (bool), ``true`` if registration is removed, ``false`` otherwise.
        """
        return self.proxy_manager.remove_distributed_object_listener(registration_id)

    def get_distributed_objects(self):
        """
        Returns all distributed objects such as; queue, map, set, list, topic, lock, multimap.
        Also, as a side effect, it clears the local instances of the destroyed proxies.
        :return:(Sequence), List of instances created by Hazelcast.
        """
        request = client_get_distributed_objects_codec.encode_request()
        invocation = Invocation(request, response_handler=lambda m: m)
        self.invocation_service.invoke(invocation)
        response = client_get_distributed_objects_codec.decode_response(invocation.future.result())

        distributed_objects = self.proxy_manager.get_distributed_objects()
        local_distributed_object_infos = set()
        for dist_obj in distributed_objects:
            local_distributed_object_infos.add(DistributedObjectInfo(dist_obj.service_name, dist_obj.name))

        for dist_obj_info in response:
            local_distributed_object_infos.discard(dist_obj_info)
            self.proxy_manager.get_or_create(dist_obj_info.service_name, dist_obj_info.name, create_on_remote=False)

        for dist_obj_info in local_distributed_object_infos:
            self.proxy_manager.destroy_proxy(dist_obj_info.service_name, dist_obj_info.name, destroy_on_remote=False)

        return self.proxy_manager.get_distributed_objects()

    def shutdown(self):
        """
        Shuts down this HazelcastClient.
        """
        with self._shutdown_lock:
            if self.lifecycle_service.running:
                self.lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTTING_DOWN)
                self.lifecycle_service.shutdown()
                self.near_cache_manager.destroy_near_caches()
                self.connection_manager.shutdown()
                self.invocation_service.shutdown()
                self._statistics.shutdown()
                self.reactor.shutdown()
                self.lifecycle_service.fire_lifecycle_event(LifecycleState.SHUTDOWN)

    def _create_address_provider(self):
        config = self.config
        cluster_members = config.cluster_members
        address_list_provided = len(cluster_members) > 0
        cloud_discovery_token = config.cloud_discovery_token
        cloud_enabled = cloud_discovery_token is not None
        if address_list_provided and cloud_enabled:
            raise IllegalStateError("Only one discovery method can be enabled at a time. "
                                    "Cluster members given explicitly: %s, Hazelcast Cloud enabled: %s"
                                    % (address_list_provided, cloud_enabled))

        if cloud_enabled:
            connection_timeout = self._get_connection_timeout(config)
            return HazelcastCloudAddressProvider(cloud_discovery_token, connection_timeout, self._logger_extras)

        return DefaultAddressProvider(cluster_members)

    def _init_logger(self):
        config = self.config
        logging_config = config.logging_config
        if logging_config:
            logging.config.dictConfig(logging_config)
        else:
            logging.config.dictConfig(DEFAULT_LOGGING)
            self.logger.setLevel(config.logging_level)

    def _create_client_name(self, client_id):
        client_name = self.config.client_name
        if client_name:
            return client_name
        return "hz.client_%s" % client_id

    @staticmethod
    def _get_connection_timeout(config):
        timeout = config.connection_timeout
        return six.MAXSIZE if timeout == 0 else timeout

    @staticmethod
    def _init_load_balancer(config):
        load_balancer = config.load_balancer
        if not load_balancer:
            load_balancer = RoundRobinLB()
        return load_balancer
