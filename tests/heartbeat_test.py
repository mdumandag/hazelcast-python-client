import time

from hazelcast import HazelcastClient
from hazelcast.core import Address
from tests.base import HazelcastTestCase
from hazelcast.config import ClientConfig, ClientProperties
from tests.util import configure_logging, open_connection_to_address, random_string, wait_for_partition_table


class HeartbeatTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()
        cls.rc = cls.create_rc()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.cluster = self.create_cluster(self.rc)
        self.member = self.rc.startMember(self.cluster.id)
        self.config = ClientConfig()
        self.config.cluster_name = self.cluster.id

        self.config.set_property(ClientProperties.HEARTBEAT_INTERVAL.name, 500)
        self.config.set_property(ClientProperties.HEARTBEAT_TIMEOUT.name, 2000)

        self.client = HazelcastClient(self.config)

    def tearDown(self):
        self.client.shutdown()
        self.rc.shutdownCluster(self.cluster.id)

    def test_heartbeat_stopped_and_restored(self):
        member2 = self.rc.startMember(self.cluster.id)
        addr = Address(member2.host, member2.port)
        wait_for_partition_table(self.client)
        open_connection_to_address(self.client, member2.uuid)

        def connection_collector():
            connections = []

            def collector(c, *args):
                connections.append(c)

            collector.connections = connections
            return collector

        connection_added_collector = connection_collector()
        connection_removed_collector = connection_collector()

        self.client.connection_manager.add_listener(connection_added_collector, connection_removed_collector)

        self.simulate_heartbeat_lost(self.client, addr, 2)

        def assert_heartbeat_stopped_and_restored():
            self.assertEqual(1, len(connection_added_collector.connections))
            self.assertEqual(1, len(connection_removed_collector.connections))
            stopped_connection = connection_added_collector.connections[0]
            restored_connection = connection_removed_collector.connections[0]
            self.assertEqual(stopped_connection.connected_address, Address(member2.host, member2.port))
            self.assertEqual(restored_connection.connected_address, Address(member2.host, member2.port))

        self.assertTrueEventually(assert_heartbeat_stopped_and_restored)

    @staticmethod
    def simulate_heartbeat_lost(client, address, timeout):
        connection = client.connection_manager.get_connection_from_address(address)
        connection.last_read_time -= timeout
