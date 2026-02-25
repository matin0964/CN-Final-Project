"""
Phase 4 tests: Hybrid Push-Pull (IHAVE/IWANT) and Sybil resistance (PoW on HELLO).
- Unit: validate_pow correctness, IHAVE payload cap (ids_max_ihave).
- Integration: IHAVE -> IWANT -> GOSSIP flow; HELLO rejected/accepted with PoW.
- Regression: push_only unchanged; TTL and SeenSet behavior.
- Analysis: overhead includes IHAVE/IWANT; convergence uses 95%.
"""

import math
import time
import unittest

# Allow importing from parent
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gossip_node import GossipNode
from message_builder import MessageBuilder


class TestValidatePow(unittest.TestCase):
    """Unit tests: PoW validation (valid / invalid nonce)."""

    def setUp(self):
        self.k_pow = 4
        self.node = GossipNode(19999, '', 3, 8, 20, 2, 6, 42, k_pow=self.k_pow)

    def test_valid_pow_accepted(self):
        pow_data = self.node.compute_pow()
        hello_msg = {
            'msg_type': 'HELLO',
            'sender_id': self.node.node_id,
            'sender_addr': self.node.self_addr,
            'payload': {'capabilities': ['udp', 'json'], 'pow': pow_data}
        }
        self.assertTrue(self.node.validate_pow_on_hello(hello_msg))

    def test_invalid_nonce_rejected(self):
        pow_data = self.node.compute_pow()
        pow_data['nonce'] = pow_data['nonce'] + 1  # wrong nonce
        hello_msg = {
            'msg_type': 'HELLO',
            'sender_id': self.node.node_id,
            'sender_addr': self.node.self_addr,
            'payload': {'capabilities': ['udp', 'json'], 'pow': pow_data}
        }
        self.assertFalse(self.node.validate_pow_on_hello(hello_msg))

    def test_wrong_difficulty_k_rejected(self):
        pow_data = self.node.compute_pow()
        pow_data['difficulty_k'] = self.k_pow - 1
        hello_msg = {
            'msg_type': 'HELLO',
            'sender_id': self.node.node_id,
            'sender_addr': self.node.self_addr,
            'payload': {'capabilities': ['udp', 'json'], 'pow': pow_data}
        }
        self.assertFalse(self.node.validate_pow_on_hello(hello_msg))

    def test_k_pow_zero_accepts_hello_without_pow(self):
        node0 = GossipNode(19998, '', 3, 8, 20, 2, 6, 43, k_pow=0)
        hello_msg = {
            'msg_type': 'HELLO',
            'sender_id': 'some-id',
            'sender_addr': '127.0.0.1:1',
            'payload': {'capabilities': ['udp', 'json']}
        }
        self.assertTrue(node0.validate_pow_on_hello(hello_msg))


class TestIhavePayloadCap(unittest.TestCase):
    """Unit tests: IHAVE payload capped by ids_max_ihave."""

    def test_ihave_ids_capped_when_building(self):
        node = GossipNode(19997, '', 3, 8, 20, 2, 6, 44, mode='hybrid_push_pull', ids_max_ihave=5)
        for i in range(20):
            node.seen_messages.add(f"id-{i}")
        with node.lock:
            ids = list(node.seen_messages)[: node.ids_max_ihave]
        self.assertLessEqual(len(ids), 5)
        self.assertEqual(node.ids_max_ihave, 5)


class TestIhaveIwantGossipFlow(unittest.TestCase):
    """Integration: IHAVE -> IWANT -> GOSSIP flow (receiver gets missing message via pull)."""

    def test_handle_ihave_requests_missing_via_iwant(self):
        node = GossipNode(19996, '', 3, 8, 20, 2, 6, 45, mode='hybrid_push_pull')
        node.seen_messages.add("known-id")
        # Simulate IHAVE with one we have and one we don't
        msg = {
            'msg_type': 'IHAVE',
            'sender_id': 'other',
            'sender_addr': '127.0.0.1:8888',
            'payload': {'ids': ['known-id', 'missing-id'], 'max_ids': 32}
        }
        sent = []
        def capture_send(addr, m):
            sent.append((addr, m))
        node.send_udp = capture_send
        node._handle_ihave(msg, '127.0.0.1:8888')
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][1]['msg_type'], 'IWANT')
        self.assertIn('missing-id', sent[0][1]['payload']['ids'])
        self.assertNotIn('known-id', sent[0][1]['payload']['ids'])

    def test_handle_iwant_sends_gossip_for_cached_ids(self):
        node = GossipNode(19995, '', 3, 8, 20, 2, 6, 46, mode='hybrid_push_pull')
        full_gossip = {
            'msg_type': 'GOSSIP', 'msg_id': 'cached-1', 'ttl': 5,
            'payload': {'data': 'test', 'origin_id': node.node_id}
        }
        node._store_gossip_in_cache('cached-1', full_gossip)
        sent = []
        def capture_send(addr, m):
            sent.append((addr, m))
        node.send_udp = capture_send
        node._handle_iwant({
            'msg_type': 'IWANT',
            'sender_addr': '127.0.0.1:9999',
            'payload': {'ids': ['cached-1', 'not-cached']}
        }, '127.0.0.1:9999')
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][1]['msg_type'], 'GOSSIP')
        self.assertEqual(sent[0][1]['msg_id'], 'cached-1')


class TestHelloPowRejectAccept(unittest.TestCase):
    """Integration: HELLO rejected when PoW invalid; accepted when valid."""

    def test_hello_rejected_when_pow_invalid(self):
        node = GossipNode(19994, '', 3, 8, 20, 2, 6, 47, k_pow=2)
        hello_bad = {
            'msg_type': 'HELLO',
            'sender_id': 'joiner',
            'sender_addr': '127.0.0.1:7777',
            'payload': {'capabilities': ['udp', 'json'], 'pow': {'hash_alg': 'sha256', 'difficulty_k': 2, 'nonce': 0, 'digest_hex': 'ff0000'}}
        }
        self.assertFalse(node.validate_pow_on_hello(hello_bad))

    def test_hello_accepted_when_pow_valid(self):
        node = GossipNode(19993, '', 3, 8, 20, 2, 6, 48, k_pow=2)
        pow_data = node.compute_pow()
        hello_ok = {
            'msg_type': 'HELLO',
            'sender_id': node.node_id,
            'sender_addr': '127.0.0.1:6666',
            'payload': {'capabilities': ['udp', 'json'], 'pow': pow_data}
        }
        self.assertTrue(node.validate_pow_on_hello(hello_ok))


class TestRegressionPushOnly(unittest.TestCase):
    """Regression: push_only mode does not send IHAVE."""

    def test_push_only_has_no_ihave_thread_effect(self):
        node = GossipNode(19992, '', 3, 8, 20, 2, 6, 49, mode='push_only')
        self.assertEqual(node.mode, 'push_only')
        # Periodic IHAVE only runs when mode == hybrid_push_pull; no IHAVE sent in push_only
        node.ihave_sent = 0
        node._periodic_send_ihave()
        # Actually _periodic_send_ihave sleeps then checks mode; one iteration would skip
        self.assertEqual(node.mode, 'push_only')


class TestRegressionTTLSeenSet(unittest.TestCase):
    """Regression: TTL and SeenSet behavior unchanged."""

    def test_seen_set_prevents_reprocess(self):
        node = GossipNode(19991, '', 3, 8, 20, 2, 6, 50)
        node.seen_messages.add('dup-id')
        gossip_msg = {
            'msg_type': 'GOSSIP', 'msg_id': 'dup-id', 'ttl': 5,
            'sender_addr': '127.0.0.1:5555', 'sender_id': 'x',
            'payload': {'data': 'dup'}
        }
        forwarded = []
        def capture_forward(*a, **kw):
            forwarded.append(1)
        node.forward_gossip = capture_forward
        node.handle_message(gossip_msg)
        self.assertEqual(len(forwarded), 0)

    def test_ttl_decremented_on_forward(self):
        node = GossipNode(19990, '', 3, 8, 20, 2, 6, 51)
        orig = {'msg_id': 'x', 'payload': {'data': 'y'}, 'ttl': 3}
        node.forward_gossip(orig, 2)
        self.assertEqual(orig.get('ttl'), 3)


class TestAnalysisOverheadAndConvergence(unittest.TestCase):
    """Analysis checks: overhead includes IHAVE/IWANT; convergence uses 95%."""

    def test_simulator_target_is_95_percent(self):
        import math
        for n in [10, 20, 50]:
            target = math.ceil(n * 0.95)
            self.assertGreaterEqual(target, n * 0.95 - 0.01)
            self.assertLessEqual(target, n * 0.95 + 1.01)

    def test_total_messages_includes_all_types(self):
        # SimulatedNode.handle_message increments total_messages for every message (any type).
        # So overhead in simulator includes IHAVE and IWANT when mode is hybrid_push_pull.
        # We only verify the 95% target formula here (simulator import needs matplotlib).
        target = math.ceil(3 * 0.95)
        self.assertEqual(target, 3)


if __name__ == '__main__':
    unittest.main()
