"""
Message builder for the gossip protocol.
Builds messages in the standard wire format used by all nodes.
"""

import time
import uuid


class MessageBuilder:
    """
    Constructs protocol messages in the project's standard format.
    All messages include version, msg_id, type, sender info, timestamp, TTL, and payload.
    """

    @staticmethod
    def build(msg_type, sender_id, sender_addr, payload, ttl, msg_id=None):
        """
        Build a message dict ready for JSON serialization and UDP send.

        Args:
            msg_type: Protocol message type (e.g. HELLO, GET_PEERS, GOSSIP, PING, PONG).
            sender_id: Unique identifier of the sending node.
            sender_addr: Address string "ip:port" of the sender.
            payload: Dict with message-specific data.
            ttl: Time-to-live (hop count) for propagation.
            msg_id: Optional message ID; if omitted, a new UUID is generated.

        Returns:
            A dict with keys: version, msg_id, msg_type, sender_id, sender_addr,
            timestamp_ms, ttl, payload.
        """
        return {
            "version": 1,
            "msg_id": msg_id if msg_id else str(uuid.uuid4()),
            "msg_type": msg_type,
            "sender_id": sender_id,
            "sender_addr": sender_addr,
            "timestamp_ms": int(time.time() * 1000),
            "ttl": ttl,
            "payload": payload
        }
