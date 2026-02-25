"""
Entry point for the gossip protocol node.
Parses command-line arguments and starts a GossipNode.
"""

import argparse

from gossip_node import GossipNode


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gossip Protocol Node (Phase 1-4)')
    parser.add_argument('--port', type=int, required=True, help="Port to bind the UDP socket")
    parser.add_argument('--bootstrap', type=str, default="", help="IP:Port of a known node to connect to")
    parser.add_argument('--fanout', type=int, default=3, help="Number of peers to forward gossip to")
    parser.add_argument('--ttl', type=int, default=8, help="Time To Live for messages")
    parser.add_argument('--peer-limit', type=int, default=20, help="Maximum number of connections")
    parser.add_argument('--ping-interval', type=int, default=2, help="Seconds between PINGs")
    parser.add_argument('--peer-timeout', type=int, default=6, help="Seconds before a peer is considered dead")
    parser.add_argument('--seed', type=int, default=42, help="Random seed for reproducibility")
    # Phase 4: Hybrid push-pull
    parser.add_argument('--mode', type=str, default='push_only', choices=['push_only', 'hybrid_push_pull'],
                        help="Gossip mode: push_only or hybrid_push_pull")
    parser.add_argument('--interval-pull', type=float, default=5.0, help="Seconds between IHAVE rounds (hybrid mode)")
    parser.add_argument('--ids-max-ihave', type=int, default=32, help="Max message IDs to send in one IHAVE")
    # Phase 4: Sybil resistance
    parser.add_argument('--k-pow', type=int, default=0, help="PoW difficulty (leading zero hex chars); 0 disables")

    args = parser.parse_args()

    node = GossipNode(
        port=args.port,
        bootstrap=args.bootstrap,
        fanout=args.fanout,
        ttl=args.ttl,
        peer_limit=args.peer_limit,
        ping_interval=args.ping_interval,
        peer_timeout=args.peer_timeout,
        seed=args.seed,
        mode=args.mode,
        interval_pull=args.interval_pull,
        ids_max_ihave=args.ids_max_ihave,
        k_pow=args.k_pow
    )

    node.start()
