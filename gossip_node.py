"""
Gossip protocol node: UDP listener, peer management, and message routing.
Handles HELLO, GET_PEERS, PEERS_LIST, PING/PONG, and GOSSIP message types.
"""

import json
import random
import socket
import threading
import time
import uuid
from datetime import datetime

from message_builder import MessageBuilder


class GossipNode:
    """
    Main gossip node: binds to a UDP port, discovers peers via bootstrap,
    maintains peer list with PING/timeout, and routes/forwards GOSSIP messages.
    """

    def __init__(self, port, bootstrap, fanout, ttl, peer_limit, ping_interval, peer_timeout, seed):
        # Identity and bind address
        self.node_id = str(uuid.uuid4())
        self.host = '127.0.0.1'
        self.port = port
        self.self_addr = f"{self.host}:{self.port}"

        # Protocol parameters
        self.bootstrap = bootstrap
        self.fanout = fanout
        self.ttl = ttl
        self.peer_limit = peer_limit
        self.ping_interval = ping_interval
        self.peer_timeout = peer_timeout

        random.seed(seed)

        # State: peers and deduplication
        # peers: {"ip:port": {"node_id": id, "last_seen": timestamp}}
        self.peers = {}
        # seen_messages: set of msg_id to avoid re-processing and loops
        self.seen_messages = set()
        self.lock = threading.Lock()

        # UDP socket bound to this node's port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))

        self.log(f"Node Initialized! ID: {self.node_id[:8]}... Addr: {self.self_addr}")

    def log(self, message):
        """Print a timestamped log line for this node."""
        t = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{t}] [Node {self.port}] {message}")

    def send_udp(self, target_addr, message_dict):
        """Serialize message to JSON and send over UDP to target_addr (ip:port)."""
        try:
            ip, port_str = target_addr.split(':')
            port = int(port_str)
            data = json.dumps(message_dict).encode('utf-8')
            self.sock.sendto(data, (ip, port))
        except Exception as e:
            self.log(f"Error sending to {target_addr}: {e}")

    # ---------- Network and message handling ----------
    def listen_thread(self):
        """Background thread: continuously read from socket and dispatch incoming messages."""
        self.log("Listener thread started...")
        while True:
            try:
                data, addr = self.sock.recvfrom(4096)
                msg = json.loads(data.decode('utf-8'))
                sender_addr = msg.get('sender_addr')

                # Any received message implies the sender is alive; refresh last_seen
                with self.lock:
                    if sender_addr in self.peers:
                        self.peers[sender_addr]['last_seen'] = time.time()

                self.handle_message(msg)
            except Exception as e:
                self.log(f"Receive error: {e}")

    def handle_message(self, msg):
        """Route incoming message by msg_type: HELLO, GET_PEERS, PEERS_LIST, PING, PONG, GOSSIP."""
        m_type = msg['msg_type']
        s_addr = msg['sender_addr']
        s_id = msg['sender_id']
        payload = msg.get('payload', {})

        if m_type == 'HELLO':
            # Add sender as peer if we have capacity
            with self.lock:
                if s_addr not in self.peers and len(self.peers) < self.peer_limit:
                    self.peers[s_addr] = {"node_id": s_id, "last_seen": time.time()}
                    self.log(f"New peer added from HELLO: {s_addr}")

        elif m_type == 'GET_PEERS':
            # Reply with our peer list (up to max_peers from payload)
            with self.lock:
                peer_list = [{"node_id": data["node_id"], "addr": addr} for addr, data in self.peers.items()]

            max_peers = payload.get('max_peers', 20)
            response_payload = {"peers": peer_list[:max_peers]}

            response_msg = MessageBuilder.build('PEERS_LIST', self.node_id, self.self_addr, response_payload, self.ttl)
            self.send_udp(s_addr, response_msg)

        elif m_type == 'PEERS_LIST':
            # Add discovered peers and send HELLO to each (if under limit)
            received_peers = payload.get('peers', [])
            for p in received_peers:
                p_addr = p['addr']
                if p_addr != self.self_addr:
                    with self.lock:
                        if p_addr not in self.peers and len(self.peers) < self.peer_limit:
                            self.peers[p_addr] = {"node_id": p['node_id'], "last_seen": time.time()}
                            self.log(f"Discovered new peer via PEERS_LIST: {p_addr}")

                            hello_msg = MessageBuilder.build('HELLO', self.node_id, self.self_addr, {"capabilities": ["udp", "json"]}, self.ttl)
                            self.send_udp(p_addr, hello_msg)

        elif m_type == 'PING':
            # Answer with PONG carrying same ping_id and seq
            pong_payload = {"ping_id": payload.get('ping_id'), "seq": payload.get('seq')}
            pong_msg = MessageBuilder.build('PONG', self.node_id, self.self_addr, pong_payload, self.ttl)
            self.send_udp(s_addr, pong_msg)

        elif m_type == 'PONG':
            # last_seen already updated in listen_thread; nothing else to do
            pass

        elif m_type == 'GOSSIP':
            msg_id = msg['msg_id']
            if msg_id in self.seen_messages:
                return

            self.seen_messages.add(msg_id)
            self.log(f"*** RECEIVED GOSSIP: '{payload.get('data')}' from {s_addr} ***")

            # Forward to up to fanout peers if TTL allows
            current_ttl = msg.get('ttl', self.ttl) - 1
            if current_ttl > 0:
                self.forward_gossip(msg, current_ttl, exclude_addr=s_addr)

    def forward_gossip(self, original_msg, new_ttl, exclude_addr=None):
        """Forward a GOSSIP message to up to fanout randomly chosen peers (excluding exclude_addr). Preserves msg_id for deduplication."""
        with self.lock:
            available_peers = [p for p in self.peers.keys() if p != exclude_addr]

        if not available_peers:
            return

        selected_peers = random.sample(available_peers, min(self.fanout, len(available_peers)))

        forward_msg = MessageBuilder.build(
            'GOSSIP', self.node_id, self.self_addr,
            original_msg['payload'], new_ttl, msg_id=original_msg['msg_id']
        )

        for p_addr in selected_peers:
            self.send_udp(p_addr, forward_msg)

    # ---------- Periodic maintenance ----------
    def periodic_thread(self):
        """Background thread: send PINGs at ping_interval and remove peers that exceed peer_timeout."""
        self.log("Periodic tasks thread started...")
        seq_num = 1
        while True:
            time.sleep(self.ping_interval)
            current_time = time.time()
            dead_peers = []

            with self.lock:
                for addr, data in self.peers.items():
                    if current_time - data['last_seen'] > self.peer_timeout:
                        dead_peers.append(addr)
                    else:
                        ping_msg = MessageBuilder.build('PING', self.node_id, self.self_addr, {"ping_id": str(uuid.uuid4()), "seq": seq_num}, self.ttl)
                        self.send_udp(addr, ping_msg)

                for addr in dead_peers:
                    del self.peers[addr]
                    self.log(f"Peer {addr} timed out and was removed.")

            seq_num += 1

    # ---------- Bootstrap and CLI ----------
    def start(self):
        """Start listener and periodic threads, optionally bootstrap, then run CLI loop."""
        t1 = threading.Thread(target=self.listen_thread, daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.periodic_thread, daemon=True)
        t2.start()

        if self.bootstrap and self.bootstrap != self.self_addr:
            self.log(f"Bootstrapping via {self.bootstrap}...")
            hello_msg = MessageBuilder.build('HELLO', self.node_id, self.self_addr, {"capabilities": ["udp", "json"]}, self.ttl)
            self.send_udp(self.bootstrap, hello_msg)

            time.sleep(0.5)

            get_peers_msg = MessageBuilder.build('GET_PEERS', self.node_id, self.self_addr, {"max_peers": self.peer_limit}, self.ttl)
            self.send_udp(self.bootstrap, get_peers_msg)

        self.cli_loop()

    def cli_loop(self):
        """Interactive CLI: 'gossip <message>' to broadcast, 'peers' to list peers, 'exit' to quit."""
        print("\nCommands: \n- type 'gossip <your_message>' to broadcast \n- type 'peers' to see peer list \n- type 'exit' to close\n")
        while True:
            try:
                cmd = input()
                if cmd.startswith("gossip "):
                    text = cmd.split("gossip ", 1)[1]
                    payload = {
                        "topic": "custom",
                        "data": text,
                        "origin_id": self.node_id,
                        "origin_timestamp_ms": int(time.time() * 1000)
                    }
                    gossip_msg = MessageBuilder.build('GOSSIP', self.node_id, self.self_addr, payload, self.ttl)

                    self.seen_messages.add(gossip_msg['msg_id'])

                    self.forward_gossip(gossip_msg, self.ttl)
                    self.log(f"Initiated GOSSIP: '{text}'")

                elif cmd == "peers":
                    with self.lock:
                        print(f"Current Peers ({len(self.peers)}/{self.peer_limit}):")
                        for addr, data in self.peers.items():
                            idle_time = int(time.time() - data['last_seen'])
                            print(f"  - {addr} (ID: {data['node_id'][:8]}..., Idle: {idle_time}s)")
                elif cmd == "exit":
                    self.log("Shutting down...")
                    break
            except KeyboardInterrupt:
                print("\nExiting...")
                break
