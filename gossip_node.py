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
        # peers: {"ip:port": {"node_id": id, "last_seen": timestamp, "failed_pings": count}}
        self.peers = {}
        # seen_messages: set of msg_id to avoid re-processing and loops
        self.seen_messages = set()
        # gossip_reception_times: {"msg_id": timestamp_ms} for Phase 3 analysis
        self.gossip_reception_times = {}
        # pending_pings: {ping_id: (addr, sent_time_ms)} for CLI ping -> PONG round-trip
        self.pending_pings = {}
        self.lock = threading.Lock()

        # Control flags for thread shutdown
        self.running = False
        self.threads = []

        # UDP socket bound to this node's port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow address reuse
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(1.0)  # Set timeout so we can check running flag periodically

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
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                try:
                    msg = json.loads(data.decode('utf-8'))
                except json.JSONDecodeError as e:
                    self.log(f"Invalid JSON received from {addr}: {e}")
                    continue
                except UnicodeDecodeError as e:
                    self.log(f"Invalid encoding in message from {addr}: {e}")
                    continue

                sender_addr = msg.get('sender_addr')
                if not sender_addr:
                    self.log(f"Message missing sender_addr from {addr}")
                    continue

                # Any received message implies the sender is alive; refresh last_seen
                with self.lock:
                    if sender_addr in self.peers:
                        self.peers[sender_addr]['last_seen'] = time.time()

                self.handle_message(msg)
            except socket.timeout:
                # Timeout occurred, just loop and check running flag again
                continue
            except socket.error as e:
                if self.running:  # Only log if we're still supposed to be running
                    self.log(f"Socket error: {e}")
                break
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    self.log(f"Unexpected receive error: {e}")
                break
        
        self.log("Listener thread stopped")

    def _add_peer(self, addr, node_id):
        """
        Add a peer to the peer list. If peer_limit is reached, remove the oldest peer
        (least recently seen) to make room. This implements the replacement policy.
        
        Returns:
            True if peer was newly added, False if peer already existed
        """
        with self.lock:
            if addr in self.peers:
                # Already a peer, just update last_seen
                self.peers[addr]['last_seen'] = time.time()
                return False  # Peer already existed
            
            if len(self.peers) < self.peer_limit:
                # Have capacity, add directly
                self.peers[addr] = {"node_id": node_id, "last_seen": time.time(), "failed_pings": 0}
                return True  # Newly added
            else:
                # At capacity, remove oldest peer (least recently seen)
                oldest_addr = min(self.peers.keys(), key=lambda a: self.peers[a]['last_seen'])
                removed_id = self.peers[oldest_addr]['node_id']
                del self.peers[oldest_addr]
                self.log(f"Peer limit reached. Removed oldest peer {oldest_addr} (ID: {removed_id[:8]}...)")
                
                # Now add the new peer
                self.peers[addr] = {"node_id": node_id, "last_seen": time.time(), "failed_pings": 0}
                return True  # Newly added

    def handle_message(self, msg):
        """Route incoming message by msg_type: HELLO, GET_PEERS, PEERS_LIST, PING, PONG, GOSSIP."""
        # Validate message structure
        if not isinstance(msg, dict):
            self.log(f"Invalid message: not a dictionary")
            return
        
        m_type = msg.get('msg_type')
        if not m_type:
            self.log(f"Invalid message: missing msg_type")
            return
            
        s_addr = msg.get('sender_addr')
        s_id = msg.get('sender_id')
        payload = msg.get('payload', {})

        if m_type == 'HELLO':
            # Add sender as peer (with replacement policy if needed)
            # Only send HELLO back if peer was newly added to avoid infinite loops
            was_new = self._add_peer(s_addr, s_id)
            if was_new:
                self.log(f"New peer added from HELLO: {s_addr}")
                # Send HELLO back to establish bidirectional connection (only for new peers)
                hello_response = MessageBuilder.build('HELLO', self.node_id, self.self_addr, {"capabilities": ["udp", "json"]}, self.ttl)
                self.send_udp(s_addr, hello_response)

        elif m_type == 'GET_PEERS':
            # Reply with our peer list (up to max_peers from payload)
            with self.lock:
                peer_list = [{"node_id": data["node_id"], "addr": addr} for addr, data in self.peers.items()]

            max_peers = payload.get('max_peers', 20)
            response_payload = {"peers": peer_list[:max_peers]}

            response_msg = MessageBuilder.build('PEERS_LIST', self.node_id, self.self_addr, response_payload, self.ttl)
            self.send_udp(s_addr, response_msg)

        elif m_type == 'PEERS_LIST':
            # Add discovered peers and send HELLO to each (using replacement policy if needed)
            received_peers = payload.get('peers', [])
            for p in received_peers:
                p_addr = p.get('addr')
                p_node_id = p.get('node_id')
                if not p_addr or not p_node_id:
                    continue
                    
                if p_addr != self.self_addr:
                    was_new = self._add_peer(p_addr, p_node_id)
                    if was_new:
                        self.log(f"Discovered new peer via PEERS_LIST: {p_addr}")
                        # Only send HELLO to newly discovered peers
                        hello_msg = MessageBuilder.build('HELLO', self.node_id, self.self_addr, {"capabilities": ["udp", "json"]}, self.ttl)
                        self.send_udp(p_addr, hello_msg)

        elif m_type == 'PING':
            # Answer with PONG carrying same ping_id and seq (per protocol)
            ping_id = payload.get('ping_id')
            seq = payload.get('seq')
            pong_payload = {"ping_id": ping_id, "seq": seq}
            pong_msg = MessageBuilder.build('PONG', self.node_id, self.self_addr, pong_payload, self.ttl)
            self.send_udp(s_addr, pong_msg)
            self.log(f"Received PING from {s_addr}, sent PONG (seq={seq})")

        elif m_type == 'PONG':
            # last_seen already updated in listen_thread
            ping_id = payload.get('ping_id')
            seq = payload.get('seq')
            self.log(f"Received PONG from {s_addr} (seq={seq})")
            # Reset failed_pings counter since we got a PONG
            with self.lock:
                if s_addr in self.peers:
                    self.peers[s_addr]['failed_pings'] = 0
                # If this PONG is for a CLI-initiated ping, report RTT
                if ping_id and ping_id in self.pending_pings:
                    _addr, sent_ms = self.pending_pings.pop(ping_id)
                    rtt_ms = int(time.time() * 1000) - sent_ms
                    self.log(f"PONG round-trip to {s_addr}: {rtt_ms} ms")

        elif m_type == 'GOSSIP':
            msg_id = msg.get('msg_id')
            if not msg_id:
                self.log("Invalid GOSSIP message: missing msg_id")
                return
                
            if msg_id in self.seen_messages:
                return

            # Store reception time for Phase 3 analysis
            reception_time_ms = int(time.time() * 1000)
            with self.lock:
                self.seen_messages.add(msg_id)
                self.gossip_reception_times[msg_id] = reception_time_ms

            self.log(f"*** RECEIVED GOSSIP: '{payload.get('data')}' from {s_addr} (msg_id: {msg_id[:8]}...) ***")

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
            self.log(f"Forwarded GOSSIP to {p_addr}")

    # ---------- Periodic maintenance ----------
    def periodic_thread(self):
        """Background thread: remove peers that have 3+ consecutive failed CLI pings."""
        self.log("Periodic tasks thread started...")
        while self.running:
            time.sleep(self.ping_interval)
            dead_peers = []

            with self.lock:
                for addr, data in self.peers.items():
                    # Remove only if 3+ consecutive failed CLI pings (no general timeout)
                    if data.get('failed_pings', 0) >= 3:
                        dead_peers.append(addr)

                for addr in dead_peers:
                    del self.peers[addr]
                    self.log(f"Peer {addr} removed: 3 consecutive PINGs without PONG")
        
        self.log("Periodic thread stopped")

    def auto_ping_thread(self):
        """Pings every known peer once every ping_interval."""
        self.log(f"Auto-ping thread started (Interval: {self.ping_interval}s)")
        while self.running:
            time.sleep(self.ping_interval)
            with self.lock:
                peer_addrs = list(self.peers.keys())
            
            for addr in peer_addrs:
                self._send_ping_to_peer(addr)
        
        self.log("Auto-ping thread stopped")

    def _send_ping_to_peer(self, addr):
        """Internal helper to send a ping if not already pinged this cycle."""
        with self.lock:
            if addr not in self.peers:
                return
            
            peer_info = self.peers[addr]
            # Check if already pinged in this interval to satisfy "cannot be pinged twice"
            if peer_info.get('ping_pending', False):
                return

            peer_info['ping_pending'] = True
            ping_id = str(uuid.uuid4())
            self.pending_pings[ping_id] = (addr, int(time.time() * 1000))
            
            ping_msg = MessageBuilder.build(
                'PING', self.node_id, self.self_addr,
                {"ping_id": ping_id, "seq": 0}, self.ttl
            )
        
        self.send_udp(addr, ping_msg)

    # ---------- Bootstrap and CLI ----------
    def start(self):
        """Start listener and periodic threads, optionally bootstrap, then run CLI loop."""
        self.running = True
        
        # Create and start threads
        t1 = threading.Thread(target=self.listen_thread, daemon=True)
        t1.start()
        self.threads.append(t1)

        t2 = threading.Thread(target=self.periodic_thread, daemon=True)
        t2.start()
        self.threads.append(t2)

        t3 = threading.Thread(target=self.auto_ping_thread, daemon=True)
        t3.start()
        self.threads.append(t3)

        if self.bootstrap and self.bootstrap != self.self_addr:
            self.log(f"Bootstrapping via {self.bootstrap}...")
            hello_msg = MessageBuilder.build('HELLO', self.node_id, self.self_addr, {"capabilities": ["udp", "json"]}, self.ttl)
            self.send_udp(self.bootstrap, hello_msg)

            time.sleep(0.5)

            get_peers_msg = MessageBuilder.build('GET_PEERS', self.node_id, self.self_addr, {"max_peers": self.peer_limit}, self.ttl)
            self.send_udp(self.bootstrap, get_peers_msg)

        self.cli_loop()

    def stop(self):
        """
        Stop all threads and release resources (socket, etc.)
        This method should be called when shutting down the node.
        """
        self.log("Stopping node...")
        
        # Signal threads to stop
        self.running = False
        
        # Close socket to interrupt any blocking recvfrom calls
        try:
            if self.sock:
                self.sock.close()
                self.log("Socket closed")
        except Exception as e:
            self.log(f"Error closing socket: {e}")
        
        # Wait for threads to finish (with timeout)
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=0.1)
        
        
        self.log("Node stopped successfully")

    def send_gossip(self, text):
        """Send a gossip message to the network."""
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

    def cli_loop(self):
        """Interactive CLI for gossip, peers, ping, stats, help, exit."""
        print("\nCommands: gossip <msg> | peers | ping <addr> | stats | help | exit\n")
        while self.running:
            try:
                # Use non-blocking input with timeout to check running flag
                import sys
                import select
                
                # Check if there's input available (with timeout)
                if sys.stdin in select.select([sys.stdin], [], [], 0.5)[0]:
                    cmd = sys.stdin.readline().strip()
                    if not cmd:
                        continue
                        
                    if cmd.startswith("gossip "):
                        text = cmd.split("gossip ", 1)[1]
                        self.send_gossip(text)

                    elif cmd == "peers":
                        with self.lock:
                            print(f"Current Peers ({len(self.peers)}/{self.peer_limit}):")
                            for addr, data in self.peers.items():
                                idle_time = int(time.time() - data['last_seen'])
                                print(f"  - {addr} (ID: {data['node_id'][:8]}..., Idle: {idle_time}s)")
                    
                    elif cmd.startswith("ping "):
                        addr = cmd.split("ping ", 1)[1].strip()
                        if not addr:
                            self.log("Usage: ping <addr>  e.g. ping 127.0.0.1:8001")
                            continue
                        with self.lock:
                            if addr not in self.peers:
                                self.log(f"Unknown peer: {addr}. Use 'peers' to list addresses.")
                                continue
                            # Increment failed_pings counter (will be reset when PONG received)
                            self.peers[addr]['failed_pings'] = self.peers[addr].get('failed_pings', 0) + 1
                            failed_count = self.peers[addr]['failed_pings']
                        ping_id = str(uuid.uuid4())
                        sent_ms = int(time.time() * 1000)
                        with self.lock:
                            self.pending_pings[ping_id] = (addr, sent_ms)
                        ping_msg = MessageBuilder.build(
                            'PING', self.node_id, self.self_addr,
                            {"ping_id": ping_id, "seq": 0}, self.ttl
                        )
                        self.send_udp(addr, ping_msg)
                        self.log(f"Sent PING to {addr} (ping_id={ping_id[:8]}..., failed_pings={failed_count})")

                    elif cmd == "stats":
                        with self.lock:
                            print(f"\nNode Statistics:")
                            print(f"  Node ID: {self.node_id}")
                            print(f"  Address: {self.self_addr}")
                            print(f"  Peers: {len(self.peers)}/{self.peer_limit}")
                            print(f"  Seen Messages: {len(self.seen_messages)}")
                            print(f"  Gossip Messages Received: {len(self.gossip_reception_times)}")
                            if self.gossip_reception_times:
                                oldest = min(self.gossip_reception_times.values())
                                newest = max(self.gossip_reception_times.values())
                                print(f"  Gossip Reception Range: {newest - oldest}ms")
                    elif cmd == "help":
                        print("\nCommands: gossip <msg> | peers | ping <addr> | stats | help | exit\n")
                    elif cmd == "exit":
                        self.log("Shutting down...")
                        self.stop()
                        break
            except KeyboardInterrupt:
                print("\nExiting...")
                self.stop()
                break
            except Exception as e:
                self.log(f"CLI error: {e}")