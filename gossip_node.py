"""
Gossip protocol node: UDP listener, peer management, and message routing.
Handles HELLO, GET_PEERS, PEERS_LIST, PING/PONG, GOSSIP, IHAVE, IWANT (Phase 4).
Phase 4: Hybrid push-pull (IHAVE/IWANT) and Sybil resistance (PoW on HELLO).
"""

import hashlib
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

    def __init__(self, port, bootstrap, fanout, ttl, peer_limit, ping_interval, peer_timeout, seed,
                 mode='push_only', interval_pull=5.0, ids_max_ihave=32, k_pow=0, *args, **kwargs):
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
        # Phase 4: Hybrid push-pull
        self.mode = mode if mode in ('push_only', 'hybrid_push_pull') else 'push_only'
        self.interval_pull = float(interval_pull)
        self.ids_max_ihave = max(1, int(ids_max_ihave))
        # Phase 4: Sybil resistance
        self.k_pow = max(0, int(k_pow))

        self.ping_seqs = {}

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

        # Phase 4: message cache for IWANT responses (bounded)
        self._message_cache_max = 500
        self.message_cache = {}  # msg_id -> full GOSSIP message dict
        self._message_cache_order = []  # FIFO for eviction
        # Phase 4: counts for logging/analysis
        self.ihave_sent = 0
        self.ihave_received = 0
        self.iwant_sent = 0
        self.iwant_received = 0
        self.hello_rejected_pow = 0

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

    # ---------- Phase 4: Message cache (for IWANT responses) ----------
    def _store_gossip_in_cache(self, msg_id, full_gossip_msg):
        """Store a full GOSSIP message by msg_id; evict oldest if over limit."""
        if not msg_id or not full_gossip_msg:
            return
        with self.lock:
            if msg_id in self.message_cache:
                return
            while len(self.message_cache) >= self._message_cache_max and self._message_cache_order:
                old_id = self._message_cache_order.pop(0)
                self.message_cache.pop(old_id, None)
            self.message_cache[msg_id] = full_gossip_msg
            self._message_cache_order.append(msg_id)

    # ---------- Phase 4: PoW (Sybil resistance) ----------
    @staticmethod
    def _pow_digest(node_id, nonce):
        """H(node_id || nonce) as hex; use sha256."""
        data = f"{node_id}{nonce}".encode('utf-8')
        return hashlib.sha256(data).hexdigest()

    def validate_pow_on_hello(self, hello_msg):
        """
        Validate PoW in HELLO payload. If k_pow > 0, payload must contain pow with
        digest_hex = H(sender_id || nonce) with k_pow leading zero hex chars.
        Returns True if valid (or k_pow==0), False otherwise.
        """
        if self.k_pow <= 0:
            return True
        payload = hello_msg.get('payload') or {}
        pow_data = payload.get('pow')
        if not isinstance(pow_data, dict):
            return False
        sender_id = hello_msg.get('sender_id')
        if not sender_id:
            return False
        difficulty_k = pow_data.get('difficulty_k')
        if difficulty_k != self.k_pow:
            return False
        nonce = pow_data.get('nonce')
        digest_hex = pow_data.get('digest_hex')
        if nonce is None or not digest_hex:
            return False
        expected = self._pow_digest(sender_id, nonce)
        if not expected.startswith('0' * self.k_pow):
            return False
        if expected != digest_hex:
            return False
        return True

    def compute_pow(self, node_id=None):
        """
        Find nonce such that H(node_id || nonce) starts with k_pow leading zero hex chars.
        Returns dict: hash_alg, difficulty_k, nonce, digest_hex. If k_pow==0, returns minimal valid pow.
        """
        node_id = node_id or self.node_id
        if self.k_pow <= 0:
            nonce = 0
            digest = self._pow_digest(node_id, nonce)
            return {"hash_alg": "sha256", "difficulty_k": 0, "nonce": nonce, "digest_hex": digest}
        prefix = '0' * self.k_pow
        nonce = 0
        while True:
            digest = self._pow_digest(node_id, nonce)
            if digest.startswith(prefix):
                return {"hash_alg": "sha256", "difficulty_k": self.k_pow, "nonce": nonce, "digest_hex": digest}
            nonce += 1

    def _build_hello_payload(self):
        """Build HELLO payload (capabilities + optional PoW for Phase 4)."""
        payload = {"capabilities": ["udp", "json"]}
        if self.k_pow > 0:
            t0 = time.perf_counter()
            payload["pow"] = self.compute_pow()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self.log(f"PoW computed in {elapsed_ms:.2f} ms (k_pow={self.k_pow})")
        return payload

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
            # Phase 4: Validate PoW before accepting HELLO
            if not self.validate_pow_on_hello(msg):
                with self.lock:
                    self.hello_rejected_pow += 1
                self.log(f"HELLO rejected from {s_addr}: invalid or missing PoW (k_pow={self.k_pow})")
                return
            # Add sender as peer (with replacement policy if needed)
            # Only send HELLO back if peer was newly added to avoid infinite loops
            was_new = self._add_peer(s_addr, s_id)
            if was_new:
                self.log(f"New peer added from HELLO: {s_addr}")
                # Send HELLO back (with PoW if k_pow > 0)
                hello_payload = self._build_hello_payload()
                hello_response = MessageBuilder.build('HELLO', self.node_id, self.self_addr, hello_payload, self.ttl)
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
                        hello_payload = self._build_hello_payload()
                        hello_msg = MessageBuilder.build('HELLO', self.node_id, self.self_addr, hello_payload, self.ttl)
                        self.send_udp(p_addr, hello_msg)

        elif m_type == 'IHAVE':
            self._handle_ihave(msg, s_addr)

        elif m_type == 'IWANT':
            self._handle_iwant(msg, s_addr)

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

            # Store in cache for Phase 4 IWANT responses (before adding to seen to keep one copy)
            self._store_gossip_in_cache(msg_id, msg)

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

    # ---------- Phase 4: IHAVE / IWANT ----------
    def _handle_ihave(self, msg, sender_addr):
        """Compare IHAVE ids with SeenSet; request missing ids via IWANT."""
        with self.lock:
            self.ihave_received += 1
        payload = msg.get('payload') or {}
        ids = payload.get('ids')
        if not isinstance(ids, list):
            return
        with self.lock:
            missing = [mid for mid in ids if mid not in self.seen_messages]
        if not missing:
            return
        iwant_payload = {"ids": missing}
        iwant_msg = MessageBuilder.build('IWANT', self.node_id, self.self_addr, iwant_payload, self.ttl)
        self.send_udp(sender_addr, iwant_msg)
        with self.lock:
            self.iwant_sent += 1
        self.log(f"IHAVE from {sender_addr}: requested {len(missing)} missing via IWANT")

    def _handle_iwant(self, msg, sender_addr):
        """Respond to IWANT by sending full GOSSIP messages for requested ids we have in cache."""
        with self.lock:
            self.iwant_received += 1
        payload = msg.get('payload') or {}
        ids = payload.get('ids')
        if not isinstance(ids, list):
            return
        for mid in ids:
            with self.lock:
                full_msg = self.message_cache.get(mid)
            if not full_msg:
                continue
            # Re-send as GOSSIP (same format: msg_id, payload, ttl)
            current_ttl = full_msg.get('ttl', self.ttl)
            if current_ttl <= 0:
                continue
            gossip_msg = MessageBuilder.build(
                'GOSSIP', self.node_id, self.self_addr,
                full_msg.get('payload', {}), current_ttl, msg_id=full_msg.get('msg_id')
            )
            self.send_udp(sender_addr, gossip_msg)
        if ids:
            self.log(f"IWANT from {sender_addr}: sent {len([mid for mid in ids if self.message_cache.get(mid)])} GOSSIP(es)")

    def _periodic_send_ihave(self):
        """Send IHAVE to some peers every interval_pull (only in hybrid mode)."""
        self.log("IHAVE periodic thread started...")
        while self.running:
            time.sleep(self.interval_pull)
            if self.mode != 'hybrid_push_pull':
                continue
            with self.lock:
                peer_addrs = list(self.peers.keys())
                ids = list(self.seen_messages)[: self.ids_max_ihave]
            if not peer_addrs or not ids:
                continue
            n_peers = min(self.fanout, len(peer_addrs))
            chosen = random.sample(peer_addrs, n_peers)
            payload = {"ids": ids, "max_ids": self.ids_max_ihave}
            ihave_msg = MessageBuilder.build('IHAVE', self.node_id, self.self_addr, payload, self.ttl)
            for p_addr in chosen:
                self.send_udp(p_addr, ihave_msg)
                with self.lock:
                    self.ihave_sent += 1
            self.log(f"Sent IHAVE to {len(chosen)} peer(s) with {len(ids)} id(s)")
        self.log("IHAVE periodic thread stopped")

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
            
            prev = self.ping_seqs.get(addr)
            seq = self.ping_seqs[addr] = prev + 1 if prev != None else 0


            ping_msg = MessageBuilder.build(
                'PING', self.node_id, self.self_addr,
                {"ping_id": ping_id, "seq":seq }, self.ttl
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

        if self.mode == 'hybrid_push_pull':
            t4 = threading.Thread(target=self._periodic_send_ihave, daemon=True)
            t4.start()
            self.threads.append(t4)

        if self.bootstrap and self.bootstrap != self.self_addr:
            self.log(f"Bootstrapping via {self.bootstrap}...")
            hello_payload = self._build_hello_payload()
            hello_msg = MessageBuilder.build('HELLO', self.node_id, self.self_addr, hello_payload, self.ttl)
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

        self._store_gossip_in_cache(gossip_msg['msg_id'], gossip_msg)
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
                            print(f"  Mode: {self.mode}")
                            print(f"  IHAVE sent/received: {self.ihave_sent}/{self.ihave_received}")
                            print(f"  IWANT sent/received: {self.iwant_sent}/{self.iwant_received}")
                            print(f"  HELLO rejected (PoW): {self.hello_rejected_pow}")
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