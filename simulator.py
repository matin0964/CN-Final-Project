import time
import uuid
import math
import random
import collections
import threading

# Importing your logic (Assuming these are in the same directory or defined above)
from gossip_node import GossipNode
from message_builder import MessageBuilder

class SimulatedNode(GossipNode):
    """
    Overrides the UDP layer of your GossipNode to facilitate 
    in-memory message passing for the simulation.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulator = None
        self.messages_sent_count = 0
        self.has_received_gossip = 0
        self.lock = threading.Lock()
        self.reception_time = None  # Track when node first received gossip

    # def log(self, message):
    #     pass  # Suppress logs during bulk simulation

    def send_udp(self, target_addr, message_dict):
        self.messages_sent_count += 1
        if self.simulator:
            self.simulator.deliver_message(target_addr, message_dict)

    def handle_message(self, msg):
        if not isinstance(msg, dict):
            self.log(f"Invalid message: not a dictionary")
            return
        
        m_type = msg.get('msg_type')
        if not m_type:
            self.log(f"Invalid message: missing msg_type")
            return

        if m_type == 'GOSSIP':
            with self.lock:
                # Check if this is first time receiving gossip
                if self.has_received_gossip == 0:
                    self.has_received_gossip = 1
                    self.reception_time = time.time()
                    # Notify simulator about new reception
                    if self.simulator:
                        self.simulator.node_received_gossip(self.self_addr, self.reception_time)
        
        super().handle_message(msg)

    def cli_loop(*args, **kwargs):
        pass


class GossipNetworkSimulator:
    def __init__(self, n_nodes, fanout, ttl, timeout_seconds=30, seed=42):
        self.n_nodes = n_nodes
        self.nodes = {}
        self.total_messages = 0
        self.target_count = math.ceil(n_nodes * 0.95)
        self.timeout_seconds = timeout_seconds
        self.ttl = ttl
        
        # Track reception times
        self.reception_times = {}  # node_addr -> hop_received
        self.reception_lock = threading.Lock()
        self.simulation_complete = threading.Event()
        
        # Create nodes
        prev_node = None
        for i in range(n_nodes):
            port = 20000 + i
            node = SimulatedNode(port, prev_node.self_addr if prev_node != None else None, fanout, ttl, n_nodes, 10, 60, seed + port)
            node.simulator = self
            self.nodes[node.self_addr] = node
            prev_node = node

    def deliver_message(self, target_addr, msg):
        self.total_messages += 1
        if target_addr in self.nodes:
            self.nodes[target_addr].handle_message(msg)

    def node_received_gossip(self, node_addr, time):
        """Callback when a node receives gossip for the first time"""
        with self.reception_lock:
            if node_addr not in self.reception_times:
                self.reception_times[node_addr] = time
                
                # Check if we've reached target
                if len(self.reception_times) >= self.target_count:
                    self.simulation_complete.set()

    def send_initial_gossip(self, seed_node_addr, ttl, original_time):
        """Send gossip from a seed node to start the simulation"""

        if seed_node_addr in self.nodes:
            # Create a gossip message
            seed_node = self.nodes[seed_node_addr]

            payload = {
                "topic":"tea",
                "data":"fr fr",
                "origin_id":seed_node.node_id,
                "origin_timestamp_ms":original_time
                }

            gossip_msg =  MessageBuilder.build(msg_type='GOSSIP',
                                 sender_id=seed_node.node_id, 
                                 sender_addr= seed_node.self_addr,
                                 payload= payload,
                                 ttl=ttl,
                                )
            
            # Mark seed node as having received gossip at hop 0
            self.node_received_gossip(seed_node_addr, original_time)
            
            # Have the seed node process the message (which will trigger forwarding)
            seed_node.handle_message(gossip_msg)

    def run(self):
        """
        Run the gossip simulation:
        1. Start all nodes
        2. Wait for initialization
        3. Send initial gossip from a random node
        4. Wait for propagation or timeout
        5. Return results
        """
        print(f"Starting simulation with {self.n_nodes} nodes...")
        
        # Start all nodes
        for node in self.nodes.values():
            node.start()
            time.sleep(1)
        
        # Wait a bit for initialization
        time.sleep(2)
        print("sending gossip msg...")
        
        # Clear any existing reception data
        self.reception_times.clear()
        self.total_messages = 0
        self.simulation_complete.clear()
        
        # Select a random seed node to start the gossip
        seed_node_addr = random.choice(list(self.nodes.keys()))
        print(f"Seed node: {seed_node_addr}")
        
        # Send initial gossip
        start_time = time.time()
        self.send_initial_gossip(seed_node_addr, self.ttl, start_time)
        
        # Track propagation over time
        
        hop_check_interval = 0.1  # Check every 100ms
        max_hops = self.nodes[seed_node_addr].ttl * 2  # Upper bound on hops
        
        # Monitor propagation
        while (time.time() - start_time) < self.timeout_seconds:
            time.sleep(hop_check_interval)
            
            # Check if we've reached target
            with self.reception_lock:
                current_reached = len(self.reception_times)
                if current_reached >= self.target_count:
                    print(f"Target reached: {current_reached}/{self.n_nodes} nodes")
                    break
        
        # Calculate convergence metrics
        reception_list = list(self.reception_times.values())
        
        if len(reception_list) >= self.target_count:
            # Sort reception times and find when we hit target
            reception_list.sort()
            conv_time = reception_list[self.target_count - 1] - reception_list[0] # Hop when target reached
        else:
            conv_time = "Diverged"
        
        # Calculate message efficiency
        messages_per_node = self.total_messages / self.n_nodes if self.n_nodes > 0 else 0
        
        # Stop all nodes
        print("Stopping all nodes...")
        for node in self.nodes.values():
            node.stop()
        
        # Wait for threads to finish
        time.sleep(1)
        
        # Compile results
        results = {
            "n": self.n_nodes,
            "reached": len(self.reception_times),
            "conv_time": conv_time,
            "msgs": self.total_messages,
            "msgs_per_node": round(messages_per_node, 2),
            "coverage": f"{len(self.reception_times)/self.n_nodes*100:.1f}%"
        }
        
        return results


# --- Main Execution and Chart Printing ---

if __name__ == "__main__":
    N_VALUES = [5, 10, 20]  # Added more test values
    FANOUT = 3
    TTL = 10
    SEED = 42
    TIMEOUT = 10  # seconds

    results = []

    print("Running Gossip Protocol Simulations...")
    print("=" * 80)
    
    for n in N_VALUES:
        sim = GossipNetworkSimulator(
            n_nodes=n, 
            fanout=FANOUT, 
            ttl=TTL, 
            timeout_seconds=TIMEOUT,
            seed=SEED
        )
        
        result = sim.run()
        results.append(result)
        
        # Print progress
        print(f"Completed simulation for N={n}: {result['coverage']} coverage, {result['msgs']} total messages")
        print("-" * 80)
        time.sleep(5)

    # Print Comparison Chart
    print(f"\nGossip Simulation Results (Fanout={FANOUT}, TTL={TTL})")
    print("=" * 80)
    print(f"{'Nodes (N)':<10} | {'Reached':<12} | {'Coverage':<10} | {'Conv. Time':<12} | {'Total Msgs':<12} | {'Msgs/Node':<10}")
    print("-" * 80)
    
    for r in results:
        reached_str = f"{r['reached']}/{r['n']}"
        print(f"{r['n']:<10} | {reached_str:<12} | {r['coverage']:<10} | {str(r['conv_time']):<12} | {r['msgs']:<12} | {r['msgs_per_node']:<10}")
    print("=" * 80)