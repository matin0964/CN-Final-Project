import time
import uuid
import math
import random
import collections

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
        self.has_recieved_gossip = 0

    # def log(self, message):
    #     pass # Suppress logs during bulk simulation

    # def send_udp(self, target_addr, message_dict):
    #     self.messages_sent_count += 1
    #     if self.simulator:
    #         self.simulator.deliver_message(target_addr, message_dict)

    def handle_message(self,msg):
        if not isinstance(msg, dict):
            self.log(f"Invalid message: not a dictionary")
            return
        
        m_type = msg.get('msg_type')
        if not m_type:
            self.log(f"Invalid message: missing msg_type")
            return

        if m_type == GOSSIP:
            self.simulator.total_messages += 1
            self.has_recieved_gossip = 1
        super().handle_message(msg)

class GossipNetworkSimulator:
    def __init__(self, n_nodes, fanout, ttl, seed=42):
        self.n_nodes = n_nodes
        self.nodes = {}
        self.total_messages = 0
        self.target_count = math.ceil(n_nodes * 0.95)
        
        prev_node = None
        for i in range(n_nodes):
            port = 28000 + i
            node = SimulatedNode(port, prev_node, fanout, ttl, n_nodes, 10, 60, seed)
            node.simulator = self
            self.nodes[node.self_addr] = node
            prev_node = node

    def deliver_message(self, target_addr, msg):
        self.total_messages += 1
        if target_addr in self.nodes:
            self.nodes[target_addr].handle_message(msg)

    def run(self):
        start_node_addr = list(self.nodes.keys())[0]
        start_node = self.nodes[start_node_addr]
        
        payload = {"topic": "test", "data": "sim_msg", "origin_id": start_node.node_id}
        gossip_msg = MessageBuilder.build('GOSSIP', start_node.node_id, start_node.self_addr, payload, start_node.ttl)
        msg_id = gossip_msg['msg_id']

        start_node.seen_messages.add(msg_id)
        # Using 1 as the first step for convergence calculation
        start_node.gossip_reception_times[msg_id] = 1 
        start_node.forward_gossip(gossip_msg, start_node.ttl)

        # Analyze reception times from your GossipNode's internal dict
        receptions = [node.gossip_reception_times.get(msg_id) for node in self.nodes.values() 
                     if msg_id in node.gossip_reception_times]
        
        receptions.sort()
        
        # Calculate convergence (max hops to reach 95% of target)
        conv_time = max(receptions) if len(receptions) >= self.target_count else "Diverged"
        
        return {
            "n": self.n_nodes,
            "reached": len(receptions),
            "conv_time": conv_time,
            "msgs": self.total_messages
        }

# --- Main Execution and Chart Printing ---

if __name__ == "__main__":
    N_VALUES = [5, 10, 20]
    FANOUT = 3
    TTL = 10
    SEED = 42

    results = []

    for n in N_VALUES:
        sim = GossipNetworkSimulator(n_nodes=n, fanout=FANOUT, ttl=TTL, seed=SEED)
        results.append(sim.run())

    # Print Comparison Chart
    print(f"\nGossip Simulation Results (Fanout={FANOUT}, TTL={TTL})")
    print("-" * 65)
    print(f"{'Nodes (N)':<10} | {'Reached':<12} | {'Conv. Time (Hops)':<18} | {'Total MsgsSent':<12}")
    print("-" * 65)
    
    for r in results:
        reached_str = f"{r['reached']}/{r['n']}"
        print(f"{r['n']:<10} | {reached_str:<12} | {r['conv_time']:<18} | {r['msgs']:<12}")
    print("-" * 65)