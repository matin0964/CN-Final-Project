import time
import uuid
import math
import random
import collections
import threading
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

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

    # def send_udp(self, target_addr, message_dict):
    #     self.messages_sent_count += 1
    #     if self.simulator:
    #         self.simulator.deliver_message(target_addr, message_dict)

    def handle_message(self, msg):
        if not isinstance(msg, dict):
            self.log(f"Invalid message: not a dictionary")
            return

        self.simulator.total_messages += 1
        
        m_type = msg.get('msg_type')
        if not m_type:
            self.log(f"Invalid message: missing msg_type")
            return

        if m_type == 'GOSSIP':
            self.simulator.total_gossip_msg += 1
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
        self.total_gossip_msg = 0
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
        print(f"  Running simulation with {self.n_nodes} nodes (seed-based)...")
        
        # Start all nodes
        for node in self.nodes.values():
            node.start()
            time.sleep(0.1)  # Reduced sleep for faster simulation
        
        # Wait a bit for initialization
        time.sleep(1)
        
        # Clear any existing reception data
        self.reception_times.clear()
        self.total_messages = 0
        self.total_gossip_msg = 0
        self.simulation_complete.clear()
        
        # Select a random seed node to start the gossip
        seed_node_addr = random.choice(list(self.nodes.keys()))
        
        # Send initial gossip
        start_time = time.time()
        self.send_initial_gossip(seed_node_addr, self.ttl, start_time)
        
        # Track propagation over time
        hop_check_interval = 0.1  # Check every 100ms
        
        # Monitor propagation
        while (time.time() - start_time) < self.timeout_seconds:
            time.sleep(hop_check_interval)
            
            # Check if we've reached target
            with self.reception_lock:
                current_reached = len(self.reception_times)
                if current_reached >= self.target_count:
                    break
        
        # Calculate convergence metrics
        reception_list = list(self.reception_times.values())
        
        if len(reception_list) >= self.target_count:
            # Sort reception times and find when we hit target
            reception_list.sort()
            conv_time = reception_list[self.target_count - 1] - reception_list[0]  # Time when target reached
        else:
            conv_time = float('nan')  # Use NaN for diverged cases
        
        # Calculate message efficiency
        messages_per_node = self.total_messages / self.n_nodes if self.n_nodes > 0 else 0
        
        # Stop all nodes
        for node in self.nodes.values():
            node.stop()
        
        # Wait for threads to finish
        time.sleep(0.5)
        
        # Compile results
        results = {
            "n": self.n_nodes,
            "reached": len(self.reception_times),
            "conv_time": conv_time,
            "msgs": self.total_messages,
            "gossip_msgs": self.total_gossip_msg,
            "msgs_per_node": round(messages_per_node, 2),
            "coverage": f"{len(self.reception_times)/self.n_nodes*100:.1f}%"
        }
        
        return results


def run_multiple_seeds(n_values, fanout, ttl, timeout, num_seeds=5):
    """
    Run simulations for each N with multiple seed values and combine results
    """
    all_results = defaultdict(list)
    
    print("=" * 50)
    print(f"GOSSIP PROTOCOL SIMULATION WITH {num_seeds} DIFFERENT SEEDS PER CONFIGURATION")
    print("=" * 50)
    
    for n in n_values:
        print(f"\n--- Processing N = {n} nodes ---")
        
        for seed_idx in range(num_seeds):
            # Use different seed for each run
            base_seed = 1000 * n + seed_idx * 100  # Different seed per run
            
            print(f"  Run {seed_idx + 1}/{num_seeds} (seed={base_seed})...")
            
            sim = GossipNetworkSimulator(
                n_nodes=n, 
                fanout=fanout, 
                ttl=ttl, 
                timeout_seconds=timeout,
                seed=base_seed
            )
            
            result = sim.run()
            
            # Store results
            all_results[n].append({
                'seed_idx': seed_idx,
                'conv_time': result['conv_time'] if not math.isnan(result['conv_time']) else None,
                'msgs': result['msgs'],
                'gossip_msgs': result['gossip_msgs'],
                'reached': result['reached'],
                'coverage': result['coverage']
            })
            
            # Small delay between runs
            time.sleep(1)
    
    return all_results


def aggregate_results(all_results):
    """
    Aggregate results across multiple seeds for each N
    """
    aggregated = []
    
    for n, runs in sorted(all_results.items()):
        # Filter out failed runs (where convergence didn't happen)
        valid_times = [r['conv_time'] for r in runs if r['conv_time'] is not None]
        valid_msgs = [r['msgs'] for r in runs]
        valid_gossip = [r['gossip_msgs'] for r in runs]
        valid_coverage = [float(r['coverage'].strip('%')) for r in runs]
        
        # Calculate statistics
        avg_conv_time = np.mean(valid_times) if valid_times else float('nan')
        std_conv_time = np.std(valid_times) if valid_times else float('nan')
        
        avg_msgs = np.mean(valid_msgs)
        std_msgs = np.std(valid_msgs)
        
        avg_gossip = np.mean(valid_gossip)
        avg_coverage = np.mean(valid_coverage)
        
        # Success rate (how many runs achieved target coverage)
        success_rate = len(valid_times) / len(runs) * 100
        
        aggregated.append({
            'n': n,
            'avg_conv_time': avg_conv_time,
            'std_conv_time': std_conv_time,
            'avg_msgs': avg_msgs,
            'std_msgs': std_msgs,
            'avg_gossip_msgs': avg_gossip,
            'avg_coverage': avg_coverage,
            'success_rate': success_rate,
            'num_runs': len(runs),
            'successful_runs': len(valid_times),
            'all_runs': runs  # Keep raw data for reference
        })
    
    return aggregated


def plot_results(aggregated_results, fanout, ttl):
    """
    Create charts of total messages and convergence time against N
    """
    # Extract data for plotting
    n_values = [r['n'] for r in aggregated_results]
    avg_msgs = [r['avg_msgs'] for r in aggregated_results]
    std_msgs = [r['std_msgs'] for r in aggregated_results]
    
    avg_conv_time = [r['avg_conv_time'] for r in aggregated_results]
    std_conv_time = [r['std_conv_time'] for r in aggregated_results]
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Plot 1: Total Messages vs N
    ax1.errorbar(n_values, avg_msgs, yerr=std_msgs, marker='o', capsize=5, capthick=2, 
                 color='blue', ecolor='lightblue', elinewidth=2)
    ax1.set_xlabel('Number of Nodes (N)', fontsize=12)
    ax1.set_ylabel('Total Messages', fontsize=12)
    ax1.set_title(f'Total Messages vs Network Size\n(Fanout={fanout}, TTL={ttl})', fontsize=14)
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    
    # Add trend line
    z = np.polyfit(np.log(n_values), np.log(avg_msgs), 1)
    trend_line = np.exp(z[1]) * np.array(n_values) ** z[0]
    ax1.plot(n_values, trend_line, 'r--', alpha=0.7, label=f'Trend: O(N^{z[0]:.2f})')
    ax1.legend()
    
    # Plot 2: Convergence Time vs N
    ax2.errorbar(n_values, avg_conv_time, yerr=std_conv_time, marker='s', capsize=5, capthick=2,
                 color='green', ecolor='lightgreen', elinewidth=2)
    ax2.set_xlabel('Number of Nodes (N)', fontsize=12)
    ax2.set_ylabel('Convergence Time (seconds)', fontsize=12)
    ax2.set_title(f'Convergence Time vs Network Size\n(Fanout={fanout}, TTL={ttl})', fontsize=14)
    ax2.grid(True, alpha=0.3)
    ax2.set_xscale('log')
    
    # Add trend line for convergence time
    z_time = np.polyfit(np.log(n_values), np.log(avg_conv_time), 1)
    trend_time = np.exp(z_time[1]) * np.array(n_values) ** z_time[0]
    ax2.plot(n_values, trend_time, 'r--', alpha=0.7, label=f'Trend: O(N^{z_time[0]:.2f})')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig('gossip_simulation_results.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    return fig


def print_detailed_results(aggregated_results, fanout, ttl, num_seeds):
    """
    Print detailed aggregated results in a formatted table
    """
    print("\n" + "=" * 60)
    print(f"AGGREGATED RESULTS (Average over {num_seeds} seeds per N)")
    print("=" * 60)
    print(f"Fanout={fanout}, TTL={ttl}")
    print("-" * 60)
    print(f"{'N':<6} | {'Success':<10} | {'Avg Coverage':<12} | {'Avg Msgs':<12} | {'Std Msgs':<10} | {'Avg Gossip':<12} | {'Avg Time':<12} | {'Std Time':<10}")
    print("-" * 60)
    
    for r in aggregated_results:
        success_str = f"{r['successful_runs']}/{r['num_runs']}"
        print(f"{r['n']:<6} | {success_str:<10} | {r['avg_coverage']:.1f}%{' ':<4} | {r['avg_msgs']:<12.0f} | {r['std_msgs']:<10.2f} | {r['avg_gossip_msgs']:<12.0f} | {r['avg_conv_time']:<12.4f} | {r['std_conv_time']:<10.4f}")
    
    print("=" * 60)


# --- Main Execution ---

if __name__ == "__main__":
    # Configuration
    N_VALUES = [10, 20, 50]  # Network sizes to test
    FANOUT = 3
    TTL = 10
    TIMEOUT = 15  # seconds
    NUM_SEEDS = 5  # Number of different seeds to try per N

    # Run simulations with multiple seeds
    all_results = run_multiple_seeds(
        n_values=N_VALUES,
        fanout=FANOUT,
        ttl=TTL,
        timeout=TIMEOUT,
        num_seeds=NUM_SEEDS
    )
    
    # Aggregate results
    aggregated = aggregate_results(all_results)
    
    # Print detailed results
    print_detailed_results(aggregated, FANOUT, TTL, NUM_SEEDS)
    
    # Create charts
    print("\nGenerating charts...")
    plot_results(aggregated, FANOUT, TTL)
    
    print("\nSimulation complete! Chart saved as 'gossip_simulation_results.png'")