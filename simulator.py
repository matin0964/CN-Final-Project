import time
import uuid
import math
import random
import collections
import threading
import statistics
import matplotlib.pyplot as plt

# Importing your logic (Assuming these are in the same directory or defined above)
from gossip_node import GossipNode
from message_builder import MessageBuilder

class SimulatedNode(GossipNode):
    """
    Overrides the UDP layer of your GossipNode to facilitate 
    in-memory message passing for the simulation.
    """
    def __init__(self,simulator, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulator = simulator
        self.messages_sent_count = 0
        self.has_received_gossip = 0
        self.lock = threading.Lock()
        self.reception_time = None  # Track when node first received gossip

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
    def __init__(self, n_nodes, fanout, ttl, timeout_seconds=30, seed=42,
                 mode='push_only', interval_pull=5.0, ids_max_ihave=32, k_pow=0):
        self.n_nodes = n_nodes
        self.nodes = {}
        self.total_messages = 0
        self.total_gossip_msg = 0
        self.target_count = math.ceil(n_nodes * 0.95)
        self.timeout_seconds = timeout_seconds
        self.ttl = ttl
        self.mode = mode
        self.interval_pull = interval_pull
        self.ids_max_ihave = ids_max_ihave
        self.k_pow = k_pow

        # Track reception times
        self.reception_times = {}  # node_addr -> hop_received
        self.reception_lock = threading.Lock()
        self.simulation_complete = threading.Event()

        # Create nodes (Phase 4: pass mode, interval_pull, ids_max_ihave, k_pow)
        prev_node = None
        for i in range(n_nodes):
            port = 20000 + i
            node = SimulatedNode(
                self,
                port,
                prev_node.self_addr if prev_node is not None else None,
                fanout,
                ttl,
                n_nodes,
                10,
                60,
                seed + port,
                mode=mode,
                interval_pull=interval_pull,
                ids_max_ihave=ids_max_ihave,
                k_pow=k_pow
            )
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
            time.sleep(0.4)
        
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
            print("TIMEOUT")
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
            "gossip_msgs": self.total_gossip_msg,
            "msgs_per_node": round(messages_per_node, 2),
            "coverage": f"{len(self.reception_times)/self.n_nodes*100:.1f}%"
        }
        
        return results


def run_benchmarks(FANOUT=3, TTL=10, TIMEOUT=30, N_VALUES=None, SEEDS=None):
    """Run benchmarks for push_only and hybrid_push_pull. Returns dict by mode."""
    N_VALUES = N_VALUES or [10, 20, 50]
    SEEDS = SEEDS or [10, 20, 30]  # Multiple seeds for mean/std (Phase 4)

    results_by_mode = {'push_only': [], 'hybrid_push_pull': []}

    for mode in ('push_only', 'hybrid_push_pull'):
        print(f"\n>>> Mode: {mode}")
        for n in N_VALUES:
            seed_runs = []
            print(f"  N = {n} over {len(SEEDS)} seeds")
            for s in SEEDS:
                sim = GossipNetworkSimulator(
                    n_nodes=n,
                    fanout=FANOUT,
                    ttl=TTL,
                    timeout_seconds=TIMEOUT,
                    seed=s,
                    mode=mode,
                    interval_pull=5.0,
                    ids_max_ihave=32,
                    k_pow=0
                )
                res = sim.run()
                if isinstance(res['conv_time'], float):
                    seed_runs.append(res)
                time.sleep(4)

            if seed_runs:
                conv_times = [r['conv_time'] for r in seed_runs]
                msgs_list = [r['msgs'] for r in seed_runs]
                results_by_mode[mode].append({
                    'n': n,
                    'avg_conv': statistics.mean(conv_times),
                    'std_conv': statistics.stdev(conv_times) if len(conv_times) > 1 else 0.0,
                    'avg_msgs': statistics.mean(msgs_list),
                    'std_msgs': statistics.stdev(msgs_list) if len(msgs_list) > 1 else 0.0,
                    'avg_gossip': statistics.mean([r['gossip_msgs'] for r in seed_runs]),
                    'avg_coverage': statistics.mean([float(r['coverage'].strip('%')) for r in seed_runs]),
                })
            else:
                results_by_mode[mode].append({'n': n, 'avg_conv': None, 'std_conv': None,
                                             'avg_msgs': None, 'std_msgs': None})

    return results_by_mode

def plot_results(results_by_mode, fanout, ttl):
    """Plot push_only and hybrid_push_pull (Phase 4: compare modes)."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    for mode, results in results_by_mode.items():
        if not results or results[0].get('avg_conv') is None:
            continue
        ns = [r['n'] for r in results]
        msgs = [r['avg_msgs'] for r in results]
        conv_times = [r['avg_conv'] for r in results]
        std_conv = [r.get('std_conv', 0) for r in results]
        std_msgs = [r.get('std_msgs', 0) for r in results]
        ax1.errorbar(ns, msgs, yerr=std_msgs, marker='o', label=f'{mode} (msgs)')
        ax2.errorbar(ns, conv_times, yerr=std_conv, marker='s', label=f'{mode} (conv)')

    ax1.set_xlabel('Network Size (N)')
    ax1.set_ylabel('Total Messages (mean ± std)')
    ax1.legend()
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax2.set_xlabel('Network Size (N)')
    ax2.set_ylabel('Convergence Time (s) (mean ± std)')
    ax2.legend()
    ax2.grid(True, linestyle='--', alpha=0.7)
    plt.suptitle(f'Phase 4: push_only vs hybrid_push_pull (TTL={ttl}, fanout={fanout})')
    fig.tight_layout()
    plt.savefig(f'gossip_simulation_results_ttl{ttl}_fanout{fanout}.png', dpi=300, bbox_inches='tight')
    print("Chart saved as gossip_simulation_results_ttl*_fanout*.png")
    plt.show()


def run_pow_benchmark(k_values=(2, 3, 4), trials=10):
    """
    Phase 4: Measure PoW nonce generation time for 2-3 values of k_pow.
    Returns list of {k, mean_ms, std_ms, samples}.
    """
    from gossip_node import GossipNode
    results = []
    for k in k_values:
        node = GossipNode(9999, '', 3, 8, 20, 2, 6, 42, k_pow=k)
        times_ms = []
        for _ in range(trials):
            t0 = time.perf_counter()
            node.compute_pow()
            times_ms.append((time.perf_counter() - t0) * 1000)
        results.append({
            'k_pow': k,
            'mean_ms': statistics.mean(times_ms),
            'std_ms': statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0,
            'samples': times_ms
        })
    return results


if __name__ == "__main__":
    FANOUT = 3
    TTL = 10
    TIME_OUT = 30
    SEEDS = [10, 20, 30]

    results_by_mode = run_benchmarks(FANOUT=FANOUT, TTL=TTL, TIMEOUT=TIME_OUT, SEEDS=SEEDS)

    # Phase 4: Summary table (mean ± std) for both modes
    print("\n" + "=" * 70)
    print("Phase 4: push_only vs hybrid_push_pull (95% convergence, mean ± std)")
    print("=" * 70)
    for mode in ('push_only', 'hybrid_push_pull'):
        print(f"\n--- {mode} ---")
        print(f"{'N':<8} | {'Avg Msgs':<12} | {'Std Msgs':<10} | {'Avg Conv (s)':<12} | {'Std Conv':<8}")
        print("-" * 60)
        for r in results_by_mode.get(mode, []):
            if r.get('avg_conv') is not None:
                print(f"{r['n']:<8} | {r['avg_msgs']:<12.2f} | {r['std_msgs']:<10.2f} | "
                      f"{r['avg_conv']:<12.4f} | {r['std_conv']:<8.4f}")
    print("\n" + "=" * 70)

    plot_results(results_by_mode, FANOUT, TTL)

    # Phase 4: PoW benchmark (2-3 k_pow values)
    print("\n--- PoW nonce generation time (Phase 4) ---")
    pow_results = run_pow_benchmark(k_values=(2, 3, 4), trials=10)
    print(f"{'k_pow':<8} | {'mean (ms)':<12} | {'std (ms)':<10}")
    print("-" * 35)
    for row in pow_results:
        print(f"{row['k_pow']:<8} | {row['mean_ms']:<12.2f} | {row['std_ms']:<10.2f}")