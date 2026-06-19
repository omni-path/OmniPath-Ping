/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation;
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, write to the Free Software
* Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#undef PGO_TRAINING
#define PATH_TO_PGO_CONFIG "path_to_pgo_config"

#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <map>
#include <vector>
#include <set>
#include <deque>
#include <limits>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "ns3/core-module.h"
#include "ns3/qbb-header.h"
#include "ns3/qbb-helper.h"
#include "ns3/point-to-point-helper.h"
#include "ns3/applications-module.h"
#include "ns3/internet-module.h"
#include "ns3/global-route-manager.h"
#include "ns3/ipv4-static-routing-helper.h"
#include "ns3/packet.h"
#include "ns3/error-model.h"
#include "ns3/hash.h"
#include <ns3/rdma.h>
#include <ns3/rdma-client.h>
#include <ns3/rdma-client-helper.h>
#include <ns3/rdma-driver.h>
#include <ns3/opp-token-manager.h>
#include <ns3/switch-node.h>
#include <ns3/sim-setting.h>

using namespace ns3;
using namespace std;

NS_LOG_COMPONENT_DEFINE("GENERIC_SIMULATION");

uint32_t cc_mode = 1;
bool enable_qcn = true, use_dynamic_pfc_threshold = true;
uint32_t packet_payload_size = 1000, l2_chunk_size = 0, l2_ack_interval = 0;
double pause_time = 5, simulator_stop_time = 100.0;
std::string data_rate, link_delay, topology_file, flow_file, trace_file, trace_output_file;
std::string fct_output_file = "fct.txt";
std::string pfc_output_file = "pfc.txt";
std::string flow_coverage_output_file = "flow_coverage.txt";
std::string opp_probe_delay_output_file = "";
FILE *opp_probe_delay_output = NULL;
std::string opp_loopback_timeseries_output_file = "";
int32_t opp_loopback_timeseries_pod = -1;
std::string opp_loopback_peak_bucket_output_file = "";
uint64_t opp_loopback_peak_bucket_interval_ns = 100000;
std::string opp_loopback_rate_trace_output_file = "";
int32_t opp_loopback_rate_trace_node = -1;
uint64_t opp_loopback_rate_trace_start_relative_ns = 0;
uint64_t opp_loopback_rate_trace_end_relative_ns = 0;
uint64_t opp_loopback_rate_trace_interval_ns = 700;
uint32_t opp_usm_copy_latency_detail = 0;
std::string rdprobe_diag_output_file = "rdprobe_diag.txt";
std::string rdprobe_diag_result_output_file = "rdprobe_diag_result.txt";
uint32_t enable_rdprobe_diag = 0;
FILE *rdprobe_diag_file = NULL;
std::string rpingmesh_diag_result_output_file = "rpingmesh_diag_result.txt";
uint32_t enable_rpingmesh_diag = 0;
std::string opp_diag_result_output_file = "opp_diag_result.txt";
uint32_t enable_opp_diag = 0;

double alpha_resume_interval = 55, rp_timer, ewma_gain = 1 / 16;
double rate_decrease_interval = 4;
uint32_t fast_recovery_times = 5;
std::string rate_ai, rate_hai, min_rate = "100Mb/s";
std::string dctcp_rate_ai = "1000Mb/s";

bool clamp_target_rate = false, l2_back_to_zero = false;
double error_rate_per_link = 0.0;
uint32_t has_win = 1;
uint32_t global_t = 1;
uint32_t mi_thresh = 5;
bool var_win = false, fast_react = true;
bool multi_rate = true;
bool sample_feedback = false;
double pint_log_base = 1.05;
double pint_prob = 1.0;
double u_target = 0.95;
uint32_t int_multi = 1;
bool rate_bound = true;

uint32_t ack_high_prio = 0;
uint64_t link_down_time = 0;
uint32_t link_down_A = 0, link_down_B = 0;

uint32_t enable_trace = 1;
uint32_t init_debug_log = 0;
uint32_t init_link_detail_log = 0;
uint64_t switch_ingress_pipeline_delay_ns = 400;
uint64_t switch_egress_pipeline_delay_ns = 300;
uint32_t opp_tokens_per_tor = 1;
uint32_t opp_fattree_m1 = 1;
uint32_t opp_fattree_m2 = 1;
uint32_t opp_fattree_m3 = 1;
uint64_t opp_initial_interpod_probe_spread_ns = 0;
uint32_t opp_leafspine_m1 = 1;
uint32_t opp_leafspine_m2 = 1;
uint32_t opp_probe_instance_count = 1;
uint32_t fat_tree_fault_mode = 0;
uint32_t fat_tree_fault_seed = 1;
uint32_t fat_tree_fault_congestion_delay_ns = 500000;

uint32_t buffer_size = 16;

uint32_t qlen_dump_interval = 100000000, qlen_mon_interval = 100;
uint64_t qlen_mon_start = 2000000000, qlen_mon_end = 2100000000;
string qlen_mon_file;
std::string switch_qlen_trace_output_file = "";
int32_t switch_qlen_trace_node = -1;
uint64_t switch_qlen_trace_duration_ns = 1000000;
uint64_t switch_qlen_trace_interval_ns = 100;
uint64_t switch_rate_trace_window_ns = 700;
FILE *switch_qlen_trace_output = NULL;
bool switch_qlen_trace_started = false;
uint64_t switch_qlen_trace_start_ns = 0;
std::deque<std::pair<uint64_t, uint32_t> > switch_loopback_tx_records;
uint64_t switch_loopback_tx_window_bytes = 0;
std::vector<std::pair<uint64_t, uint32_t> > opp_loopback_rate_trace_tx_records;

unordered_map<uint64_t, uint32_t> rate2kmax, rate2kmin;
unordered_map<uint64_t, double> rate2pmax;

enum TopologyMode{
	TOPOLOGY_GENERAL_GRAPH = 0,
	TOPOLOGY_FAT_TREE = 1,
	TOPOLOGY_TORUS_2D = 2,
	TOPOLOGY_TORUS_3D = 3,
	TOPOLOGY_LEAF_SPINE = 4
};
TopologyMode topology_mode = TOPOLOGY_GENERAL_GRAPH;
uint32_t fat_tree_k = 0;

bool IsTorusTopology(){
	return topology_mode == TOPOLOGY_TORUS_2D || topology_mode == TOPOLOGY_TORUS_3D;
}

bool UsesGeneralGraphRouting(){
	return topology_mode == TOPOLOGY_GENERAL_GRAPH || IsTorusTopology();
}

enum SwitchRoutingMode{
	SWITCH_ROUTING_ECMP = 0,
	SWITCH_ROUTING_PACKET_SPRAY = 1
};
uint32_t switch_routing_mode = SWITCH_ROUTING_ECMP;
bool routing_mode_configured = false;

enum OppMulticastModeConfig{
	OPP_MULTICAST_MODE_STANDARD_CONFIG = 0,
	OPP_MULTICAST_MODE_USM_CONFIG = 1,
	OPP_MULTICAST_MODE_SAMPLED_CONFIG = 2
};
uint32_t opp_multicast_mode = OPP_MULTICAST_MODE_STANDARD_CONFIG;
uint32_t opp_usm_sample_period = 1;
uint32_t opp_usm_sample_rate_numerator = 1;
uint32_t opp_usm_sample_rate_denominator = 1;
uint32_t opp_usm_sample_policy = OppTokenManager::USM_SAMPLE_POLICY_QP_ORDINAL;

/************************************************
 * Runtime varibles
 ***********************************************/
std::ifstream topof, flowf, tracef;

NodeContainer n;

uint64_t nic_rate;

uint64_t maxRtt, maxBdp;
clock_t init_start_time;
uint64_t init_start_wall_us = 0;

uint64_t InitNowUs(){
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (uint64_t)tv.tv_sec * 1000000ull + (uint64_t)tv.tv_usec;
}

uint64_t GetRssBytes(){
	FILE *f = fopen("/proc/self/statm", "r");
	if (f == NULL)
		return 0;
	unsigned long size = 0, resident = 0;
	int ret = fscanf(f, "%lu %lu", &size, &resident);
	fclose(f);
	if (ret != 2)
		return 0;
	long pageSize = sysconf(_SC_PAGESIZE);
	if (pageSize <= 0)
		return 0;
	return (uint64_t)resident * (uint64_t)pageSize;
}

void InitLog(const char *stage){
	if (!init_debug_log)
		return;
	double elapsed = (double)(clock() - init_start_time) / CLOCKS_PER_SEC;
	double rssMb = (double)GetRssBytes() / 1024.0 / 1024.0;
	fprintf(stderr, "[INIT] %.3fs rss=%.1fMB %s\n", elapsed, rssMb, stage);
	fflush(stderr);
}

bool InitShouldLogProgress(uint32_t done, uint32_t total, uint32_t step){
	if (!init_debug_log)
		return false;
	return done == total || done == 1 || (step > 0 && done % step == 0);
}

void InitLogProgress(const char *stage, uint32_t done, uint32_t total){
	if (!init_debug_log)
		return;
	double elapsed = (double)(clock() - init_start_time) / CLOCKS_PER_SEC;
	double rssMb = (double)GetRssBytes() / 1024.0 / 1024.0;
	double pct = total == 0 ? 100.0 : (double)done * 100.0 / (double)total;
	fprintf(stderr, "[INIT] %.3fs rss=%.1fMB %s %u/%u %.1f%%\n", elapsed, rssMb, stage, done, total, pct);
	fflush(stderr);
}

struct LinkInitStats{
	uint64_t readUs;
	uint64_t attrUs;
	uint64_t flushUs;
	uint64_t installUs;
	uint64_t hostIpUs;
	uint64_t saveInterfaceUs;
	uint64_t ipv4SetBaseUs;
	uint64_t ipv4AssignUs;
	uint64_t pfcTraceUs;

	LinkInitStats(){
		Reset();
	}

	void Reset(){
		readUs = 0;
		attrUs = 0;
		flushUs = 0;
		installUs = 0;
		hostIpUs = 0;
		saveInterfaceUs = 0;
		ipv4SetBaseUs = 0;
		ipv4AssignUs = 0;
		pfcTraceUs = 0;
	}
};

void LinkInitAdd(uint64_t &field, uint64_t start){
	if (init_link_detail_log)
		field += InitNowUs() - start;
}

double LinkInitMs(uint64_t us){
	return (double)us / 1000.0;
}

void InitLogLinkDetail(uint32_t done, uint32_t total, uint32_t segmentStart, uint64_t segmentStartUs, LinkInitStats &stats){
	if (!init_link_detail_log)
		return;

	uint64_t now = InitNowUs();
	uint32_t segmentLinks = done - segmentStart;
	double elapsed = init_start_wall_us == 0 ? 0.0 : (double)(now - init_start_wall_us) / 1000000.0;
	double segmentSeconds = (double)(now - segmentStartUs) / 1000000.0;
	double avgUs = segmentLinks == 0 ? 0.0 : (double)(now - segmentStartUs) / (double)segmentLinks;
	double rssMb = (double)GetRssBytes() / 1024.0 / 1024.0;
	double pct = total == 0 ? 100.0 : (double)done * 100.0 / (double)total;
	fprintf(stderr,
		"[INIT-LINK] elapsed=%.3fs rss=%.1fMB links=%u/%u %.1f%% segment_links=%u segment=%.3fs avg=%.1fus read=%.3fms attr=%.3fms flush=%.3fms install=%.3fms host_ip=%.3fms save_if=%.3fms ipv4_setbase=%.3fms ipv4_assign=%.3fms pfc_trace=%.3fms\n",
		elapsed, rssMb, done, total, pct, segmentLinks, segmentSeconds, avgUs,
		LinkInitMs(stats.readUs), LinkInitMs(stats.attrUs), LinkInitMs(stats.flushUs),
		LinkInitMs(stats.installUs), LinkInitMs(stats.hostIpUs), LinkInitMs(stats.saveInterfaceUs),
		LinkInitMs(stats.ipv4SetBaseUs), LinkInitMs(stats.ipv4AssignUs), LinkInitMs(stats.pfcTraceUs));

	QbbHelper::InstallDebugStats qbbStats = QbbHelper::GetAndResetInstallDebugStats();
	fprintf(stderr,
		"[INIT-QBB] links=%u create_dev=%.3fms set_addr=%.3fms add_device=%.3fms queue=%.3fms mpi=%.3fms channel=%.3fms attach=%.3fms container=%.3fms\n",
		qbbStats.links, LinkInitMs(qbbStats.createDeviceUs), LinkInitMs(qbbStats.setAddressUs),
		LinkInitMs(qbbStats.addDeviceUs), LinkInitMs(qbbStats.queueUs), LinkInitMs(qbbStats.mpiCheckUs),
		LinkInitMs(qbbStats.createChannelUs), LinkInitMs(qbbStats.attachUs), LinkInitMs(qbbStats.containerUs));

	Ipv4AddressHelper::AssignDebugStats ipv4Stats = Ipv4AddressHelper::GetAndResetAssignDebugStats();
	fprintf(stderr,
		"[INIT-IPV4] containers=%u devices=%u created_interfaces=%u container_get=%.3fms get_node=%.3fms get_ipv4=%.3fms get_if_dev=%.3fms add_if=%.3fms new_addr=%.3fms add_addr=%.3fms set_metric_up=%.3fms retval=%.3fms\n",
		ipv4Stats.containers, ipv4Stats.devices, ipv4Stats.createdInterfaces,
		LinkInitMs(ipv4Stats.containerGetUs), LinkInitMs(ipv4Stats.getNodeUs), LinkInitMs(ipv4Stats.getIpv4Us),
		LinkInitMs(ipv4Stats.getInterfaceForDeviceUs), LinkInitMs(ipv4Stats.addInterfaceUs),
		LinkInitMs(ipv4Stats.newAddressUs), LinkInitMs(ipv4Stats.addAddressUs),
		LinkInitMs(ipv4Stats.setMetricUpUs), LinkInitMs(ipv4Stats.retvalAddUs));
	fflush(stderr);
}

struct Interface{
	uint32_t idx;
	bool up;
	uint64_t delay;
	uint64_t bw;

	Interface() : idx(0), up(false){}
};
map<Ptr<Node>, map<Ptr<Node>, Interface> > nbr2if;
// Mapping destination to next hop for each node: <node, <dest, <nexthop0, ...> > >
map<Ptr<Node>, map<Ptr<Node>, vector<Ptr<Node> > > > nextHop;
map<Ptr<Node>, map<Ptr<Node>, uint64_t> > pairDelay;
map<Ptr<Node>, map<Ptr<Node>, uint64_t> > pairTxDelay;
map<uint32_t, map<uint32_t, uint64_t> > pairBw;
map<Ptr<Node>, map<Ptr<Node>, uint64_t> > pairBdp;
map<uint32_t, map<uint32_t, uint64_t> > pairRtt;
map<uint32_t, map<uint32_t, uint32_t> > pairSwitchCount;
map<Ptr<Node>, map<Ptr<Node>, vector<Ptr<Node> > > > coverageNextHop;

std::vector<Ipv4Address> serverAddress;
std::vector<std::vector<uint32_t> > ifacePeer;
std::vector<uint32_t> hostToTor;
std::map<uint64_t, Ptr<OppTokenManager> > oppTokenManagers;

struct PairInfo{
	uint64_t rtt;
	uint64_t bw;
	uint64_t bdp;
	uint32_t switchCount;

	PairInfo() : rtt(0), bw(0), bdp(0), switchCount(0) {}
	PairInfo(uint64_t _rtt, uint64_t _bw, uint64_t _bdp, uint32_t _switchCount = 0) : rtt(_rtt), bw(_bw), bdp(_bdp), switchCount(_switchCount) {}
};

struct FatTreeLayout{
	uint32_t k;
	uint32_t half;
	uint32_t hostsPerEdge;
	uint32_t hostsPerPod;
	uint32_t hostCount;
	uint32_t edgeCount;
	uint32_t aggCount;
	uint32_t coreCount;
	uint32_t switchCount;
	uint32_t edgeBase;
	uint32_t aggBase;
	uint32_t coreBase;
	uint32_t nodeCount;

	FatTreeLayout() : k(0), half(0), hostsPerEdge(0), hostsPerPod(0), hostCount(0), edgeCount(0), aggCount(0), coreCount(0), switchCount(0), edgeBase(0), aggBase(0), coreBase(0), nodeCount(0) {}
};
FatTreeLayout fatTree;

struct FatTreeInterfaces{
	std::vector<Interface> hostToEdge;
	std::vector<Interface> edgeToHost;
	std::vector<Interface> edgeToAgg;
	std::vector<Interface> aggToEdge;
	std::vector<Interface> aggToCore;
	std::vector<Interface> coreToAgg;

	void Init(const FatTreeLayout &layout){
		hostToEdge.resize(layout.hostCount);
		edgeToHost.resize(layout.hostCount);
		edgeToAgg.resize(layout.k * layout.half * layout.half);
		aggToEdge.resize(layout.k * layout.half * layout.half);
		aggToCore.resize(layout.k * layout.half * layout.half);
		coreToAgg.resize(layout.half * layout.half * layout.k);
	}
};
FatTreeInterfaces fatTreeIf;

struct LeafSpineLayout{
	uint32_t k;
	uint32_t half;
	uint32_t hostCount;
	uint32_t leafCount;
	uint32_t spineCount;
	uint32_t switchCount;
	uint32_t leafBase;
	uint32_t spineBase;
	uint32_t nodeCount;

	LeafSpineLayout() : k(0), half(0), hostCount(0), leafCount(0), spineCount(0), switchCount(0), leafBase(0), spineBase(0), nodeCount(0) {}
};
LeafSpineLayout leafSpine;

// maintain port number for each host pair
std::unordered_map<uint32_t, unordered_map<uint32_t, uint16_t> > portNumder;

struct FlowInput{
	uint32_t src, dst, pg, maxPacketCount, port, dport;
	double start_time;
	uint32_t idx;
};
FlowInput flow_input = {0};
uint32_t flow_num;

PairInfo GetPairInfo(uint32_t src, uint32_t dst);
void GetOppTokenGroupForFlow(uint32_t src, uint32_t dst, uint32_t &sourceTorId, uint32_t &dstPodId);
uint32_t FatTreeAggNode(uint32_t pod, uint32_t agg);
uint32_t FatTreeCoreNode(uint32_t group, uint32_t col);

struct FlowCoverageKey{
	uint32_t sip, dip;
	uint16_t sport, dport, pg;

	bool operator<(const FlowCoverageKey &o) const{
		if (sip != o.sip) return sip < o.sip;
		if (dip != o.dip) return dip < o.dip;
		if (sport != o.sport) return sport < o.sport;
		if (dport != o.dport) return dport < o.dport;
		return pg < o.pg;
	}

	bool operator==(const FlowCoverageKey &o) const{
		return sip == o.sip && dip == o.dip && sport == o.sport &&
			dport == o.dport && pg == o.pg;
	}
};

struct FlowCoverageRecord{
	uint32_t src, dst, pg;
	uint16_t sport, dport;
	std::set<uint64_t> observedLinks;
	std::set<uint64_t> possibleLinks;
};

std::map<FlowCoverageKey, FlowCoverageRecord> flowCoverage;

struct RpingmeshDiagPacketKey{
	FlowCoverageKey flow;
	uint32_t seq;

	bool operator<(const RpingmeshDiagPacketKey &o) const{
		if (flow < o.flow) return true;
		if (o.flow < flow) return false;
		return seq < o.seq;
	}
};

struct RpingmeshDiagFlowState{
	uint32_t sentCount;
	uint32_t ackCount;
	uint32_t ttlExpected;
	uint32_t minObservedTtl;
	uint32_t maxObservedTtl;
	uint32_t ttlMismatchCount;
	uint64_t maxProbeAckDelayNs;
	uint32_t tracerouteAckCount;
	std::set<uint32_t> tracerouteSwitchIds;
	bool tracerouteTriggered;
	uint32_t tracerouteSelectedCore;
	uint32_t tracerouteSelectedCoreAckCount;
	uint64_t tracerouteSelectedCoreMaxDelayNs;
	uint32_t tracerouteFaultCore;
	std::string tracerouteResult;
	bool ttlFault;
	bool congestionFault;

	RpingmeshDiagFlowState()
		: sentCount(0), ackCount(0), ttlExpected(0), minObservedTtl(0xffffffffu),
		  maxObservedTtl(0), ttlMismatchCount(0), maxProbeAckDelayNs(0),
		  tracerouteAckCount(0), tracerouteTriggered(false), tracerouteSelectedCore(0),
		  tracerouteSelectedCoreAckCount(0), tracerouteSelectedCoreMaxDelayNs(0),
		  tracerouteFaultCore(0), tracerouteResult("fn"),
		  ttlFault(false), congestionFault(false) {}
};

std::map<FlowCoverageKey, RpingmeshDiagFlowState> rpingmeshDiagFlows;
std::map<RpingmeshDiagPacketKey, uint64_t> rpingmeshDiagSendTimes;

struct OppDiagFlowState{
	uint32_t ackCount;
	uint32_t faultyAckCount;
	uint32_t maxQDelay;
	bool faultyLinkFault;
	bool congestionFault;
	bool loopFault;

	OppDiagFlowState()
		: ackCount(0), faultyAckCount(0), maxQDelay(0),
		  faultyLinkFault(false), congestionFault(false), loopFault(false) {}
};

std::map<FlowCoverageKey, OppDiagFlowState> oppDiagFlows;

struct RdProbeDiagPacketKey{
	FlowCoverageKey flow;
	uint32_t seq;
	uint32_t nodeId;

	bool operator<(const RdProbeDiagPacketKey &o) const{
		if (flow < o.flow) return true;
		if (o.flow < flow) return false;
		if (seq != o.seq) return seq < o.seq;
		return nodeId < o.nodeId;
	}
};

struct RdProbeDiagPacketState{
	bool ingress;
	bool egress;
	uint32_t ingressPort;
	uint32_t egressPort;
	uint64_t ingressTimeNs;
	uint64_t egressTimeNs;

	RdProbeDiagPacketState() : ingress(false), egress(false), ingressPort(0), egressPort(0),
		ingressTimeNs(0), egressTimeNs(0) {}
};

std::map<RdProbeDiagPacketKey, RdProbeDiagPacketState> rdProbeDiagPackets;
std::map<FlowCoverageKey, bool> rdProbeDiagFlowFoundFault;

struct RdProbeDiagFlowPacketKey{
	FlowCoverageKey flow;
	uint32_t seq;

	bool operator<(const RdProbeDiagFlowPacketKey &o) const{
		if (flow < o.flow) return true;
		if (o.flow < flow) return false;
		return seq < o.seq;
	}
};

std::map<RdProbeDiagFlowPacketKey, std::vector<uint32_t> > rdProbeDiagIngressPaths;

struct RdProbeDiagTraceEvent{
	uint64_t timeNs;
	uint32_t event;
	uint32_t nodeId;
	uint32_t port;
	uint32_t sip;
	uint32_t dip;
	uint16_t sport;
	uint16_t dport;
	uint32_t pg;
	uint32_t seq;
};

std::map<RdProbeDiagFlowPacketKey, std::vector<RdProbeDiagTraceEvent> > rdProbeDiagPendingTraces;

void RegisterFlowCoverage(uint32_t src, uint32_t dst, uint32_t pg, uint16_t sport, uint16_t dport, uint32_t packetCount);
void TrackFlowCoverageLink(CustomHeader &ch, uint32_t nodeId, uint32_t inDev, uint32_t outDev);
void TraceSwitchQlenFirstPacket(CustomHeader &ch, uint32_t nodeId, uint32_t inDev, uint64_t timeNs);
void TraceSwitchLoopbackTx(uint32_t nodeId, uint32_t port, uint64_t timeNs, uint32_t bytes);
void TraceRdProbeDiag(CustomHeader &ch, uint32_t nodeId, uint32_t port, uint64_t timeNs, uint32_t event);
void TraceRpingmeshProbeSent(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport, uint16_t pg, uint32_t seq, uint64_t timeNs);
bool CheckRpingmeshProbeTtl(CustomHeader &ch, uint8_t observedTtl);
void TraceRpingmeshAckReceived(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport, uint16_t pg, uint32_t seq, uint64_t timeNs, uint32_t ackFlags, uint32_t tracerouteSwitchId);
void TraceOppAckReceived(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport, uint16_t pg, uint32_t maximumQDelay, uint32_t faultyLink, uint8_t isLoop);
bool IsFatTreeRdProbeIngressPathLegalPrefix(const FlowCoverageRecord &record, const std::vector<uint32_t> &path);
bool IsRdProbeIngressPathLegalPrefix(const FlowCoverageRecord &record, const std::vector<uint32_t> &path);
void FinalizeRpingmeshTracerouteDiagnosis(Ptr<RdmaQueuePair> q);
void DumpFlowCoverage();
void DumpRdProbeDiagnosis();
void DumpRpingmeshDiagnosis();
void DumpOppDiagnosis();
void DumpSwitchProbeAckRxCounts();

void ReadFlowInput(){
	if (flow_input.idx < flow_num){
		flowf >> flow_input.src >> flow_input.dst >> flow_input.pg >> flow_input.dport >> flow_input.maxPacketCount >> flow_input.start_time;
		NS_ASSERT(n.Get(flow_input.src)->GetNodeType() == 0 && n.Get(flow_input.dst)->GetNodeType() == 0);
	}
}

void ScheduleFlowInputs(){
	while (flow_input.idx < flow_num && Seconds(flow_input.start_time) == Simulator::Now()){
		PairInfo pair = GetPairInfo(flow_input.src, flow_input.dst);
		NS_ASSERT_MSG(pair.switchCount <= 0xff, "OPP remaining hop count exceeds 8 bits");
		uint32_t oppTokenSourceTorId = 0, oppTokenDstPodId = 0;
		GetOppTokenGroupForFlow(flow_input.src, flow_input.dst, oppTokenSourceTorId, oppTokenDstPodId);
		for (uint32_t instanceId = 0; instanceId < opp_probe_instance_count; instanceId++){
			uint16_t &nextPort = portNumder[flow_input.src][flow_input.dst];
			if (nextPort == 0)
				nextPort = 10000;
			NS_ASSERT_MSG(nextPort < 65535, "source port space exhausted for replicated probe instances");
			uint32_t port = nextPort++; // get a new port number
			RegisterFlowCoverage(flow_input.src, flow_input.dst, flow_input.pg, port, flow_input.dport, flow_input.maxPacketCount);
			RdmaClientHelper clientHelper(flow_input.pg, serverAddress[flow_input.src], serverAddress[flow_input.dst], port, flow_input.dport, flow_input.maxPacketCount, has_win?(global_t==1?maxBdp:pair.bdp):0, global_t==1?maxRtt:pair.rtt, pair.switchCount, oppTokenSourceTorId, oppTokenDstPodId, instanceId);
			ApplicationContainer appCon = clientHelper.Install(n.Get(flow_input.src));
			appCon.Start(Time(0));
		}

		// get the next flow input
		flow_input.idx++;
		ReadFlowInput();
	}

	// schedule the next time to run this function
	if (flow_input.idx < flow_num){
		Simulator::Schedule(Seconds(flow_input.start_time)-Simulator::Now(), ScheduleFlowInputs);
	}else { // no more flows, close the file
		flowf.close();
	}
}

Ipv4Address node_id_to_ip(uint32_t id){
	return Ipv4Address(0x0b000001 + ((id / 256) * 0x00010000) + ((id % 256) * 0x00000100));
}

uint32_t ip_to_node_id(Ipv4Address ip){
	return (ip.Get() >> 8) & 0xffff;
}

void AssignIpv4AddressToDevice(Ptr<Node> node, Ptr<NetDevice> device, Ipv4Address address, Ipv4Mask mask){
	Ptr<Ipv4> ipv4 = node->GetObject<Ipv4>();
	NS_ASSERT_MSG(ipv4 != NULL, "node must have IPv4 stack installed");
	int32_t interface = ipv4->GetInterfaceForDevice(device);
	if (interface == -1)
		interface = ipv4->AddInterface(device);
	NS_ASSERT_MSG(interface >= 0, "IPv4 interface index not found");
	ipv4->AddAddress(interface, Ipv4InterfaceAddress(address, mask));
	ipv4->SetMetric(interface, 1);
	ipv4->SetUp(interface);
}

void InitFatTreeLayout(uint32_t node_num, uint32_t switch_num, uint32_t link_num){
	NS_ASSERT_MSG(fat_tree_k > 0 && fat_tree_k % 2 == 0, "FAT_TREE_K must be a positive even number");
	uint64_t k = fat_tree_k;
	uint64_t half = k / 2;
	uint64_t hostCount = k * k * k / 4;
	uint64_t edgeCount = k * k / 2;
	uint64_t aggCount = k * k / 2;
	uint64_t coreCount = k * k / 4;
	uint64_t switchCount = edgeCount + aggCount + coreCount;
	uint64_t expectedNodeCount = hostCount + switchCount;
	uint64_t expectedLinkCount = hostCount * 3;
	NS_ASSERT_MSG(expectedNodeCount <= 0xffffffffull, "fat-tree node count exceeds uint32_t");
	NS_ASSERT_MSG(node_num == expectedNodeCount, "topology node count does not match FAT_TREE_K");
	NS_ASSERT_MSG(switch_num == switchCount, "topology switch count does not match FAT_TREE_K");
	NS_ASSERT_MSG(link_num == expectedLinkCount, "topology link count does not match FAT_TREE_K");
	NS_ASSERT_MSG(hostCount <= 65536ull, "current host IP encoding supports at most 65536 hosts");
	NS_ASSERT_MSG(edgeCount <= 65535ull, "fat-tree OPP source ToR ID field supports at most 65535 edge switches");
	NS_ASSERT_MSG(k <= 255ull, "fat-tree OPP destination PoD ID field supports at most k=255");

	fatTree.k = fat_tree_k;
	fatTree.half = (uint32_t)half;
	fatTree.hostsPerEdge = (uint32_t)half;
	fatTree.hostsPerPod = (uint32_t)(half * half);
	fatTree.hostCount = (uint32_t)hostCount;
	fatTree.edgeCount = (uint32_t)edgeCount;
	fatTree.aggCount = (uint32_t)aggCount;
	fatTree.coreCount = (uint32_t)coreCount;
	fatTree.switchCount = (uint32_t)switchCount;
	fatTree.edgeBase = fatTree.hostCount;
	fatTree.aggBase = fatTree.edgeBase + fatTree.edgeCount;
	fatTree.coreBase = fatTree.aggBase + fatTree.aggCount;
	fatTree.nodeCount = (uint32_t)expectedNodeCount;
	fatTreeIf.Init(fatTree);
}

void ValidateFatTreeNodeTypes(const std::vector<uint32_t> &node_type){
	for (uint32_t i = 0; i < fatTree.nodeCount; i++){
		if (i < fatTree.hostCount)
			NS_ASSERT_MSG(node_type[i] == 0, "fat-tree host IDs must be contiguous from 0");
		else
			NS_ASSERT_MSG(node_type[i] == 1, "fat-tree switch IDs must follow all host IDs");
	}
}

void InitLeafSpineLayout(uint32_t node_num, uint32_t switch_num, uint32_t link_num){
	NS_ASSERT_MSG(switch_num > 0 && (switch_num * 2) % 3 == 0, "leaf-spine switch count must be 3*k/2");
	uint64_t k = (uint64_t)switch_num * 2 / 3;
	NS_ASSERT_MSG(k > 0 && k % 2 == 0, "leaf-spine inferred k must be positive and even");
	uint64_t half = k / 2;
	uint64_t hostCount = k * half;
	uint64_t leafCount = k;
	uint64_t spineCount = half;
	uint64_t expectedSwitchCount = leafCount + spineCount;
	uint64_t expectedNodeCount = hostCount + expectedSwitchCount;
	uint64_t expectedLinkCount = hostCount + leafCount * spineCount;
	NS_ASSERT_MSG(expectedNodeCount <= 0xffffffffull, "leaf-spine node count exceeds uint32_t");
	NS_ASSERT_MSG(node_num == expectedNodeCount, "topology node count does not match LEAF_SPINE layout");
	NS_ASSERT_MSG(switch_num == expectedSwitchCount, "topology switch count does not match LEAF_SPINE layout");
	NS_ASSERT_MSG(link_num == expectedLinkCount, "topology link count does not match LEAF_SPINE layout");

	leafSpine.k = (uint32_t)k;
	leafSpine.half = (uint32_t)half;
	leafSpine.hostCount = (uint32_t)hostCount;
	leafSpine.leafCount = (uint32_t)leafCount;
	leafSpine.spineCount = (uint32_t)spineCount;
	leafSpine.switchCount = (uint32_t)expectedSwitchCount;
	leafSpine.leafBase = leafSpine.hostCount;
	leafSpine.spineBase = leafSpine.leafBase + leafSpine.leafCount;
	leafSpine.nodeCount = (uint32_t)expectedNodeCount;
}

void ValidateLeafSpineNodeTypes(const std::vector<uint32_t> &node_type){
	for (uint32_t i = 0; i < leafSpine.nodeCount; i++){
		if (i < leafSpine.hostCount)
			NS_ASSERT_MSG(node_type[i] == 0, "leaf-spine host IDs must be contiguous from 0");
		else
			NS_ASSERT_MSG(node_type[i] == 1, "leaf-spine switch IDs must follow all host IDs");
	}
}

uint32_t LeafSpineHostLeafIndex(uint32_t host){
	return host / leafSpine.half;
}

uint32_t LeafSpineHostLeaf(uint32_t host){
	return leafSpine.leafBase + LeafSpineHostLeafIndex(host);
}

uint32_t LeafSpineLeafNode(uint32_t leaf){
	return leafSpine.leafBase + leaf;
}

uint32_t LeafSpineSpineNode(uint32_t spine){
	return leafSpine.spineBase + spine;
}

bool IsFaultDiagnosisTopology(){
	return topology_mode == TOPOLOGY_FAT_TREE || topology_mode == TOPOLOGY_LEAF_SPINE;
}

uint32_t GetProbeFaultSwitchNode(){
	if (topology_mode == TOPOLOGY_FAT_TREE)
		return FatTreeAggNode(0, 0);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		return LeafSpineLeafNode(0);
	return 0;
}

uint32_t GetRpingmeshFaultTracerouteSwitchNode(){
	if (topology_mode == TOPOLOGY_FAT_TREE)
		return FatTreeCoreNode(0, 0);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		return LeafSpineSpineNode(0);
	return 0;
}

void GetRpingmeshTracerouteCandidateSwitches(std::vector<uint32_t> &candidates){
	candidates.clear();
	if (topology_mode == TOPOLOGY_FAT_TREE){
		for (uint32_t coreLocal = 0; coreLocal < fatTree.coreCount; coreLocal++)
			candidates.push_back(fatTree.coreBase + coreLocal);
	}else if (topology_mode == TOPOLOGY_LEAF_SPINE){
		for (uint32_t spineIdx = 0; spineIdx < leafSpine.spineCount; spineIdx++)
			candidates.push_back(LeafSpineSpineNode(spineIdx));
	}
}

uint32_t FatTreeHostPod(uint32_t host){
	return host / fatTree.hostsPerPod;
}

uint32_t FatTreeHostEdge(uint32_t host){
	return (host % fatTree.hostsPerPod) / fatTree.hostsPerEdge;
}

uint32_t FatTreeHostIndex(uint32_t host){
	return host % fatTree.hostsPerEdge;
}

uint32_t FatTreeEdgeNode(uint32_t pod, uint32_t edge){
	return fatTree.edgeBase + pod * fatTree.half + edge;
}

uint32_t FatTreeAggNode(uint32_t pod, uint32_t agg){
	return fatTree.aggBase + pod * fatTree.half + agg;
}

uint32_t FatTreeCoreNode(uint32_t group, uint32_t col){
	return fatTree.coreBase + group * fatTree.half + col;
}

uint32_t FatTreeHostEdgeNode(uint32_t host){
	return FatTreeEdgeNode(FatTreeHostPod(host), FatTreeHostEdge(host));
}

void GetOppTokenGroupForFlow(uint32_t src, uint32_t dst, uint32_t &sourceTorId, uint32_t &dstPodId){
	sourceTorId = 0;
	dstPodId = 0;
	if (topology_mode == TOPOLOGY_FAT_TREE){
		uint32_t srcPod = FatTreeHostPod(src);
		uint32_t dstPod = FatTreeHostPod(dst);
		uint32_t srcEdge = FatTreeHostEdge(src);
		uint32_t dstEdge = FatTreeHostEdge(dst);
		if (srcPod == dstPod && srcEdge == dstEdge)
			return;

		sourceTorId = srcPod * fatTree.half + srcEdge + 1;
		if (srcPod != dstPod)
			dstPodId = dstPod + 1;
		NS_ASSERT_MSG(sourceTorId <= 0xffff, "OPP fat-tree source ToR token field exceeds 16 bits");
		NS_ASSERT_MSG(dstPodId <= 0xff, "OPP fat-tree destination PoD token field exceeds 8 bits");
		return;
	}
	if (topology_mode == TOPOLOGY_LEAF_SPINE){
		uint32_t srcLeaf = LeafSpineHostLeafIndex(src);
		uint32_t dstLeaf = LeafSpineHostLeafIndex(dst);
		sourceTorId = srcLeaf + 1;
		if (srcLeaf != dstLeaf)
			dstPodId = dstLeaf + 1;
		NS_ASSERT_MSG(sourceTorId <= 0xffff, "OPP leaf-spine source ToR token field exceeds 16 bits");
		NS_ASSERT_MSG(dstPodId <= 0xff, "OPP leaf-spine destination ToR token field exceeds 8 bits");
	}
}

uint32_t GetOppTokenTorForHost(uint32_t host){
	if (topology_mode == TOPOLOGY_FAT_TREE)
		return FatTreeHostEdgeNode(host);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		return LeafSpineHostLeaf(host);
	NS_ASSERT_MSG(host < hostToTor.size() && hostToTor[host] != 0xffffffffu, "OPP token manager requires each host to have a direct ToR/leaf switch");
	return hostToTor[host];
}

uint64_t GetOppTokenManagerKey(uint32_t instanceId, uint32_t torId){
	return ((uint64_t)instanceId << 32) | (uint64_t)torId;
}

uint32_t GetOppLegacyTokenIdStride(){
	uint64_t stride = (uint64_t)serverAddress.size() * (uint64_t)opp_tokens_per_tor;
	NS_ASSERT_MSG(stride > 0 && stride <= 0xffffffffull, "legacy OPP token ID stride exceeds 32 bits");
	return (uint32_t)stride;
}

void ValidateOppProbeInstanceCapacity(){
	NS_ASSERT_MSG(opp_probe_instance_count > 0, "OPP_PROBE_INSTANCE_COUNT must be positive");
	if (opp_probe_instance_count == 1)
		return;

	if (topology_mode == TOPOLOGY_FAT_TREE){
		uint64_t sourceStride = (uint64_t)fatTree.edgeCount + 1;
		uint64_t maxEncodedSource = ((uint64_t)opp_probe_instance_count - 1) * sourceStride + fatTree.edgeCount;
		NS_ASSERT_MSG(maxEncodedSource <= 0xffffull, "OPP_PROBE_INSTANCE_COUNT makes fat-tree token source field exceed 16 bits");
		return;
	}
	if (topology_mode == TOPOLOGY_LEAF_SPINE){
		uint64_t sourceStride = (uint64_t)leafSpine.leafCount + 1;
		uint64_t maxEncodedSource = ((uint64_t)opp_probe_instance_count - 1) * sourceStride + leafSpine.leafCount;
		NS_ASSERT_MSG(maxEncodedSource <= 0xffffull, "OPP_PROBE_INSTANCE_COUNT makes leaf-spine token source field exceed 16 bits");
		return;
	}

	uint64_t stride = GetOppLegacyTokenIdStride();
	uint64_t maxServerId = serverAddress.empty() ? 0 : (uint64_t)serverAddress.size() - 1;
	uint64_t maxTokenId = ((uint64_t)opp_probe_instance_count - 1) * stride +
		maxServerId * (uint64_t)opp_tokens_per_tor + (uint64_t)opp_tokens_per_tor - 1;
	NS_ASSERT_MSG(maxTokenId <= 0xffffffffull, "OPP_PROBE_INSTANCE_COUNT makes legacy token IDs exceed 32 bits");
}

Ptr<OppTokenManager> GetOppTokenManager(uint32_t instanceId, uint32_t torId){
	uint64_t key = GetOppTokenManagerKey(instanceId, torId);
	std::map<uint64_t, Ptr<OppTokenManager> >::iterator it = oppTokenManagers.find(key);
	if (it != oppTokenManagers.end())
		return it->second;
	NS_ASSERT_MSG(instanceId < opp_probe_instance_count, "OPP token manager instance ID exceeds OPP_PROBE_INSTANCE_COUNT");
	Ptr<OppTokenManager> manager = CreateObject<OppTokenManager>();
	manager->SetProbeInstance(instanceId, opp_probe_instance_count);
	if (topology_mode == TOPOLOGY_FAT_TREE)
	{
		manager->SetGroupedSourceIdStride(fatTree.edgeCount + 1);
		manager->ConfigureFatTree(torId, opp_fattree_m1, opp_fattree_m2, opp_fattree_m3, fatTree.half);
		manager->SetInitialInterPodProbeSpreadNs(opp_initial_interpod_probe_spread_ns);
	}
	else if (topology_mode == TOPOLOGY_LEAF_SPINE)
	{
		manager->SetGroupedSourceIdStride(leafSpine.leafCount + 1);
		manager->ConfigureLeafSpine(torId, opp_leafspine_m1, opp_leafspine_m2);
	}
	else
	{
		manager->SetLegacyTokenIdStride(GetOppLegacyTokenIdStride());
		manager->Configure(torId, opp_tokens_per_tor);
	}
	manager->SetUsmSampleRate(opp_usm_sample_rate_numerator, opp_usm_sample_rate_denominator);
	manager->SetUsmSamplePolicy(opp_usm_sample_policy);
	oppTokenManagers[key] = manager;
	return manager;
}

uint32_t FatTreeEdgeToAggIndex(uint32_t pod, uint32_t edge, uint32_t agg){
	return (pod * fatTree.half + edge) * fatTree.half + agg;
}

uint32_t FatTreeAggToEdgeIndex(uint32_t pod, uint32_t agg, uint32_t edge){
	return (pod * fatTree.half + agg) * fatTree.half + edge;
}

uint32_t FatTreeAggToCoreIndex(uint32_t pod, uint32_t agg, uint32_t coreCol){
	return (pod * fatTree.half + agg) * fatTree.half + coreCol;
}

uint32_t FatTreeCoreToAggIndex(uint32_t group, uint32_t coreCol, uint32_t pod){
	return (group * fatTree.half + coreCol) * fatTree.k + pod;
}

void SaveInterface(Interface &out, Ptr<QbbNetDevice> dev){
	out.idx = dev->GetIfIndex();
	out.up = true;
	out.delay = DynamicCast<QbbChannel>(dev->GetChannel())->GetDelay().GetTimeStep();
	out.bw = dev->GetDataRate().GetBitRate();
}

void SaveFatTreeInterfaces(uint32_t src, uint32_t dst, Ptr<QbbNetDevice> srcDev, Ptr<QbbNetDevice> dstDev){
	NS_ASSERT_MSG(src < fatTree.nodeCount && dst < fatTree.nodeCount, "fat-tree link endpoint out of range");

	if (src >= fatTree.hostCount && dst < fatTree.hostCount){
		std::swap(src, dst);
		std::swap(srcDev, dstDev);
	}

	if (src < fatTree.hostCount && dst >= fatTree.edgeBase && dst < fatTree.aggBase){
		uint32_t pod = FatTreeHostPod(src);
		uint32_t edge = FatTreeHostEdge(src);
		NS_ASSERT_MSG(dst == FatTreeEdgeNode(pod, edge), "host must connect to its canonical edge switch");
		SaveInterface(fatTreeIf.hostToEdge[src], srcDev);
		SaveInterface(fatTreeIf.edgeToHost[src], dstDev);
		return;
	}

	if (src >= fatTree.edgeBase && src < fatTree.aggBase && dst >= fatTree.aggBase && dst < fatTree.coreBase){
		uint32_t edgeLocal = src - fatTree.edgeBase;
		uint32_t aggLocal = dst - fatTree.aggBase;
		uint32_t edgePod = edgeLocal / fatTree.half;
		uint32_t edge = edgeLocal % fatTree.half;
		uint32_t aggPod = aggLocal / fatTree.half;
		uint32_t agg = aggLocal % fatTree.half;
		NS_ASSERT_MSG(edgePod == aggPod, "edge and aggregation switch must be in the same pod");
		SaveInterface(fatTreeIf.edgeToAgg[FatTreeEdgeToAggIndex(edgePod, edge, agg)], srcDev);
		SaveInterface(fatTreeIf.aggToEdge[FatTreeAggToEdgeIndex(edgePod, agg, edge)], dstDev);
		return;
	}
	if (dst >= fatTree.edgeBase && dst < fatTree.aggBase && src >= fatTree.aggBase && src < fatTree.coreBase){
		SaveFatTreeInterfaces(dst, src, dstDev, srcDev);
		return;
	}

	if (src >= fatTree.aggBase && src < fatTree.coreBase && dst >= fatTree.coreBase){
		uint32_t aggLocal = src - fatTree.aggBase;
		uint32_t coreLocal = dst - fatTree.coreBase;
		uint32_t pod = aggLocal / fatTree.half;
		uint32_t agg = aggLocal % fatTree.half;
		uint32_t coreGroup = coreLocal / fatTree.half;
		uint32_t coreCol = coreLocal % fatTree.half;
		NS_ASSERT_MSG(coreGroup == agg, "aggregation switch must connect to matching core group");
		SaveInterface(fatTreeIf.aggToCore[FatTreeAggToCoreIndex(pod, agg, coreCol)], srcDev);
		SaveInterface(fatTreeIf.coreToAgg[FatTreeCoreToAggIndex(coreGroup, coreCol, pod)], dstDev);
		return;
	}
	if (dst >= fatTree.aggBase && dst < fatTree.coreBase && src >= fatTree.coreBase){
		SaveFatTreeInterfaces(dst, src, dstDev, srcDev);
		return;
	}

	NS_ASSERT_MSG(false, "invalid fat-tree link");
}

Interface& GetInterface(uint32_t src, uint32_t dst){
	if (topology_mode == TOPOLOGY_FAT_TREE){
		if (src < fatTree.hostCount){
			NS_ASSERT_MSG(dst == FatTreeHostEdgeNode(src), "invalid fat-tree host uplink lookup");
			return fatTreeIf.hostToEdge[src];
		}
		if (dst < fatTree.hostCount){
			NS_ASSERT_MSG(src == FatTreeHostEdgeNode(dst), "invalid fat-tree edge-to-host lookup");
			return fatTreeIf.edgeToHost[dst];
		}
		if (src >= fatTree.edgeBase && src < fatTree.aggBase && dst >= fatTree.aggBase && dst < fatTree.coreBase){
			uint32_t edgeLocal = src - fatTree.edgeBase;
			uint32_t aggLocal = dst - fatTree.aggBase;
			uint32_t edgePod = edgeLocal / fatTree.half;
			uint32_t aggPod = aggLocal / fatTree.half;
			NS_ASSERT_MSG(edgePod == aggPod, "invalid fat-tree edge-to-aggregation lookup");
			return fatTreeIf.edgeToAgg[FatTreeEdgeToAggIndex(edgePod, edgeLocal % fatTree.half, aggLocal % fatTree.half)];
		}
		if (src >= fatTree.aggBase && src < fatTree.coreBase && dst >= fatTree.edgeBase && dst < fatTree.aggBase){
			uint32_t aggLocal = src - fatTree.aggBase;
			uint32_t edgeLocal = dst - fatTree.edgeBase;
			uint32_t aggPod = aggLocal / fatTree.half;
			uint32_t edgePod = edgeLocal / fatTree.half;
			NS_ASSERT_MSG(aggPod == edgePod, "invalid fat-tree aggregation-to-edge lookup");
			return fatTreeIf.aggToEdge[FatTreeAggToEdgeIndex(aggPod, aggLocal % fatTree.half, edgeLocal % fatTree.half)];
		}
		if (src >= fatTree.aggBase && src < fatTree.coreBase && dst >= fatTree.coreBase){
			uint32_t aggLocal = src - fatTree.aggBase;
			uint32_t coreLocal = dst - fatTree.coreBase;
			NS_ASSERT_MSG(coreLocal / fatTree.half == aggLocal % fatTree.half, "invalid fat-tree aggregation-to-core lookup");
			return fatTreeIf.aggToCore[FatTreeAggToCoreIndex(aggLocal / fatTree.half, aggLocal % fatTree.half, coreLocal % fatTree.half)];
		}
		if (src >= fatTree.coreBase && dst >= fatTree.aggBase && dst < fatTree.coreBase){
			uint32_t coreLocal = src - fatTree.coreBase;
			uint32_t aggLocal = dst - fatTree.aggBase;
			NS_ASSERT_MSG(coreLocal / fatTree.half == aggLocal % fatTree.half, "invalid fat-tree core-to-aggregation lookup");
			return fatTreeIf.coreToAgg[FatTreeCoreToAggIndex(coreLocal / fatTree.half, coreLocal % fatTree.half, aggLocal / fatTree.half)];
		}
		NS_ASSERT_MSG(false, "missing fat-tree interface");
	}
	Ptr<Node> s = n.Get(src), d = n.Get(dst);
	NS_ASSERT_MSG(nbr2if.find(s) != nbr2if.end() && nbr2if[s].find(d) != nbr2if[s].end(), "missing topology link");
	return nbr2if[s][d];
}

void AddFatTreeHop(uint32_t src, uint32_t dst, uint64_t &delay, uint64_t &txDelay, uint64_t &bw){
	Interface &intf = GetInterface(src, dst);
	delay += intf.delay;
	txDelay += packet_payload_size * 1000000000lu * 8 / intf.bw;
	bw = std::min(bw, intf.bw);
}

PairInfo GetFatTreePairInfo(uint32_t src, uint32_t dst){
	NS_ASSERT_MSG(src < fatTree.hostCount && dst < fatTree.hostCount, "fat-tree pair info requires host IDs");
	if (src == dst)
		return PairInfo(0, nic_rate, 0);

	uint32_t srcPod = FatTreeHostPod(src);
	uint32_t dstPod = FatTreeHostPod(dst);
	uint32_t srcEdge = FatTreeHostEdge(src);
	uint32_t dstEdge = FatTreeHostEdge(dst);
	uint32_t srcEdgeNode = FatTreeEdgeNode(srcPod, srcEdge);
	uint32_t dstEdgeNode = FatTreeEdgeNode(dstPod, dstEdge);
	uint64_t delay = 0, txDelay = 0, bw = 0xfffffffffffffffflu;
	uint32_t switchCount = 0;

	AddFatTreeHop(src, srcEdgeNode, delay, txDelay, bw);
	if (srcPod == dstPod && srcEdge == dstEdge){
		switchCount = 1;
		AddFatTreeHop(dstEdgeNode, dst, delay, txDelay, bw);
	}else if (srcPod == dstPod){
		switchCount = 3;
		uint32_t aggNode = FatTreeAggNode(srcPod, 0);
		AddFatTreeHop(srcEdgeNode, aggNode, delay, txDelay, bw);
		AddFatTreeHop(aggNode, dstEdgeNode, delay, txDelay, bw);
		AddFatTreeHop(dstEdgeNode, dst, delay, txDelay, bw);
	}else{
		switchCount = 5;
		uint32_t aggIndex = 0;
		uint32_t coreCol = 0;
		uint32_t srcAggNode = FatTreeAggNode(srcPod, aggIndex);
		uint32_t coreNode = FatTreeCoreNode(aggIndex, coreCol);
		uint32_t dstAggNode = FatTreeAggNode(dstPod, aggIndex);
		AddFatTreeHop(srcEdgeNode, srcAggNode, delay, txDelay, bw);
		AddFatTreeHop(srcAggNode, coreNode, delay, txDelay, bw);
		AddFatTreeHop(coreNode, dstAggNode, delay, txDelay, bw);
		AddFatTreeHop(dstAggNode, dstEdgeNode, delay, txDelay, bw);
		AddFatTreeHop(dstEdgeNode, dst, delay, txDelay, bw);
	}

	uint64_t rtt = delay * 2 + txDelay;
	uint64_t bdp = rtt * bw / 1000000000 / 8;
	return PairInfo(rtt, bw, bdp, switchCount);
}

void AddLeafSpineHop(uint32_t src, uint32_t dst, uint64_t &delay, uint64_t &txDelay, uint64_t &bw){
	Interface &intf = GetInterface(src, dst);
	delay += intf.delay;
	txDelay += packet_payload_size * 1000000000lu * 8 / intf.bw;
	bw = std::min(bw, intf.bw);
}

PairInfo GetLeafSpinePairInfo(uint32_t src, uint32_t dst){
	NS_ASSERT_MSG(src < leafSpine.hostCount && dst < leafSpine.hostCount, "leaf-spine pair info requires host IDs");
	if (src == dst)
		return PairInfo(0, nic_rate, 0);

	uint32_t srcLeaf = LeafSpineHostLeaf(src);
	uint32_t dstLeaf = LeafSpineHostLeaf(dst);
	uint64_t delay = 0, txDelay = 0, bw = 0xfffffffffffffffflu;
	uint32_t switchCount = srcLeaf == dstLeaf ? 1 : 3;

	AddLeafSpineHop(src, srcLeaf, delay, txDelay, bw);
	if (srcLeaf != dstLeaf){
		uint32_t spineNode = LeafSpineSpineNode(0);
		AddLeafSpineHop(srcLeaf, spineNode, delay, txDelay, bw);
		AddLeafSpineHop(spineNode, dstLeaf, delay, txDelay, bw);
	}
	AddLeafSpineHop(dstLeaf, dst, delay, txDelay, bw);

	uint64_t rtt = delay * 2 + txDelay;
	uint64_t bdp = rtt * bw / 1000000000 / 8;
	return PairInfo(rtt, bw, bdp, switchCount);
}

PairInfo GetPairInfo(uint32_t src, uint32_t dst){
	if (topology_mode == TOPOLOGY_FAT_TREE)
		return GetFatTreePairInfo(src, dst);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		return GetLeafSpinePairInfo(src, dst);
	return PairInfo(pairRtt[src][dst], pairBw[src][dst], pairBdp[n.Get(src)][n.Get(dst)], pairSwitchCount[src][dst]);
}

uint64_t MakeCoverageLinkKey(uint32_t src, uint32_t dst){
	return ((uint64_t)src << 32) | (uint64_t)dst;
}

FlowCoverageKey MakeCoverageFlowKey(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport, uint16_t pg){
	FlowCoverageKey key;
	key.sip = sip;
	key.dip = dip;
	key.sport = sport;
	key.dport = dport;
	key.pg = pg;
	return key;
}

void TraceRpingmeshProbeSent(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport,
		uint16_t pg, uint32_t seq, uint64_t timeNs){
	if (!enable_rpingmesh_diag || cc_mode != RDMA_CC_MODE_RPINGMESH)
		return;
	FlowCoverageKey flow = MakeCoverageFlowKey(sip, dip, sport, dport, pg);
	RpingmeshDiagFlowState &state = rpingmeshDiagFlows[flow];
	state.sentCount++;
	if (fat_tree_fault_mode == 4){
		RpingmeshDiagPacketKey pkt;
		pkt.flow = flow;
		pkt.seq = seq;
		rpingmeshDiagSendTimes[pkt] = timeNs;
	}
}

bool CheckRpingmeshProbeTtl(CustomHeader &ch, uint8_t observedTtl){
	if (!enable_rpingmesh_diag || cc_mode != RDMA_CC_MODE_RPINGMESH ||
			fat_tree_fault_mode != 2)
		return false;
	uint32_t src = ip_to_node_id(Ipv4Address(ch.sip));
	uint32_t dst = ip_to_node_id(Ipv4Address(ch.dip));
	PairInfo pair = GetPairInfo(src, dst);
	uint32_t expectedTtl = pair.switchCount <= 64 ? 64 - pair.switchCount : 0;
	FlowCoverageKey flow = MakeCoverageFlowKey(ch.sip, ch.dip, ch.udp.sport, ch.udp.dport, ch.udp.pg);
	RpingmeshDiagFlowState &state = rpingmeshDiagFlows[flow];
	state.ttlExpected = expectedTtl;
	if (observedTtl < state.minObservedTtl)
		state.minObservedTtl = observedTtl;
	if (observedTtl > state.maxObservedTtl)
		state.maxObservedTtl = observedTtl;
	if (observedTtl != expectedTtl)
		state.ttlMismatchCount++;
	return observedTtl != expectedTtl;
}

void TraceRpingmeshAckReceived(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport,
		uint16_t pg, uint32_t seq, uint64_t timeNs, uint32_t ackFlags,
		uint32_t tracerouteSwitchId){
	if (!enable_rpingmesh_diag || cc_mode != RDMA_CC_MODE_RPINGMESH)
		return;
	bool markedFault = ((ackFlags >> qbbHeader::FLAG_RPINGMESH_FAULT) & 1) != 0;
	bool tracerouteAck = ((ackFlags >> qbbHeader::FLAG_RPINGMESH_TRACEROUTE) & 1) != 0;
	FlowCoverageKey flow = MakeCoverageFlowKey(sip, dip, sport, dport, pg);
	RpingmeshDiagFlowState &state = rpingmeshDiagFlows[flow];
	if (tracerouteAck){
		state.tracerouteAckCount++;
		state.tracerouteSwitchIds.insert(tracerouteSwitchId);
		return;
	}
	state.ackCount++;
	if (fat_tree_fault_mode == 2 && markedFault)
		state.ttlFault = true;
	if (fat_tree_fault_mode == 4){
		RpingmeshDiagPacketKey pkt;
		pkt.flow = flow;
		pkt.seq = seq;
		std::map<RpingmeshDiagPacketKey, uint64_t>::iterator it = rpingmeshDiagSendTimes.find(pkt);
		if (it != rpingmeshDiagSendTimes.end()){
			uint64_t delay = timeNs > it->second ? timeNs - it->second : 0;
			if (delay > state.maxProbeAckDelayNs)
				state.maxProbeAckDelayNs = delay;
			if (delay > 300000)
				state.congestionFault = true;
			rpingmeshDiagSendTimes.erase(it);
		}
	}
}

void TraceOppAckReceived(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport,
		uint16_t pg, uint32_t maximumQDelay, uint32_t faultyLink, uint8_t isLoop){
	if (!enable_opp_diag || cc_mode != RDMA_CC_MODE_OPP)
		return;
	FlowCoverageKey flow = MakeCoverageFlowKey(sip, dip, sport, dport, pg);
	OppDiagFlowState &state = oppDiagFlows[flow];
	state.ackCount++;
	if (faultyLink != 0){
		state.faultyAckCount++;
		if (fat_tree_fault_mode == 1 || fat_tree_fault_mode == 3)
			state.faultyLinkFault = true;
		if (fat_tree_fault_mode == 2 && isLoop != 0)
			state.loopFault = true;
	}
	if (maximumQDelay > state.maxQDelay)
		state.maxQDelay = maximumQDelay;
	if (fat_tree_fault_mode == 4 && maximumQDelay > 200000)
		state.congestionFault = true;
}

uint32_t PickRpingmeshTracerouteMinCore(const std::vector<uint32_t> &candidates){
	NS_ASSERT_MSG(!candidates.empty(), "must have at least one min-core candidate");
	uint32_t idx = (uint32_t)UniformVariable(0, (double)candidates.size()).GetValue();
	if (idx >= candidates.size())
		idx = candidates.size() - 1;
	return candidates[idx];
}

bool IsFatTreeRpingmeshTracerouteLegalSwitch(const FlowCoverageRecord &record, uint32_t nodeId){
	if (record.src == record.dst)
		return false;

	uint32_t srcPod = FatTreeHostPod(record.src);
	uint32_t dstPod = FatTreeHostPod(record.dst);
	uint32_t srcEdge = FatTreeHostEdge(record.src);
	uint32_t dstEdge = FatTreeHostEdge(record.dst);
	if (srcPod == dstPod && srcEdge == dstEdge)
		return false;

	if (srcPod == dstPod)
		return nodeId == FatTreeEdgeNode(dstPod, dstEdge);

	return nodeId >= fatTree.coreBase && nodeId < fatTree.coreBase + fatTree.coreCount;
}

bool IsLeafSpineRpingmeshTracerouteLegalSwitch(const FlowCoverageRecord &record, uint32_t nodeId){
	if (record.src == record.dst)
		return false;

	uint32_t srcLeaf = LeafSpineHostLeaf(record.src);
	uint32_t dstLeaf = LeafSpineHostLeaf(record.dst);
	if (srcLeaf == dstLeaf)
		return false;

	if (fat_tree_fault_mode == 2)
		return false;

	return nodeId >= leafSpine.spineBase &&
		nodeId < leafSpine.spineBase + leafSpine.spineCount;
}

bool IsRpingmeshTracerouteLegalSwitch(const FlowCoverageRecord &record, uint32_t nodeId){
	if (topology_mode == TOPOLOGY_FAT_TREE)
		return IsFatTreeRpingmeshTracerouteLegalSwitch(record, nodeId);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		return IsLeafSpineRpingmeshTracerouteLegalSwitch(record, nodeId);
	return false;
}

void FinalizeRpingmeshTracerouteDiagnosis(Ptr<RdmaQueuePair> q){
	if (!enable_rpingmesh_diag || !IsFaultDiagnosisTopology() ||
			cc_mode != RDMA_CC_MODE_RPINGMESH ||
			(fat_tree_fault_mode != 1 && fat_tree_fault_mode != 2 &&
			 fat_tree_fault_mode != 3 && fat_tree_fault_mode != 4))
		return;
	FlowCoverageKey flow = MakeCoverageFlowKey(q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->m_pg);
	RpingmeshDiagFlowState &state = rpingmeshDiagFlows[flow];
	state.tracerouteTriggered = q->m_rpingmeshTracerouteTriggered;

	if (fat_tree_fault_mode == 2){
		state.tracerouteResult = "fn";
		if (!q->m_rpingmeshTracerouteTriggered)
			return;

		std::map<FlowCoverageKey, FlowCoverageRecord>::const_iterator flowIt = flowCoverage.find(flow);
		if (flowIt == flowCoverage.end())
			return;

		for (std::map<uint32_t, uint32_t>::const_iterator it =
				q->m_rpingmeshTracerouteAckCountByCore.begin();
				it != q->m_rpingmeshTracerouteAckCountByCore.end(); ++it){
			if (it->second > 0 && !IsRpingmeshTracerouteLegalSwitch(flowIt->second, it->first)){
				state.tracerouteResult = "tp";
				return;
			}
		}
		return;
	}

	state.tracerouteFaultCore = GetRpingmeshFaultTracerouteSwitchNode();
	state.tracerouteResult = "fn";
	if (!q->m_rpingmeshTracerouteTriggered)
		return;

	if (fat_tree_fault_mode == 4){
		uint64_t maxDelayNs = 0;
		std::vector<uint32_t> maxCores;
		for (std::map<uint32_t, uint64_t>::const_iterator it =
				q->m_rpingmeshTracerouteMaxDelayByCore.begin();
				it != q->m_rpingmeshTracerouteMaxDelayByCore.end(); ++it){
			if (it->second > maxDelayNs){
				maxDelayNs = it->second;
				maxCores.clear();
				maxCores.push_back(it->first);
			}else if (it->second == maxDelayNs){
				maxCores.push_back(it->first);
			}
		}
		if (maxCores.empty())
			return;
		uint32_t selectedCore = PickRpingmeshTracerouteMinCore(maxCores);
		uint32_t selectedAckCount = 0;
		std::map<uint32_t, uint32_t>::const_iterator countIt =
			q->m_rpingmeshTracerouteAckCountByCore.find(selectedCore);
		if (countIt != q->m_rpingmeshTracerouteAckCountByCore.end())
			selectedAckCount = countIt->second;
		state.tracerouteSelectedCore = selectedCore;
		state.tracerouteSelectedCoreAckCount = selectedAckCount;
		state.tracerouteSelectedCoreMaxDelayNs = maxDelayNs;
		if (maxDelayNs > 300000 && selectedCore == state.tracerouteFaultCore)
			state.tracerouteResult = "tp";
		return;
	}

	std::vector<uint32_t> candidates;
	GetRpingmeshTracerouteCandidateSwitches(candidates);
	if (candidates.empty())
		return;

	uint32_t minAckCount = 0xffffffffu;
	std::vector<uint32_t> minCores;
	for (uint32_t i = 0; i < candidates.size(); i++){
		uint32_t coreId = candidates[i];
		uint32_t ackCount = 0;
		std::map<uint32_t, uint32_t>::const_iterator countIt =
			q->m_rpingmeshTracerouteAckCountByCore.find(coreId);
		if (countIt != q->m_rpingmeshTracerouteAckCountByCore.end())
			ackCount = countIt->second;
		if (ackCount < minAckCount){
			minAckCount = ackCount;
			minCores.clear();
			minCores.push_back(coreId);
		}else if (ackCount == minAckCount){
			minCores.push_back(coreId);
		}
	}

	uint32_t selectedCore = PickRpingmeshTracerouteMinCore(minCores);
	uint64_t selectedMaxDelayNs = 0;
	std::map<uint32_t, uint64_t>::const_iterator delayIt =
		q->m_rpingmeshTracerouteMaxDelayByCore.find(selectedCore);
	if (delayIt != q->m_rpingmeshTracerouteMaxDelayByCore.end())
		selectedMaxDelayNs = delayIt->second;
	state.tracerouteSelectedCore = selectedCore;
	state.tracerouteSelectedCoreAckCount = minAckCount;
	state.tracerouteSelectedCoreMaxDelayNs = selectedMaxDelayNs;
	state.tracerouteResult = selectedCore == state.tracerouteFaultCore ? "tp" : "fp";
}

uint32_t GetCoverageFlowHash(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport){
	union{
		struct {
			uint32_t sip, dip;
			uint16_t sport, dport;
		};
		char c[12];
	} buf;
	buf.sip = sip;
	buf.dip = dip;
	buf.sport = sport;
	buf.dport = dport;
	return Hash32(buf.c, 12);
}

void AddCoverageLink(std::set<uint64_t> &links, uint32_t src, uint32_t dst){
	if (src == dst)
		return;
	if (IsTorusTopology() && src < n.GetN() && dst < n.GetN()){
		bool srcServer = n.Get(src)->GetNodeType() == 0;
		bool dstServer = n.Get(dst)->GetNodeType() == 0;
		if (srcServer != dstServer)
			return;
	}
	links.insert(MakeCoverageLinkKey(src, dst));
}

bool GetPeerForInterface(uint32_t nodeId, uint32_t intf, uint32_t &peer){
	if (nodeId >= ifacePeer.size() || intf >= ifacePeer[nodeId].size())
		return false;
	peer = ifacePeer[nodeId][intf];
	return peer != 0xffffffffu;
}

void CollectPossibleLinksGeneral(uint32_t nodeId, uint32_t dst, std::set<uint64_t> &links, std::set<uint32_t> &expanded){
	if (nodeId == dst)
		return;
	if (!expanded.insert(nodeId).second)
		return;

	Ptr<Node> node = n.Get(nodeId);
	Ptr<Node> dstNode = n.Get(dst);
	auto nodeIt = coverageNextHop.find(node);
	if (nodeIt == coverageNextHop.end())
		return;
	auto dstIt = nodeIt->second.find(dstNode);
	if (dstIt == nodeIt->second.end())
		return;

	for (auto next : dstIt->second){
		uint32_t nextId = next->GetId();
		AddCoverageLink(links, nodeId, nextId);
		CollectPossibleLinksGeneral(nextId, dst, links, expanded);
	}
}

void CollectPossibleLinksFatTree(uint32_t src, uint32_t dst, std::set<uint64_t> &links){
	if (src == dst)
		return;

	uint32_t srcPod = FatTreeHostPod(src);
	uint32_t dstPod = FatTreeHostPod(dst);
	uint32_t srcEdge = FatTreeHostEdge(src);
	uint32_t dstEdge = FatTreeHostEdge(dst);
	uint32_t srcEdgeNode = FatTreeEdgeNode(srcPod, srcEdge);

	if (srcPod == dstPod && srcEdge == dstEdge){
		AddCoverageLink(links, src, srcEdgeNode);
		return;
	}

	if (srcPod == dstPod){
		for (uint32_t agg = 0; agg < fatTree.half; agg++){
			uint32_t aggNode = FatTreeAggNode(srcPod, agg);
			AddCoverageLink(links, srcEdgeNode, aggNode);
		}
		return;
	}

	for (uint32_t agg = 0; agg < fatTree.half; agg++){
		uint32_t srcAggNode = FatTreeAggNode(srcPod, agg);
		for (uint32_t coreCol = 0; coreCol < fatTree.half; coreCol++){
			uint32_t coreNode = FatTreeCoreNode(agg, coreCol);
			AddCoverageLink(links, srcAggNode, coreNode);
		}
	}
}

void CollectPossibleLinksLeafSpine(uint32_t src, uint32_t dst, std::set<uint64_t> &links){
	if (src == dst)
		return;

	uint32_t srcLeaf = LeafSpineHostLeaf(src);
	uint32_t dstLeaf = LeafSpineHostLeaf(dst);
	if (srcLeaf == dstLeaf){
		AddCoverageLink(links, src, srcLeaf);
		return;
	}

	for (uint32_t spine = 0; spine < leafSpine.spineCount; spine++)
		AddCoverageLink(links, srcLeaf, leafSpine.spineBase + spine);
}

bool IsFatTreeCoverageLinkForFlow(const FlowCoverageRecord &record, uint32_t linkSrc, uint32_t linkDst){
	if (record.src == record.dst)
		return false;

	uint32_t srcPod = FatTreeHostPod(record.src);
	uint32_t dstPod = FatTreeHostPod(record.dst);
	uint32_t srcEdge = FatTreeHostEdge(record.src);
	uint32_t dstEdge = FatTreeHostEdge(record.dst);
	uint32_t srcEdgeNode = FatTreeEdgeNode(srcPod, srcEdge);

	if (srcPod == dstPod && srcEdge == dstEdge)
		return linkSrc == record.src && linkDst == srcEdgeNode;

	if (srcPod == dstPod){
		if (linkSrc != srcEdgeNode || linkDst < fatTree.aggBase || linkDst >= fatTree.coreBase)
			return false;
		uint32_t aggLocal = linkDst - fatTree.aggBase;
		return aggLocal / fatTree.half == srcPod;
	}

	if (linkSrc < fatTree.aggBase || linkSrc >= fatTree.coreBase || linkDst < fatTree.coreBase)
		return false;
	uint32_t aggLocal = linkSrc - fatTree.aggBase;
	uint32_t coreLocal = linkDst - fatTree.coreBase;
	return aggLocal / fatTree.half == srcPod && coreLocal / fatTree.half == aggLocal % fatTree.half;
}

bool IsLeafSpineCoverageLinkForFlow(const FlowCoverageRecord &record, uint32_t linkSrc, uint32_t linkDst){
	if (record.src == record.dst)
		return false;

	uint32_t srcLeaf = LeafSpineHostLeaf(record.src);
	uint32_t dstLeaf = LeafSpineHostLeaf(record.dst);
	if (srcLeaf == dstLeaf)
		return linkSrc == record.src && linkDst == srcLeaf;

	return linkSrc == srcLeaf && linkDst >= leafSpine.spineBase && linkDst < leafSpine.nodeCount;
}

void AddObservedCoverageLink(FlowCoverageRecord &record, uint32_t src, uint32_t dst){
	if (topology_mode == TOPOLOGY_FAT_TREE && !IsFatTreeCoverageLinkForFlow(record, src, dst))
		return;
	if (topology_mode == TOPOLOGY_LEAF_SPINE && !IsLeafSpineCoverageLinkForFlow(record, src, dst))
		return;
	AddCoverageLink(record.observedLinks, src, dst);
}

bool PickObservedFirstHopGeneral(uint32_t src, uint32_t dst, const FlowCoverageKey &key, uint32_t &nextHopId){
	Ptr<Node> srcNode = n.Get(src);
	Ptr<Node> dstNode = n.Get(dst);
	auto nodeIt = coverageNextHop.find(srcNode);
	if (nodeIt == coverageNextHop.end())
		return false;
	auto dstIt = nodeIt->second.find(dstNode);
	if (dstIt == nodeIt->second.end() || dstIt->second.empty())
		return false;

	std::vector<uint32_t> liveNexts;
	for (auto next : dstIt->second){
		uint32_t candidate = next->GetId();
		Interface &intf = GetInterface(src, candidate);
		if (intf.up)
			liveNexts.push_back(candidate);
	}
	if (liveNexts.empty())
		return false;

	uint32_t hash = GetCoverageFlowHash(key.sip, key.dip, key.sport, key.dport);
	nextHopId = liveNexts[hash % liveNexts.size()];
	return true;
}

void RegisterFlowCoverage(uint32_t src, uint32_t dst, uint32_t pg, uint16_t sport, uint16_t dport, uint32_t packetCount){
	FlowCoverageKey key = MakeCoverageFlowKey(serverAddress[src].Get(), serverAddress[dst].Get(), sport, dport, pg);
	FlowCoverageRecord &record = flowCoverage[key];
	record.src = src;
	record.dst = dst;
	record.pg = pg;
	record.sport = sport;
	record.dport = dport;
	record.observedLinks.clear();
	record.possibleLinks.clear();

	if (topology_mode == TOPOLOGY_FAT_TREE)
		CollectPossibleLinksFatTree(src, dst, record.possibleLinks);
	else if (topology_mode == TOPOLOGY_LEAF_SPINE)
		CollectPossibleLinksLeafSpine(src, dst, record.possibleLinks);
	else{
		std::set<uint32_t> expanded;
		CollectPossibleLinksGeneral(src, dst, record.possibleLinks, expanded);
	}

	if (packetCount == 0 || src == dst)
		return;

	if (topology_mode == TOPOLOGY_FAT_TREE){
		AddObservedCoverageLink(record, src, FatTreeHostEdgeNode(src));
	}else if (topology_mode == TOPOLOGY_LEAF_SPINE){
		AddObservedCoverageLink(record, src, LeafSpineHostLeaf(src));
	}else{
		uint32_t firstHop;
		if (PickObservedFirstHopGeneral(src, dst, key, firstHop))
			AddCoverageLink(record.observedLinks, src, firstHop);
	}
}

void TrackFlowCoverageLink(CustomHeader &ch, uint32_t nodeId, uint32_t inDev, uint32_t outDev){
	if (ch.l3Prot != 0x11)
		return;

	FlowCoverageKey key = MakeCoverageFlowKey(ch.sip, ch.dip, ch.udp.sport, ch.udp.dport, ch.udp.pg);
	auto it = flowCoverage.find(key);
	if (it == flowCoverage.end())
		return;

	uint32_t peer;
	if (GetPeerForInterface(nodeId, outDev, peer))
		AddObservedCoverageLink(it->second, nodeId, peer);
}

uint64_t GetSwitchPortQlenBytes(Ptr<SwitchNode> sw, uint32_t port){
	uint64_t size = 0;
	for (uint32_t q = 0; q < SwitchMmu::qCnt; q++)
		size += sw->m_mmu->egress_bytes[port][q];
	return size;
}

void PruneSwitchLoopbackTxRecords(uint64_t nowNs){
	while (!switch_loopback_tx_records.empty() &&
			switch_loopback_tx_records.front().first + switch_rate_trace_window_ns <= nowNs){
		if (switch_loopback_tx_window_bytes >= switch_loopback_tx_records.front().second)
			switch_loopback_tx_window_bytes -= switch_loopback_tx_records.front().second;
		else
			switch_loopback_tx_window_bytes = 0;
		switch_loopback_tx_records.pop_front();
	}
}

void SampleSwitchQlenTrace(){
	if (switch_qlen_trace_output == NULL || !switch_qlen_trace_started)
		return;
	if (switch_qlen_trace_node < 0 || (uint32_t)switch_qlen_trace_node >= n.GetN())
		return;
	Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get((uint32_t)switch_qlen_trace_node));
	if (sw == NULL)
		return;

	uint64_t nowNs = Simulator::Now().GetTimeStep();
	uint64_t relativeNs = nowNs >= switch_qlen_trace_start_ns ? nowNs - switch_qlen_trace_start_ns : 0;
	uint32_t loopbackPort = sw->GetOppLoopbackPort();
	if (loopbackPort >= sw->GetNDevices())
		return;
	uint64_t loopbackBytes = GetSwitchPortQlenBytes(sw, loopbackPort);
	PruneSwitchLoopbackTxRecords(nowNs);
	long double rate700Gbps = (long double)switch_loopback_tx_window_bytes * 8.0L /
		(long double)switch_rate_trace_window_ns;

	fprintf(switch_qlen_trace_output, "%lu,%lu,%.3Lf,%d,%u,%lu,%lu,%.9Lf\n",
		nowNs, relativeNs, (long double)relativeNs / 1000.0L, switch_qlen_trace_node,
		loopbackPort, loopbackBytes, switch_loopback_tx_window_bytes, rate700Gbps);
	fflush(switch_qlen_trace_output);

	if (relativeNs + switch_qlen_trace_interval_ns <= switch_qlen_trace_duration_ns)
		Simulator::Schedule(NanoSeconds(switch_qlen_trace_interval_ns), &SampleSwitchQlenTrace);
}

void TraceSwitchQlenFirstPacket(CustomHeader &ch, uint32_t nodeId, uint32_t inDev, uint64_t timeNs){
	(void)ch;
	(void)inDev;
	if (switch_qlen_trace_output == NULL || switch_qlen_trace_started)
		return;
	if (switch_qlen_trace_node < 0 || nodeId != (uint32_t)switch_qlen_trace_node)
		return;
	switch_qlen_trace_started = true;
	switch_qlen_trace_start_ns = timeNs;
	SampleSwitchQlenTrace();
}

void TraceSwitchLoopbackTx(uint32_t nodeId, uint32_t port, uint64_t timeNs, uint32_t bytes){
	if (!opp_loopback_rate_trace_output_file.empty() &&
			opp_loopback_rate_trace_node >= 0 &&
			nodeId == (uint32_t)opp_loopback_rate_trace_node){
		(void)port;
		opp_loopback_rate_trace_tx_records.push_back(std::make_pair(timeNs, bytes));
	}
	if (switch_qlen_trace_output == NULL || !switch_qlen_trace_started)
		return;
	if (switch_qlen_trace_node < 0 || nodeId != (uint32_t)switch_qlen_trace_node)
		return;
	(void)port;
	PruneSwitchLoopbackTxRecords(timeNs);
	switch_loopback_tx_records.push_back(std::make_pair(timeNs, bytes));
	switch_loopback_tx_window_bytes += bytes;
}

bool IsRdProbeDiagPacketDeliveredToDst(CustomHeader &ch, uint32_t nodeId, uint32_t port){
	uint32_t peer;
	return GetPeerForInterface(nodeId, port, peer) && peer == ip_to_node_id(Ipv4Address(ch.dip));
}

RdProbeDiagTraceEvent MakeRdProbeDiagTraceEvent(CustomHeader &ch, uint32_t nodeId, uint32_t port,
		uint64_t timeNs, uint32_t event){
	RdProbeDiagTraceEvent trace;
	trace.timeNs = timeNs;
	trace.event = event;
	trace.nodeId = nodeId;
	trace.port = port;
	trace.sip = ch.sip;
	trace.dip = ch.dip;
	trace.sport = ch.udp.sport;
	trace.dport = ch.udp.dport;
	trace.pg = ch.udp.pg;
	trace.seq = ch.udp.seq;
	return trace;
}

void WriteRdProbeDiagTraceEvent(const RdProbeDiagTraceEvent &trace){
	if (rdprobe_diag_file == NULL)
		return;
	fprintf(rdprobe_diag_file, "%lu %c %u %u %08x %08x %u %u %u %u\n",
		trace.timeNs, trace.event == 0 ? 'I' : 'E', trace.nodeId, trace.port,
		trace.sip, trace.dip, trace.sport, trace.dport, trace.pg, trace.seq);
}

void AppendRdProbeDiagTrace(const RdProbeDiagFlowPacketKey &key, const RdProbeDiagTraceEvent &trace){
	rdProbeDiagPendingTraces[key].push_back(trace);
}

void DropRdProbeDiagTrace(const RdProbeDiagFlowPacketKey &key){
	rdProbeDiagPendingTraces.erase(key);
}

void FlushRdProbeDiagTrace(const RdProbeDiagFlowPacketKey &key){
	std::map<RdProbeDiagFlowPacketKey, std::vector<RdProbeDiagTraceEvent> >::iterator it =
		rdProbeDiagPendingTraces.find(key);
	if (it == rdProbeDiagPendingTraces.end())
		return;
	for (auto &trace : it->second)
		WriteRdProbeDiagTraceEvent(trace);
	rdProbeDiagPendingTraces.erase(it);
}

void TraceRdProbeDiag(CustomHeader &ch, uint32_t nodeId, uint32_t port, uint64_t timeNs, uint32_t event){
	if (rdprobe_diag_file == NULL || ch.l3Prot != 0x11)
		return;

	RdProbeDiagTraceEvent trace = MakeRdProbeDiagTraceEvent(ch, nodeId, port, timeNs, event);
	if (fat_tree_fault_mode < 1 || fat_tree_fault_mode > 4){
		WriteRdProbeDiagTraceEvent(trace);
		return;
	}

	RdProbeDiagPacketKey key;
	key.flow = MakeCoverageFlowKey(ch.sip, ch.dip, ch.udp.sport, ch.udp.dport, ch.udp.pg);
	key.seq = ch.udp.seq;
	key.nodeId = nodeId;
	RdProbeDiagFlowPacketKey traceKey;
	traceKey.flow = key.flow;
	traceKey.seq = key.seq;

	if (fat_tree_fault_mode == 2){
		if (rdProbeDiagFlowFoundFault.find(key.flow) != rdProbeDiagFlowFoundFault.end())
			return;
		AppendRdProbeDiagTrace(traceKey, trace);
		if (event == 0){
			std::vector<uint32_t> &path = rdProbeDiagIngressPaths[traceKey];
			path.push_back(nodeId);
			std::map<FlowCoverageKey, FlowCoverageRecord>::iterator flowIt = flowCoverage.find(key.flow);
			if (flowIt != flowCoverage.end() && !IsRdProbeIngressPathLegalPrefix(flowIt->second, path)){
				rdProbeDiagFlowFoundFault[key.flow] = true;
				FlushRdProbeDiagTrace(traceKey);
				rdProbeDiagIngressPaths.erase(traceKey);
			}
		}else if (IsRdProbeDiagPacketDeliveredToDst(ch, nodeId, port)){
			DropRdProbeDiagTrace(traceKey);
			rdProbeDiagIngressPaths.erase(traceKey);
		}
		return;
	}

	if (rdProbeDiagFlowFoundFault.find(key.flow) != rdProbeDiagFlowFoundFault.end() && fat_tree_fault_mode == 4)
		return;
	AppendRdProbeDiagTrace(traceKey, trace);

	if (event == 0){
		RdProbeDiagPacketState &state = rdProbeDiagPackets[key];
		state.ingress = true;
		state.egress = false;
		state.ingressPort = port;
		state.ingressTimeNs = timeNs;
		return;
	}

	std::map<RdProbeDiagPacketKey, RdProbeDiagPacketState>::iterator stateIt = rdProbeDiagPackets.find(key);
	if (stateIt == rdProbeDiagPackets.end()){
		if (IsRdProbeDiagPacketDeliveredToDst(ch, nodeId, port))
			DropRdProbeDiagTrace(traceKey);
		return;
	}
	bool deliveredToDst = IsRdProbeDiagPacketDeliveredToDst(ch, nodeId, port);
	if (fat_tree_fault_mode == 4){
		const RdProbeDiagPacketState &state = stateIt->second;
		if (timeNs > state.ingressTimeNs && timeNs - state.ingressTimeNs > 200000){
			rdProbeDiagFlowFoundFault[key.flow] = true;
			FlushRdProbeDiagTrace(traceKey);
		}else if (deliveredToDst){
			DropRdProbeDiagTrace(traceKey);
		}
	}else if (deliveredToDst){
		DropRdProbeDiagTrace(traceKey);
	}
	rdProbeDiagPackets.erase(stateIt);
}

void DumpFlowCoverage(){
	FILE *fout = fopen(flow_coverage_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open FLOW_COVERAGE_OUTPUT_FILE");
	for (auto &it : flowCoverage){
		FlowCoverageRecord &record = it.second;
		uint64_t observed = record.observedLinks.size();
		uint64_t possible = record.possibleLinks.size();
		double coverage = possible == 0 ? 0.0 : (double)observed / (double)possible;
		fprintf(fout, "%u %u %u %u %u %lu %lu %.6f\n",
			record.src, record.dst, record.pg, record.sport, record.dport,
			observed, possible, coverage);
	}
	fclose(fout);
}

bool IsFatTreeRdProbeIngressPathLegalPrefix(const FlowCoverageRecord &record, const std::vector<uint32_t> &path){
	if (record.src == record.dst)
		return path.empty();

	uint32_t srcPod = FatTreeHostPod(record.src);
	uint32_t dstPod = FatTreeHostPod(record.dst);
	uint32_t srcEdge = FatTreeHostEdge(record.src);
	uint32_t dstEdge = FatTreeHostEdge(record.dst);
	uint32_t srcEdgeNode = FatTreeEdgeNode(srcPod, srcEdge);
	uint32_t dstEdgeNode = FatTreeEdgeNode(dstPod, dstEdge);
	int aggIdx = -1;

	if (path.size() > 5)
		return false;
	for (uint32_t i = 0; i < path.size(); i++){
		uint32_t nodeId = path[i];
		if (srcPod == dstPod && srcEdge == dstEdge){
			if (i > 0 || nodeId != srcEdgeNode)
				return false;
			continue;
		}

		if (srcPod == dstPod){
			if (path.size() > 3)
				return false;
			if (i == 0 && nodeId != srcEdgeNode)
				return false;
			if (i == 1){
				if (nodeId < fatTree.aggBase || nodeId >= fatTree.coreBase)
					return false;
				uint32_t local = nodeId - fatTree.aggBase;
				if (local / fatTree.half != srcPod)
					return false;
			}
			if (i == 2 && nodeId != dstEdgeNode)
				return false;
			continue;
		}

		if (i == 0 && nodeId != srcEdgeNode)
			return false;
		if (i == 1){
			if (nodeId < fatTree.aggBase || nodeId >= fatTree.coreBase)
				return false;
			uint32_t local = nodeId - fatTree.aggBase;
			if (local / fatTree.half != srcPod)
				return false;
			aggIdx = local % fatTree.half;
		}
		if (i == 2){
			if (aggIdx < 0 || nodeId < fatTree.coreBase)
				return false;
			uint32_t coreLocal = nodeId - fatTree.coreBase;
			if (coreLocal / fatTree.half != (uint32_t)aggIdx)
				return false;
		}
		if (i == 3){
			if (aggIdx < 0 || nodeId < fatTree.aggBase || nodeId >= fatTree.coreBase)
				return false;
			uint32_t local = nodeId - fatTree.aggBase;
			if (local / fatTree.half != dstPod || local % fatTree.half != (uint32_t)aggIdx)
				return false;
		}
		if (i == 4 && nodeId != dstEdgeNode)
			return false;
	}
	return true;
}

bool IsLeafSpineRdProbeIngressPathLegalPrefix(const FlowCoverageRecord &record, const std::vector<uint32_t> &path){
	if (record.src == record.dst)
		return path.empty();

	uint32_t srcLeaf = LeafSpineHostLeaf(record.src);
	uint32_t dstLeaf = LeafSpineHostLeaf(record.dst);
	if (srcLeaf == dstLeaf){
		if (path.size() > 1)
			return false;
		return path.empty() || path[0] == srcLeaf;
	}

	if (path.size() > 3)
		return false;
	for (uint32_t i = 0; i < path.size(); i++){
		uint32_t nodeId = path[i];
		if (i == 0 && nodeId != srcLeaf)
			return false;
		if (i == 1 &&
				(nodeId < leafSpine.spineBase ||
				 nodeId >= leafSpine.spineBase + leafSpine.spineCount))
			return false;
		if (i == 2 && nodeId != dstLeaf)
			return false;
	}
	return true;
}

bool IsRdProbeIngressPathLegalPrefix(const FlowCoverageRecord &record, const std::vector<uint32_t> &path){
	if (topology_mode == TOPOLOGY_FAT_TREE)
		return IsFatTreeRdProbeIngressPathLegalPrefix(record, path);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		return IsLeafSpineRdProbeIngressPathLegalPrefix(record, path);
	return false;
}

void DumpRdProbeDiagnosis(){
	if (!enable_rdprobe_diag || !IsFaultDiagnosisTopology() ||
			cc_mode != RDMA_CC_MODE_RDPROBE ||
			(fat_tree_fault_mode != 1 && fat_tree_fault_mode != 2 &&
			 fat_tree_fault_mode != 3 && fat_tree_fault_mode != 4))
		return;

	uint32_t expectedFaultSwitch = GetProbeFaultSwitchNode();
	std::map<FlowCoverageKey, std::set<uint32_t> > suspectedSwitches;
	std::set<RdProbeDiagFlowPacketKey> abnormalTraceKeys;
	if (fat_tree_fault_mode == 1 || fat_tree_fault_mode == 3){
		for (auto &it : rdProbeDiagPackets){
			const RdProbeDiagPacketKey &key = it.first;
			const RdProbeDiagPacketState &state = it.second;
			if (state.ingress && !state.egress){
				suspectedSwitches[key.flow].insert(key.nodeId);
				RdProbeDiagFlowPacketKey traceKey;
				traceKey.flow = key.flow;
				traceKey.seq = key.seq;
				abnormalTraceKeys.insert(traceKey);
			}
		}
	}
	if (fat_tree_fault_mode == 2){
		for (auto &it : rdProbeDiagPendingTraces){
			rdProbeDiagFlowFoundFault[it.first.flow] = true;
			abnormalTraceKeys.insert(it.first);
		}
	}
	for (auto &traceKey : abnormalTraceKeys)
		FlushRdProbeDiagTrace(traceKey);

	FILE *fout = fopen(rdprobe_diag_result_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open RDPROBE_DIAG_RESULT_OUTPUT_FILE");
	for (auto &it : flowCoverage){
		const FlowCoverageKey &key = it.first;
		const FlowCoverageRecord &record = it.second;
		bool foundFault = false;
		if (fat_tree_fault_mode == 1){
			std::map<FlowCoverageKey, std::set<uint32_t> >::iterator suspectedIt = suspectedSwitches.find(key);
			foundFault = suspectedIt != suspectedSwitches.end() &&
				suspectedIt->second.find(expectedFaultSwitch) != suspectedIt->second.end();
		}else if (fat_tree_fault_mode == 3){
			std::map<FlowCoverageKey, std::set<uint32_t> >::iterator suspectedIt = suspectedSwitches.find(key);
			foundFault = suspectedIt != suspectedSwitches.end() && !suspectedIt->second.empty();
		}else if (fat_tree_fault_mode == 2 || fat_tree_fault_mode == 4){
			foundFault = rdProbeDiagFlowFoundFault.find(key) != rdProbeDiagFlowFoundFault.end();
		}
		fprintf(fout, "%u %u %u %u %u %u\n",
			record.src, record.dst, record.pg, record.sport, record.dport,
			foundFault ? 1 : 0);
	}
	fclose(fout);
}

void DumpRpingmeshDiagnosis(){
	if (!enable_rpingmesh_diag || !IsFaultDiagnosisTopology() ||
			cc_mode != RDMA_CC_MODE_RPINGMESH ||
			(fat_tree_fault_mode != 1 && fat_tree_fault_mode != 2 &&
			 fat_tree_fault_mode != 3 && fat_tree_fault_mode != 4))
		return;

	FILE *fout = fopen(rpingmesh_diag_result_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open RPINGMESH_DIAG_RESULT_OUTPUT_FILE");
	for (auto &it : flowCoverage){
		const FlowCoverageKey &key = it.first;
		const FlowCoverageRecord &record = it.second;
		std::map<FlowCoverageKey, RpingmeshDiagFlowState>::iterator stateIt =
			rpingmeshDiagFlows.find(key);
		uint32_t sent = 0, acked = 0, ttlExpected = 0, minTtl = 0, maxTtl = 0, ttlMismatchCount = 0;
		uint32_t tracerouteAckCount = 0;
		uint32_t tracerouteSelectedCore = 0, tracerouteSelectedCoreAckCount = 0;
		uint32_t tracerouteFaultCore = 0;
		uint64_t tracerouteSelectedCoreMaxDelayNs = 0;
		std::string tracerouteSwitchIds = "-";
		std::string tracerouteResult = "fn";
		uint64_t maxProbeAckDelayNs = 0;
		bool foundFault = false;
		if (stateIt != rpingmeshDiagFlows.end()){
			const RpingmeshDiagFlowState &state = stateIt->second;
			sent = state.sentCount;
			acked = state.ackCount;
			ttlExpected = state.ttlExpected;
			minTtl = state.minObservedTtl == 0xffffffffu ? 0 : state.minObservedTtl;
			maxTtl = state.maxObservedTtl;
			ttlMismatchCount = state.ttlMismatchCount;
			maxProbeAckDelayNs = state.maxProbeAckDelayNs;
			tracerouteAckCount = state.tracerouteAckCount;
			tracerouteSelectedCore = state.tracerouteSelectedCore;
			tracerouteSelectedCoreAckCount = state.tracerouteSelectedCoreAckCount;
			tracerouteSelectedCoreMaxDelayNs = state.tracerouteSelectedCoreMaxDelayNs;
			tracerouteFaultCore = state.tracerouteFaultCore;
			tracerouteResult = state.tracerouteResult;
			if (!state.tracerouteSwitchIds.empty()){
				std::ostringstream oss;
				bool first = true;
				for (std::set<uint32_t>::const_iterator idIt = state.tracerouteSwitchIds.begin();
						idIt != state.tracerouteSwitchIds.end(); ++idIt){
					if (!first)
						oss << ",";
					oss << *idIt;
					first = false;
				}
				tracerouteSwitchIds = oss.str();
			}
			if (fat_tree_fault_mode == 1 || fat_tree_fault_mode == 2 ||
					fat_tree_fault_mode == 3 || fat_tree_fault_mode == 4)
				foundFault = state.tracerouteTriggered;
		}
		fprintf(fout, "%u %u %u %u %u %u %u %u %lu %u %u %u %u %u %s %u %u %lu %u %s\n",
			record.src, record.dst, record.pg, record.sport, record.dport,
			foundFault ? 1 : 0, sent, acked, maxProbeAckDelayNs,
			ttlExpected, minTtl, maxTtl, ttlMismatchCount,
			tracerouteAckCount, tracerouteSwitchIds.c_str(),
			tracerouteSelectedCore, tracerouteSelectedCoreAckCount,
			tracerouteSelectedCoreMaxDelayNs, tracerouteFaultCore,
			tracerouteResult.c_str());
	}
	fclose(fout);
}

void DumpOppDiagnosis(){
	if (!enable_opp_diag || !IsFaultDiagnosisTopology() ||
			cc_mode != RDMA_CC_MODE_OPP ||
			(fat_tree_fault_mode != 1 && fat_tree_fault_mode != 2 &&
			 fat_tree_fault_mode != 3 &&
			 fat_tree_fault_mode != 4))
		return;

	FILE *fout = fopen(opp_diag_result_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open OPP_DIAG_RESULT_OUTPUT_FILE");
	for (auto &it : flowCoverage){
		const FlowCoverageKey &key = it.first;
		const FlowCoverageRecord &record = it.second;
		std::map<FlowCoverageKey, OppDiagFlowState>::iterator stateIt = oppDiagFlows.find(key);
		uint32_t ackCount = 0, faultyAckCount = 0, maxQDelay = 0;
		bool foundFault = false;
		if (stateIt != oppDiagFlows.end()){
			const OppDiagFlowState &state = stateIt->second;
			ackCount = state.ackCount;
			faultyAckCount = state.faultyAckCount;
			maxQDelay = state.maxQDelay;
			if (fat_tree_fault_mode == 1 || fat_tree_fault_mode == 3)
				foundFault = state.faultyLinkFault;
			else if (fat_tree_fault_mode == 2)
				foundFault = state.loopFault;
			else if (fat_tree_fault_mode == 4)
				foundFault = state.congestionFault;
		}
		fprintf(fout, "%u %u %u %u %u %u %u %u %u\n",
			record.src, record.dst, record.pg, record.sport, record.dport,
			foundFault ? 1 : 0, ackCount, faultyAckCount, maxQDelay);
	}
	fclose(fout);
}

void DumpSwitchProbeAckRxCounts(){
	if (!IsRpingmeshLikeCcMode(cc_mode))
		return;
	uint64_t totalProbeAckRxCount = 0;
	uint32_t switchCount = 0;
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() != 1)
			continue;
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
		switchCount++;
		totalProbeAckRxCount += sw->GetProbeAckRxCount();
		std::cout << "SWITCH_PROBE_ACK_RX_COUNT node " << i
			<< " count " << sw->GetProbeAckRxCount() << "\n";
	}
	double avgProbeAckRxCount = switchCount > 0 ? static_cast<double>(totalProbeAckRxCount) / static_cast<double>(switchCount) : 0.0;
	std::cout << "SWITCH_PROBE_ACK_RX_COUNT_TOTAL " << totalProbeAckRxCount << "\n";
	std::cout << "SWITCH_PROBE_ACK_RX_COUNT_AVG " << avgProbeAckRxCount << "\n";
}

uint64_t GetOppLoopbackPeakTxRateAfterOffset(
		const std::vector<SwitchNode::OppLoopbackTxWindowRecord> &records,
		uint64_t intervalNs, uint64_t offsetNs){
	if (records.empty())
		return 0;
	uint64_t thresholdNs = records[0].startNs + offsetNs;
	uint64_t peakRateBps = 0;
	for (uint32_t i = 0; i < records.size(); i++){
		const SwitchNode::OppLoopbackTxWindowRecord &record = records[i];
		if (record.startNs < thresholdNs)
			continue;
		uint64_t rateBps = (uint64_t)((long double)record.bytes * 8.0e9 / (long double)intervalNs);
		if (rateBps > peakRateBps)
			peakRateBps = rateBps;
	}
	return peakRateBps;
}

void DumpOppLoopbackStats(){
	if (cc_mode != RDMA_CC_MODE_OPP)
		return;
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() != 1)
			continue;
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
		uint32_t port = sw->GetOppLoopbackPort();
		if (port >= sw->GetNDevices())
			continue;
		sw->FinalizeOppLoopbackStats();
		uint64_t peakRate10usBps = sw->GetOppLoopbackPeakTxRate10usBps();
		uint64_t peakRate1usBps = sw->GetOppLoopbackPeakTxRate1usBps();
		uint64_t peakRate700nsBps = sw->GetOppLoopbackPeakTxRate700nsBps();
		uint64_t peakRate100nsBps = sw->GetOppLoopbackPeakTxRate100nsBps();
		uint64_t peakRate700nsAfter200usBps = GetOppLoopbackPeakTxRateAfterOffset(
			sw->GetOppLoopbackTxWindows700ns(),
			SwitchNode::GetOppLoopbackTxSampleInterval700nsNs(),
			200000);
		double peakRate10usGbps = (double)peakRate10usBps / 1e9;
		double peakRate1usGbps = (double)peakRate1usBps / 1e9;
		double peakRate700nsGbps = (double)peakRate700nsBps / 1e9;
		double peakRate100nsGbps = (double)peakRate100nsBps / 1e9;
		double peakRate700nsAfter200usGbps = (double)peakRate700nsAfter200usBps / 1e9;
		std::cout << "OPP_LOOPBACK_STATS node " << i
			<< " port " << port
			<< " rx_probe_count " << sw->GetOppLoopbackRxProbeCount()
			<< " peak_tx_rate_bps " << peakRate10usBps
			<< " peak_tx_rate_gbps " << peakRate10usGbps
			<< " peak_tx_rate_10us_bps " << peakRate10usBps
			<< " peak_tx_rate_10us_gbps " << peakRate10usGbps
			<< " peak_tx_rate_1us_bps " << peakRate1usBps
			<< " peak_tx_rate_1us_gbps " << peakRate1usGbps
			<< " peak_tx_rate_700ns_bps " << peakRate700nsBps
			<< " peak_tx_rate_700ns_gbps " << peakRate700nsGbps
			<< " peak_tx_rate_700ns_after_200us_bps " << peakRate700nsAfter200usBps
			<< " peak_tx_rate_700ns_after_200us_gbps " << peakRate700nsAfter200usGbps
			<< " peak_tx_rate_100ns_bps " << peakRate100nsBps
			<< " peak_tx_rate_100ns_gbps " << peakRate100nsGbps
			<< " peak_queue_bytes " << sw->GetOppLoopbackPeakQueueBytes()
			<< " sample_interval_ns " << SwitchNode::GetOppLoopbackTxSampleIntervalNs()
			<< " sample_interval_10us_ns " << SwitchNode::GetOppLoopbackTxSampleInterval10usNs()
			<< " sample_interval_1us_ns " << SwitchNode::GetOppLoopbackTxSampleInterval1usNs()
			<< " sample_interval_700ns_ns " << SwitchNode::GetOppLoopbackTxSampleInterval700nsNs()
			<< " sample_interval_100ns_ns " << SwitchNode::GetOppLoopbackTxSampleInterval100nsNs()
			<< "\n";
	}
	uint64_t usmCopyCount = SwitchNode::GetOppUsmCopyStatsCount();
	uint64_t usmCopyCompleted = SwitchNode::GetOppUsmCopyStatsCompletedCount();
	uint64_t usmCopySumNs = SwitchNode::GetOppUsmCopyStatsSumNs();
	double usmCopyAvgNs = usmCopyCompleted > 0 ?
		(double)usmCopySumNs / (double)usmCopyCompleted : 0.0;
	std::cout << "OPP_USM_COPY_STATS"
		<< " count " << usmCopyCount
		<< " completed " << usmCopyCompleted
		<< " min_ns " << SwitchNode::GetOppUsmCopyStatsMinNs()
		<< " max_ns " << SwitchNode::GetOppUsmCopyStatsMaxNs()
		<< " sum_ns " << usmCopySumNs
		<< " avg_ns " << usmCopyAvgNs
		<< "\n";
}

struct OppLoopbackPeakBucketRecord {
	bool valid;
	uint32_t nodeId;
	uint64_t rateBps;
	uint64_t windowStartRelativeNs;
	uint64_t windowStartNs;
	uint64_t windowBytes;

	OppLoopbackPeakBucketRecord()
		: valid(false), nodeId(0), rateBps(0), windowStartRelativeNs(0),
		  windowStartNs(0), windowBytes(0) {}
};

bool GetOppLoopbackGlobalStartNs(uint64_t &globalStartNs){
	bool hasGlobalStart = false;
	globalStartNs = 0;
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() != 1)
			continue;
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
		uint32_t port = sw->GetOppLoopbackPort();
		if (port >= sw->GetNDevices())
			continue;
		sw->FinalizeOppLoopbackStats();
		const std::vector<SwitchNode::OppLoopbackTxWindowRecord> &records =
			sw->GetOppLoopbackTxWindows700ns();
		if (records.empty())
			continue;
		if (!hasGlobalStart || records[0].startNs < globalStartNs){
			hasGlobalStart = true;
			globalStartNs = records[0].startNs;
		}
	}
	return hasGlobalStart;
}

void DumpOppLoopbackPeakBuckets(){
	if (cc_mode != RDMA_CC_MODE_OPP || opp_loopback_peak_bucket_output_file.empty())
		return;
	NS_ASSERT_MSG(opp_loopback_peak_bucket_interval_ns > 0,
		"OPP_LOOPBACK_PEAK_BUCKET_INTERVAL_NS must be positive");

	// Keep bucket boundaries shared across switches.
	uint64_t globalStartNs = 0;
	bool hasGlobalStart = GetOppLoopbackGlobalStartNs(globalStartNs);

	std::vector<OppLoopbackPeakBucketRecord> buckets;
	if (!hasGlobalStart)
		buckets.clear();
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() != 1)
			continue;
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
		uint32_t port = sw->GetOppLoopbackPort();
		if (port >= sw->GetNDevices())
			continue;
		sw->FinalizeOppLoopbackStats();

		const std::vector<SwitchNode::OppLoopbackTxWindowRecord> &records =
			sw->GetOppLoopbackTxWindows700ns();
		if (records.empty())
			continue;
		uint64_t intervalNs = SwitchNode::GetOppLoopbackTxSampleInterval700nsNs();
		for (uint32_t j = 0; j < records.size(); j++){
			const SwitchNode::OppLoopbackTxWindowRecord &record = records[j];
			uint64_t relativeNs = record.startNs - globalStartNs;
			uint64_t bucketIdx = relativeNs / opp_loopback_peak_bucket_interval_ns;
			if (bucketIdx >= buckets.size())
				buckets.resize(bucketIdx + 1);
			uint64_t rateBps = (uint64_t)((long double)record.bytes * 8.0e9 / (long double)intervalNs);
			OppLoopbackPeakBucketRecord &bucket = buckets[bucketIdx];
			if (!bucket.valid || rateBps > bucket.rateBps){
				bucket.valid = true;
				bucket.nodeId = i;
				bucket.rateBps = rateBps;
				bucket.windowStartRelativeNs = relativeNs;
				bucket.windowStartNs = record.startNs;
				bucket.windowBytes = record.bytes;
			}
		}
	}

	FILE *fout = fopen(opp_loopback_peak_bucket_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open OPP_LOOPBACK_PEAK_BUCKET_OUTPUT_FILE");
	fprintf(fout, "bucket_index,bucket_start_relative_ns,bucket_end_relative_ns,bucket_start_relative_us,bucket_end_relative_us,node,peak_tx_rate_700ns_bps,peak_tx_rate_700ns_gbps,window_start_relative_ns,window_start_relative_us,window_start_ns,window_bytes\n");
	for (uint32_t i = 0; i < buckets.size(); i++){
		uint64_t bucketStartNs = (uint64_t)i * opp_loopback_peak_bucket_interval_ns;
		uint64_t bucketEndNs = bucketStartNs + opp_loopback_peak_bucket_interval_ns;
		const OppLoopbackPeakBucketRecord &bucket = buckets[i];
		int32_t node = bucket.valid ? (int32_t)bucket.nodeId : -1;
		double rateGbps = (double)bucket.rateBps / 1e9;
		fprintf(fout, "%u,%lu,%lu,%.3f,%.3f,%d,%lu,%.9f,%lu,%.3f,%lu,%lu\n",
			i, bucketStartNs, bucketEndNs,
			(double)bucketStartNs / 1000.0, (double)bucketEndNs / 1000.0,
			node, bucket.rateBps, rateGbps, bucket.windowStartRelativeNs,
			(double)bucket.windowStartRelativeNs / 1000.0,
			bucket.windowStartNs, bucket.windowBytes);
	}
	fclose(fout);
}

void DumpOppLoopbackRateTrace(){
	if (cc_mode != RDMA_CC_MODE_OPP || opp_loopback_rate_trace_output_file.empty())
		return;
	NS_ASSERT_MSG(opp_loopback_rate_trace_node >= 0,
		"OPP_LOOPBACK_RATE_TRACE_NODE must be set when OPP_LOOPBACK_RATE_TRACE_OUTPUT_FILE is set");
	NS_ASSERT_MSG((uint32_t)opp_loopback_rate_trace_node < n.GetN(),
		"OPP_LOOPBACK_RATE_TRACE_NODE is out of range");
	NS_ASSERT_MSG(n.Get((uint32_t)opp_loopback_rate_trace_node)->GetNodeType() == 1,
		"OPP_LOOPBACK_RATE_TRACE_NODE must be a switch");
	NS_ASSERT_MSG(opp_loopback_rate_trace_interval_ns > 0,
		"OPP_LOOPBACK_RATE_TRACE_INTERVAL_NS must be positive");
	NS_ASSERT_MSG(opp_loopback_rate_trace_end_relative_ns >
		opp_loopback_rate_trace_start_relative_ns,
		"OPP_LOOPBACK_RATE_TRACE_END_NS must be greater than OPP_LOOPBACK_RATE_TRACE_START_NS");

	Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(
		n.Get((uint32_t)opp_loopback_rate_trace_node));
	uint32_t loopbackPort = sw->GetOppLoopbackPort();
	NS_ASSERT_MSG(loopbackPort < sw->GetNDevices(),
		"OPP_LOOPBACK_RATE_TRACE_NODE must have an OPP loopback port");

	uint64_t globalStartNs = 0;
	bool hasGlobalStart = GetOppLoopbackGlobalStartNs(globalStartNs);
	FILE *fout = fopen(opp_loopback_rate_trace_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open OPP_LOOPBACK_RATE_TRACE_OUTPUT_FILE");
	fprintf(fout, "global_start_ns,relative_bin_start_ns,relative_bin_end_ns,relative_bin_start_us,relative_bin_end_us,sim_bin_start_ns,sim_bin_end_ns,node,loopback_port,bytes,rate_bps,rate_gbps\n");
	if (!hasGlobalStart){
		fclose(fout);
		return;
	}

	size_t eventIdx = 0;
	for (uint64_t binStartRelativeNs = opp_loopback_rate_trace_start_relative_ns;
			binStartRelativeNs + opp_loopback_rate_trace_interval_ns <=
				opp_loopback_rate_trace_end_relative_ns;
			binStartRelativeNs += opp_loopback_rate_trace_interval_ns){
		uint64_t binEndRelativeNs = binStartRelativeNs +
			opp_loopback_rate_trace_interval_ns;
		uint64_t binStartNs = globalStartNs + binStartRelativeNs;
		uint64_t binEndNs = globalStartNs + binEndRelativeNs;
		while (eventIdx < opp_loopback_rate_trace_tx_records.size() &&
				opp_loopback_rate_trace_tx_records[eventIdx].first < binStartNs)
			eventIdx++;
		uint64_t bytes = 0;
		size_t scanIdx = eventIdx;
		while (scanIdx < opp_loopback_rate_trace_tx_records.size() &&
				opp_loopback_rate_trace_tx_records[scanIdx].first < binEndNs){
			bytes += opp_loopback_rate_trace_tx_records[scanIdx].second;
			scanIdx++;
		}
		eventIdx = scanIdx;
		long double rateBpsDouble = (long double)bytes * 8.0e9L /
			(long double)opp_loopback_rate_trace_interval_ns;
		uint64_t rateBps = rateBpsDouble >
			(long double)std::numeric_limits<uint64_t>::max() ?
			std::numeric_limits<uint64_t>::max() : (uint64_t)rateBpsDouble;
		long double rateGbps = (long double)bytes * 8.0L /
			(long double)opp_loopback_rate_trace_interval_ns;
		fprintf(fout, "%lu,%lu,%lu,%.3Lf,%.3Lf,%lu,%lu,%d,%u,%lu,%lu,%.9Lf\n",
			globalStartNs,
			binStartRelativeNs,
			binEndRelativeNs,
			(long double)binStartRelativeNs / 1000.0L,
			(long double)binEndRelativeNs / 1000.0L,
			binStartNs,
			binEndNs,
			opp_loopback_rate_trace_node,
			loopbackPort,
			bytes,
			rateBps,
			rateGbps);
	}
	fclose(fout);
}

bool GetFatTreeSwitchPodAndRole(uint32_t nodeId, uint32_t &pod, const char *&role){
	if (topology_mode != TOPOLOGY_FAT_TREE || fat_tree_k == 0)
		return false;
	uint32_t half = fat_tree_k / 2;
	uint32_t hostCount = fat_tree_k * fat_tree_k * fat_tree_k / 4;
	uint32_t edgeCount = fat_tree_k * half;
	uint32_t aggCount = edgeCount;
	uint32_t edgeBase = hostCount;
	uint32_t aggBase = edgeBase + edgeCount;
	uint32_t coreBase = aggBase + aggCount;
	if (nodeId >= edgeBase && nodeId < aggBase){
		pod = (nodeId - edgeBase) / half;
		role = "tor";
		return true;
	}
	if (nodeId >= aggBase && nodeId < coreBase){
		pod = (nodeId - aggBase) / half;
		role = "agg";
		return true;
	}
	if (nodeId >= coreBase){
		pod = 0xffffffffu;
		role = "core";
		return true;
	}
	return false;
}

void DumpOppLoopbackWindowRecords(FILE *fout, uint32_t nodeId, uint32_t port,
		const char *role, uint32_t pod, uint64_t intervalNs,
		const std::vector<SwitchNode::OppLoopbackTxWindowRecord> &records){
	for (uint32_t i = 0; i < records.size(); i++){
		const SwitchNode::OppLoopbackTxWindowRecord &record = records[i];
		double rateGbps = (double)record.bytes * 8.0 / (double)intervalNs;
		fprintf(fout, "%u,%u,%s,%u,%lu,%lu,%lu,%.9f\n",
			nodeId, port, role, pod, intervalNs, record.startNs, record.bytes, rateGbps);
	}
}

void DumpOppLoopbackTimeseries(){
	if (cc_mode != RDMA_CC_MODE_OPP || opp_loopback_timeseries_output_file.empty())
		return;
	FILE *fout = fopen(opp_loopback_timeseries_output_file.c_str(), "w");
	NS_ASSERT_MSG(fout != NULL, "failed to open OPP_LOOPBACK_TIMESERIES_OUTPUT_FILE");
	fprintf(fout, "node,port,role,pod,interval_ns,window_start_ns,bytes,rate_gbps\n");
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() != 1)
			continue;
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
		uint32_t port = sw->GetOppLoopbackPort();
		if (port >= sw->GetNDevices())
			continue;
		uint32_t pod = 0xffffffffu;
		const char *role = "switch";
		bool hasFatTreeRole = GetFatTreeSwitchPodAndRole(i, pod, role);
		if (opp_loopback_timeseries_pod >= 0){
			if (!hasFatTreeRole || pod != (uint32_t)opp_loopback_timeseries_pod)
				continue;
		}
		sw->FinalizeOppLoopbackStats();
		DumpOppLoopbackWindowRecords(fout, i, port, role, pod,
			SwitchNode::GetOppLoopbackTxSampleInterval10usNs(),
			sw->GetOppLoopbackTxWindows10us());
		DumpOppLoopbackWindowRecords(fout, i, port, role, pod,
			SwitchNode::GetOppLoopbackTxSampleInterval1usNs(),
			sw->GetOppLoopbackTxWindows1us());
	}
	fclose(fout);
}

void qp_finish(FILE* fout, Ptr<RdmaQueuePair> q){
	uint32_t sid = ip_to_node_id(q->sip), did = ip_to_node_id(q->dip);
	FinalizeRpingmeshTracerouteDiagnosis(q);
	PairInfo pair = GetPairInfo(sid, did);
	uint64_t base_rtt = pair.rtt, b = pair.bw;
	uint64_t total_bytes;
	if (q->m_isRpingmesh)
		total_bytes = q->m_size * RPINGMESH_PACKET_SIZE;
	else
		total_bytes = q->m_size + ((q->m_size-1) / packet_payload_size + 1) * (CustomHeader::GetStaticWholeHeaderSize() - IntHeader::GetStaticSize()); // translate to the minimum bytes required (with header but no INT)
	uint64_t standalone_fct = base_rtt + total_bytes * 8000000000lu / b;
	// sip, dip, sport, dport, size (B), start_time, fct (ns), standalone_fct (ns)
	fprintf(fout, "%08x %08x %u %u %lu %lu %lu %lu\n", q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->m_size, q->startTime.GetTimeStep(), (Simulator::Now() - q->startTime).GetTimeStep(), standalone_fct);
	fflush(fout);

	if (cc_mode == RDMA_CC_MODE_OPP && opp_probe_delay_output != NULL){
		fprintf(opp_probe_delay_output, "%08x,%08x,%u,%u,%u,%lu,",
			q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->m_pg, q->m_size);
		if (q->m_oppProbeFirstSendObserved)
			fprintf(opp_probe_delay_output, "%lu,", q->m_oppProbeFirstSendTimeNs);
		else
			fprintf(opp_probe_delay_output, ",");
		if (q->m_oppProbeLastAckObserved){
			uint64_t delayNs = q->m_oppProbeLastAckTimeNs >= q->m_oppProbeFirstSendTimeNs ?
				q->m_oppProbeLastAckTimeNs - q->m_oppProbeFirstSendTimeNs : 0;
			fprintf(opp_probe_delay_output, "%lu,%lu,1\n", q->m_oppProbeLastAckTimeNs, delayNs);
		}else{
			fprintf(opp_probe_delay_output, ",,0\n");
		}
		fflush(opp_probe_delay_output);
	}

	// remove rxQp from the receiver
	Ptr<Node> dstNode = n.Get(did);
	Ptr<RdmaDriver> rdma = dstNode->GetObject<RdmaDriver> ();
	rdma->m_rdma->DeleteRxQp(q->sip.Get(), q->m_pg, q->sport);
}

void get_pfc(FILE* fout, Ptr<QbbNetDevice> dev, uint32_t type){
	fprintf(fout, "%lu %u %u %u %u\n", Simulator::Now().GetTimeStep(), dev->GetNode()->GetId(), dev->GetNode()->GetNodeType(), dev->GetIfIndex(), type);
}

struct QlenDistribution{
	vector<uint32_t> cnt; // cnt[i] is the number of times that the queue len is i KB

	void add(uint32_t qlen){
		uint32_t kb = qlen / 1000;
		if (cnt.size() < kb+1)
			cnt.resize(kb+1);
		cnt[kb]++;
	}
};
map<uint32_t, map<uint32_t, QlenDistribution> > queue_result;
void monitor_buffer(FILE* qlen_output, NodeContainer *n){
	for (uint32_t i = 0; i < n->GetN(); i++){
		if (n->Get(i)->GetNodeType() == 1){ // is switch
			Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n->Get(i));
			if (queue_result.find(i) == queue_result.end())
				queue_result[i];
			for (uint32_t j = 1; j < sw->GetNDevices(); j++){
				uint32_t size = 0;
				for (uint32_t k = 0; k < SwitchMmu::qCnt; k++)
					size += sw->m_mmu->egress_bytes[j][k];
				queue_result[i][j].add(size);
			}
		}
	}
	if (Simulator::Now().GetTimeStep() % qlen_dump_interval == 0){
		fprintf(qlen_output, "time: %lu\n", Simulator::Now().GetTimeStep());
		for (auto &it0 : queue_result)
			for (auto &it1 : it0.second){
				fprintf(qlen_output, "%u %u", it0.first, it1.first);
				auto &dist = it1.second.cnt;
				for (uint32_t i = 0; i < dist.size(); i++)
					fprintf(qlen_output, " %u", dist[i]);
				fprintf(qlen_output, "\n");
			}
		fflush(qlen_output);
	}
	if (Simulator::Now().GetTimeStep() < qlen_mon_end)
		Simulator::Schedule(NanoSeconds(qlen_mon_interval), &monitor_buffer, qlen_output, n);
}

void CalculateRoute(Ptr<Node> host){
	// queue for the BFS.
	vector<Ptr<Node> > q;
	// Distance from the host to each node.
	map<Ptr<Node>, int> dis;
	map<Ptr<Node>, uint64_t> delay;
	map<Ptr<Node>, uint64_t> txDelay;
	map<Ptr<Node>, uint64_t> bw;
	map<Ptr<Node>, uint32_t> switchCnt;
	// init BFS.
	q.push_back(host);
	dis[host] = 0;
	delay[host] = 0;
	txDelay[host] = 0;
	bw[host] = 0xfffffffffffffffflu;
	switchCnt[host] = 0;
	// BFS.
	for (int i = 0; i < (int)q.size(); i++){
		Ptr<Node> now = q[i];
		int d = dis[now];
		for (auto it = nbr2if[now].begin(); it != nbr2if[now].end(); it++){
			// skip down link
			if (!it->second.up)
				continue;
			Ptr<Node> next = it->first;
			// If 'next' have not been visited.
			if (dis.find(next) == dis.end()){
				dis[next] = d + 1;
				delay[next] = delay[now] + it->second.delay;
				txDelay[next] = txDelay[now] + packet_payload_size * 1000000000lu * 8 / it->second.bw;
				bw[next] = std::min(bw[now], it->second.bw);
				switchCnt[next] = switchCnt[now] + (next->GetNodeType() == 1 ? 1 : 0);
				// we only enqueue switch, because we do not want packets to go through host as middle point
				if (next->GetNodeType() == 1)
					q.push_back(next);
			}
			// if 'now' is on the shortest path from 'next' to 'host'.
			if (d + 1 == dis[next]){
				nextHop[next][host].push_back(now);
			}
		}
	}
	for (auto it : delay)
		pairDelay[it.first][host] = it.second;
	for (auto it : txDelay)
		pairTxDelay[it.first][host] = it.second;
	for (auto it : bw)
		pairBw[it.first->GetId()][host->GetId()] = it.second;
	for (auto it : switchCnt)
		pairSwitchCount[it.first->GetId()][host->GetId()] = it.second;
}

void CalculateRoutes(NodeContainer &n){
	for (int i = 0; i < (int)n.GetN(); i++){
		Ptr<Node> node = n.Get(i);
		if (node->GetNodeType() == 0)
			CalculateRoute(node);
	}
}

void SetRoutingEntries(){
	// For each node.
	for (auto i = nextHop.begin(); i != nextHop.end(); i++){
		Ptr<Node> node = i->first;
		auto &table = i->second;
		for (auto j = table.begin(); j != table.end(); j++){
			// The destination node.
			Ptr<Node> dst = j->first;
			// The IP address of the dst.
			Ipv4Address dstAddr = dst->GetObject<Ipv4>()->GetAddress(1, 0).GetLocal();
			// The next hops towards the dst.
			vector<Ptr<Node> > nexts = j->second;
			for (int k = 0; k < (int)nexts.size(); k++){
				Ptr<Node> next = nexts[k];
				uint32_t interface = nbr2if[node][next].idx;
				if (node->GetNodeType() == 1)
					DynamicCast<SwitchNode>(node)->AddTableEntry(dstAddr, interface);
				else{
					node->GetObject<RdmaDriver>()->m_rdma->AddTableEntry(dstAddr, interface);
				}
			}
		}
	}
}

void SetFatTreeRoutingEntries(){
	for (uint32_t host = 0; host < fatTree.hostCount; host++){
		uint32_t edge = FatTreeHostEdgeNode(host);
		uint32_t interface = GetInterface(host, edge).idx;
		n.Get(host)->GetObject<RdmaDriver>()->m_rdma->AddDefaultRoute(interface);
	}

	for (uint32_t pod = 0; pod < fatTree.k; pod++){
		for (uint32_t edgeIdx = 0; edgeIdx < fatTree.half; edgeIdx++){
			uint32_t edgeNodeId = FatTreeEdgeNode(pod, edgeIdx);
			std::vector<int> downPorts(fatTree.hostsPerEdge, -1), upPorts(fatTree.half, -1);
			for (uint32_t h = 0; h < fatTree.hostsPerEdge; h++){
				uint32_t host = pod * fatTree.hostsPerPod + edgeIdx * fatTree.hostsPerEdge + h;
				downPorts[h] = GetInterface(edgeNodeId, host).idx;
			}
			for (uint32_t aggIdx = 0; aggIdx < fatTree.half; aggIdx++)
				upPorts[aggIdx] = GetInterface(edgeNodeId, FatTreeAggNode(pod, aggIdx)).idx;
			Ptr<FatTreeSwitchRouteProvider> provider = CreateObject<FatTreeSwitchRouteProvider>();
			provider->Configure(fatTree.k, 1, pod, edgeIdx, downPorts, upPorts, opp_fattree_m1, opp_fattree_m2, opp_fattree_m3, opp_probe_instance_count);
			DynamicCast<SwitchNode>(n.Get(edgeNodeId))->SetRouteProvider(provider, upPorts);
		}

		for (uint32_t aggIdx = 0; aggIdx < fatTree.half; aggIdx++){
			uint32_t aggNodeId = FatTreeAggNode(pod, aggIdx);
			std::vector<int> downPorts(fatTree.half, -1), upPorts(fatTree.half, -1);
			for (uint32_t edgeIdx = 0; edgeIdx < fatTree.half; edgeIdx++)
				downPorts[edgeIdx] = GetInterface(aggNodeId, FatTreeEdgeNode(pod, edgeIdx)).idx;
			for (uint32_t coreCol = 0; coreCol < fatTree.half; coreCol++)
				upPorts[coreCol] = GetInterface(aggNodeId, FatTreeCoreNode(aggIdx, coreCol)).idx;
			Ptr<FatTreeSwitchRouteProvider> provider = CreateObject<FatTreeSwitchRouteProvider>();
			provider->Configure(fatTree.k, 2, pod, aggIdx, downPorts, upPorts, opp_fattree_m1, opp_fattree_m2, opp_fattree_m3, opp_probe_instance_count);
			Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(aggNodeId));
			sw->SetRouteProvider(provider, upPorts);
			if (fat_tree_fault_mode != 0 && pod == 0 && aggIdx == 0){
				NS_ASSERT_MSG(!upPorts.empty() && upPorts[0] >= 0, "fat-tree fault target AGG-Core port is missing");
				sw->ConfigureFatTreeFault(fat_tree_fault_mode, (uint32_t)upPorts[0], fat_tree_fault_seed, fat_tree_fault_congestion_delay_ns);
			}
		}
	}

	for (uint32_t group = 0; group < fatTree.half; group++){
		for (uint32_t col = 0; col < fatTree.half; col++){
			uint32_t coreNodeId = FatTreeCoreNode(group, col);
			std::vector<int> downPorts(fatTree.k, -1), upPorts;
			for (uint32_t pod = 0; pod < fatTree.k; pod++)
				downPorts[pod] = GetInterface(coreNodeId, FatTreeAggNode(pod, group)).idx;
			Ptr<FatTreeSwitchRouteProvider> provider = CreateObject<FatTreeSwitchRouteProvider>();
			provider->Configure(fatTree.k, 3, 0, group, downPorts, upPorts, opp_fattree_m1, opp_fattree_m2, opp_fattree_m3, opp_probe_instance_count);
			DynamicCast<SwitchNode>(n.Get(coreNodeId))->SetRouteProvider(provider, upPorts);
		}
	}
}

void SetLeafSpineRoutingEntries(){
	for (uint32_t host = 0; host < leafSpine.hostCount; host++){
		uint32_t leaf = LeafSpineHostLeaf(host);
		uint32_t interface = GetInterface(host, leaf).idx;
		n.Get(host)->GetObject<RdmaDriver>()->m_rdma->AddDefaultRoute(interface);
	}

	for (uint32_t leafIdx = 0; leafIdx < leafSpine.leafCount; leafIdx++){
		uint32_t leafNodeId = LeafSpineLeafNode(leafIdx);
		std::vector<int> downPorts(leafSpine.half, -1), upPorts(leafSpine.spineCount, -1);
		for (uint32_t h = 0; h < leafSpine.half; h++){
			uint32_t host = leafIdx * leafSpine.half + h;
			downPorts[h] = GetInterface(leafNodeId, host).idx;
		}
		for (uint32_t spineIdx = 0; spineIdx < leafSpine.spineCount; spineIdx++)
			upPorts[spineIdx] = GetInterface(leafNodeId, LeafSpineSpineNode(spineIdx)).idx;
		Ptr<LeafSpineSwitchRouteProvider> provider = CreateObject<LeafSpineSwitchRouteProvider>();
		provider->Configure(leafSpine.k, 1, leafIdx, downPorts, upPorts, opp_leafspine_m1, opp_leafspine_m2, opp_probe_instance_count);
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(leafNodeId));
		sw->SetRouteProvider(provider, upPorts);
		if (fat_tree_fault_mode != 0 && fat_tree_fault_mode != 2 && leafIdx == 0){
			NS_ASSERT_MSG(!upPorts.empty() && upPorts[0] >= 0, "leaf-spine fault target Leaf-Spine port is missing");
			sw->ConfigureFatTreeFault(fat_tree_fault_mode, (uint32_t)upPorts[0],
				fat_tree_fault_seed, fat_tree_fault_congestion_delay_ns);
		}else if (fat_tree_fault_mode == 2 && leafIdx != 0){
			NS_ASSERT_MSG(!upPorts.empty() && upPorts[0] >= 0, "leaf-spine loop fault target Spine0 ingress port is missing");
			sw->ConfigureFatTreeFault(fat_tree_fault_mode, 0xffffffffu,
				fat_tree_fault_seed, fat_tree_fault_congestion_delay_ns, (uint32_t)upPorts[0]);
		}
	}

	for (uint32_t spineIdx = 0; spineIdx < leafSpine.spineCount; spineIdx++){
		uint32_t spineNodeId = LeafSpineSpineNode(spineIdx);
		std::vector<int> downPorts(leafSpine.leafCount, -1), upPorts;
		for (uint32_t leafIdx = 0; leafIdx < leafSpine.leafCount; leafIdx++)
			downPorts[leafIdx] = GetInterface(spineNodeId, LeafSpineLeafNode(leafIdx)).idx;
		Ptr<LeafSpineSwitchRouteProvider> provider = CreateObject<LeafSpineSwitchRouteProvider>();
		provider->Configure(leafSpine.k, 2, spineIdx, downPorts, upPorts, opp_leafspine_m1, opp_leafspine_m2, opp_probe_instance_count);
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(spineNodeId));
		sw->SetRouteProvider(provider, upPorts);
	}
}

// take down the link between a and b, and redo the routing
void TakeDownLink(NodeContainer n, Ptr<Node> a, Ptr<Node> b){
	Interface &a2b = GetInterface(a->GetId(), b->GetId());
	Interface &b2a = GetInterface(b->GetId(), a->GetId());
	if (!a2b.up)
		return;
	// take down link between a and b
	a2b.up = b2a.up = false;
	DynamicCast<QbbNetDevice>(a->GetDevice(a2b.idx))->TakeDown();
	DynamicCast<QbbNetDevice>(b->GetDevice(b2a.idx))->TakeDown();
	if (topology_mode == TOPOLOGY_FAT_TREE || topology_mode == TOPOLOGY_LEAF_SPINE){
		for (uint32_t i = 0; i < n.GetN(); i++){
			if (n.Get(i)->GetNodeType() == 0)
				n.Get(i)->GetObject<RdmaDriver>()->m_rdma->RedistributeQp();
		}
		return;
	}
	nextHop.clear();
	CalculateRoutes(n);
	// clear routing tables
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() == 1)
			DynamicCast<SwitchNode>(n.Get(i))->ClearTable();
		else
			n.Get(i)->GetObject<RdmaDriver>()->m_rdma->ClearTable();
	}
	// reset routing table
	SetRoutingEntries();

	// redistribute qp on each host
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() == 0)
			n.Get(i)->GetObject<RdmaDriver>()->m_rdma->RedistributeQp();
	}
}

uint64_t get_nic_rate(NodeContainer &n){
	for (uint32_t i = 0; i < n.GetN(); i++)
		if (n.Get(i)->GetNodeType() == 0)
			return DynamicCast<QbbNetDevice>(n.Get(i)->GetDevice(1))->GetDataRate().GetBitRate();
}

DataRate GetFirstPhysicalSwitchDataRate(Ptr<SwitchNode> sw){
	for (uint32_t j = 1; j < sw->GetNDevices(); j++){
		Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(sw->GetDevice(j));
		if (dev != NULL && !dev->IsSwitchLoopback() && dev->GetChannel() != NULL)
			return dev->GetDataRate();
	}
	NS_ASSERT_MSG(false, "OPP USM requires at least one physical QbbNetDevice on each switch");
	return DataRate("100Gbps");
}

void InstallOppLoopbackDevices(NodeContainer &n){
	for (uint32_t i = 0; i < n.GetN(); i++){
		if (n.Get(i)->GetNodeType() != 1)
			continue;
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
		sw->InstallOppLoopbackDevice(GetFirstPhysicalSwitchDataRate(sw));
	}
}

int main(int argc, char *argv[])
{
	clock_t begint, endt;
	begint = clock();
	init_start_time = begint;
	init_start_wall_us = InitNowUs();
#ifndef PGO_TRAINING
	if (argc > 1)
#else
	if (true)
#endif
	{
		//Read the configuration file
		std::ifstream conf;
#ifndef PGO_TRAINING
		std::string configPath = argv[1];
		const std::string configPrefix = "--config=";
		if (configPath.compare(0, configPrefix.size(), configPrefix) == 0)
			configPath = configPath.substr(configPrefix.size());
#else
		std::string configPath = PATH_TO_PGO_CONFIG;
#endif
		conf.open(configPath.c_str());
		if (!conf.is_open()){
			std::cerr << "Error: failed to open config file " << configPath << "\n";
			fflush(stderr);
			return 1;
		}

		std::string key;
		while (conf >> key)
		{
			//std::cout << conf.cur << "\n";

			if (key.compare("ENABLE_QCN") == 0)
			{
				uint32_t v;
				conf >> v;
				enable_qcn = v;
				if (enable_qcn)
					std::cout << "ENABLE_QCN\t\t\t" << "Yes" << "\n";
				else
					std::cout << "ENABLE_QCN\t\t\t" << "No" << "\n";
			}
			else if (key.compare("USE_DYNAMIC_PFC_THRESHOLD") == 0)
			{
				uint32_t v;
				conf >> v;
				use_dynamic_pfc_threshold = v;
				if (use_dynamic_pfc_threshold)
					std::cout << "USE_DYNAMIC_PFC_THRESHOLD\t" << "Yes" << "\n";
				else
					std::cout << "USE_DYNAMIC_PFC_THRESHOLD\t" << "No" << "\n";
			}
			else if (key.compare("CLAMP_TARGET_RATE") == 0)
			{
				uint32_t v;
				conf >> v;
				clamp_target_rate = v;
				if (clamp_target_rate)
					std::cout << "CLAMP_TARGET_RATE\t\t" << "Yes" << "\n";
				else
					std::cout << "CLAMP_TARGET_RATE\t\t" << "No" << "\n";
			}
			else if (key.compare("PAUSE_TIME") == 0)
			{
				double v;
				conf >> v;
				pause_time = v;
				std::cout << "PAUSE_TIME\t\t\t" << pause_time << "\n";
			}
			else if (key.compare("DATA_RATE") == 0)
			{
				std::string v;
				conf >> v;
				data_rate = v;
				std::cout << "DATA_RATE\t\t\t" << data_rate << "\n";
			}
			else if (key.compare("LINK_DELAY") == 0)
			{
				std::string v;
				conf >> v;
				link_delay = v;
				std::cout << "LINK_DELAY\t\t\t" << link_delay << "\n";
			}
			else if (key.compare("PACKET_PAYLOAD_SIZE") == 0)
			{
				uint32_t v;
				conf >> v;
				packet_payload_size = v;
				std::cout << "PACKET_PAYLOAD_SIZE\t\t" << packet_payload_size << "\n";
			}
			else if (key.compare("L2_CHUNK_SIZE") == 0)
			{
				uint32_t v;
				conf >> v;
				l2_chunk_size = v;
				std::cout << "L2_CHUNK_SIZE\t\t\t" << l2_chunk_size << "\n";
			}
			else if (key.compare("L2_ACK_INTERVAL") == 0)
			{
				uint32_t v;
				conf >> v;
				l2_ack_interval = v;
				std::cout << "L2_ACK_INTERVAL\t\t\t" << l2_ack_interval << "\n";
			}
			else if (key.compare("L2_BACK_TO_ZERO") == 0)
			{
				uint32_t v;
				conf >> v;
				l2_back_to_zero = v;
				if (l2_back_to_zero)
					std::cout << "L2_BACK_TO_ZERO\t\t\t" << "Yes" << "\n";
				else
					std::cout << "L2_BACK_TO_ZERO\t\t\t" << "No" << "\n";
			}
			else if (key.compare("TOPOLOGY_FILE") == 0)
			{
				std::string v;
				conf >> v;
				topology_file = v;
				std::cout << "TOPOLOGY_FILE\t\t\t" << topology_file << "\n";
			}
			else if (key.compare("TOPOLOGY_MODE") == 0)
			{
				std::string v;
				conf >> v;
		if (v.compare("FAT_TREE") == 0)
			topology_mode = TOPOLOGY_FAT_TREE;
		else if (v.compare("2D_TORUS") == 0 || v.compare("TORUS_2D") == 0)
			topology_mode = TOPOLOGY_TORUS_2D;
		else if (v.compare("3D_TORUS") == 0 || v.compare("TORUS_3D") == 0)
			topology_mode = TOPOLOGY_TORUS_3D;
		else if (v.compare("LEAF_SPINE") == 0 || v.compare("LEAFSPINE") == 0)
			topology_mode = TOPOLOGY_LEAF_SPINE;
		else{
			NS_ASSERT_MSG(v.compare("GENERAL_GRAPH") == 0, "TOPOLOGY_MODE must be GENERAL_GRAPH, FAT_TREE, 2D_TORUS, 3D_TORUS, or LEAF_SPINE");
			topology_mode = TOPOLOGY_GENERAL_GRAPH;
		}
				std::cout << "TOPOLOGY_MODE\t\t\t" << v << "\n";
			}
			else if (key.compare("ROUTING_MODE") == 0)
			{
				std::string v;
				conf >> v;
				routing_mode_configured = true;
				if (v.compare("ECMP") == 0)
					switch_routing_mode = SWITCH_ROUTING_ECMP;
				else{
					NS_ASSERT_MSG(v.compare("PACKET_SPRAY") == 0, "ROUTING_MODE must be ECMP or PACKET_SPRAY");
					switch_routing_mode = SWITCH_ROUTING_PACKET_SPRAY;
				}
				std::cout << "ROUTING_MODE\t\t\t" << v << "\n";
			}
			else if (key.compare("OPP_MULTICAST_MODE") == 0)
			{
				std::string v;
				conf >> v;
				if (v.compare("STANDARD") == 0)
					opp_multicast_mode = OPP_MULTICAST_MODE_STANDARD_CONFIG;
				else if (v.compare("USM") == 0)
					opp_multicast_mode = OPP_MULTICAST_MODE_USM_CONFIG;
				else{
					NS_ASSERT_MSG(v.compare("SAMPLED") == 0, "OPP_MULTICAST_MODE must be STANDARD, USM, or SAMPLED");
					opp_multicast_mode = OPP_MULTICAST_MODE_SAMPLED_CONFIG;
				}
				std::cout << "OPP_MULTICAST_MODE\t\t" << v << "\n";
			}
			else if (key.compare("OPP_USM_SAMPLE_PERIOD") == 0)
			{
				conf >> opp_usm_sample_period;
				NS_ASSERT_MSG(opp_usm_sample_period > 0, "OPP_USM_SAMPLE_PERIOD must be positive");
				opp_usm_sample_rate_numerator = 1;
				opp_usm_sample_rate_denominator = opp_usm_sample_period;
				std::cout << "OPP_USM_SAMPLE_PERIOD\t\t" << opp_usm_sample_period << "\n";
			}
			else if (key.compare("OPP_USM_SAMPLE_RATE") == 0)
			{
				conf >> opp_usm_sample_rate_numerator >> opp_usm_sample_rate_denominator;
				NS_ASSERT_MSG(opp_usm_sample_rate_denominator > 0, "OPP_USM_SAMPLE_RATE denominator must be positive");
				NS_ASSERT_MSG(opp_usm_sample_rate_numerator <= opp_usm_sample_rate_denominator,
					"OPP_USM_SAMPLE_RATE numerator must not exceed denominator");
				std::cout << "OPP_USM_SAMPLE_RATE\t\t" << opp_usm_sample_rate_numerator
					<< " " << opp_usm_sample_rate_denominator << "\n";
			}
			else if (key.compare("OPP_USM_SAMPLE_POLICY") == 0)
			{
				std::string v;
				conf >> v;
				if (v.compare("QP_ORDINAL") == 0 || v.compare("0") == 0)
					opp_usm_sample_policy = OppTokenManager::USM_SAMPLE_POLICY_QP_ORDINAL;
				else{
					NS_ASSERT_MSG(v.compare("TOR_GROUP_INTERVAL") == 0 || v.compare("1") == 0,
						"OPP_USM_SAMPLE_POLICY must be QP_ORDINAL/0 or TOR_GROUP_INTERVAL/1");
					opp_usm_sample_policy = OppTokenManager::USM_SAMPLE_POLICY_TOR_GROUP_INTERVAL;
				}
				std::cout << "OPP_USM_SAMPLE_POLICY\t\t" << v << "\n";
			}
			else if (key.compare("FAT_TREE_K") == 0)
			{
				conf >> fat_tree_k;
				std::cout << "FAT_TREE_K\t\t\t" << fat_tree_k << "\n";
			}
			else if (key.compare("FLOW_FILE") == 0)
			{
				std::string v;
				conf >> v;
				flow_file = v;
				std::cout << "FLOW_FILE\t\t\t" << flow_file << "\n";
			}
			else if (key.compare("TRACE_FILE") == 0)
			{
				std::string v;
				conf >> v;
				trace_file = v;
				std::cout << "TRACE_FILE\t\t\t" << trace_file << "\n";
			}
			else if (key.compare("TRACE_OUTPUT_FILE") == 0)
			{
				std::string v;
				conf >> v;
				trace_output_file = v;
				if (argc > 2)
				{
					trace_output_file = trace_output_file + std::string(argv[2]);
				}
				std::cout << "TRACE_OUTPUT_FILE\t\t" << trace_output_file << "\n";
			}
			else if (key.compare("SIMULATOR_STOP_TIME") == 0)
			{
				double v;
				conf >> v;
				simulator_stop_time = v;
				std::cout << "SIMULATOR_STOP_TIME\t\t" << simulator_stop_time << "\n";
			}
			else if (key.compare("ALPHA_RESUME_INTERVAL") == 0)
			{
				double v;
				conf >> v;
				alpha_resume_interval = v;
				std::cout << "ALPHA_RESUME_INTERVAL\t\t" << alpha_resume_interval << "\n";
			}
			else if (key.compare("RP_TIMER") == 0)
			{
				double v;
				conf >> v;
				rp_timer = v;
				std::cout << "RP_TIMER\t\t\t" << rp_timer << "\n";
			}
			else if (key.compare("EWMA_GAIN") == 0)
			{
				double v;
				conf >> v;
				ewma_gain = v;
				std::cout << "EWMA_GAIN\t\t\t" << ewma_gain << "\n";
			}
			else if (key.compare("FAST_RECOVERY_TIMES") == 0)
			{
				uint32_t v;
				conf >> v;
				fast_recovery_times = v;
				std::cout << "FAST_RECOVERY_TIMES\t\t" << fast_recovery_times << "\n";
			}
			else if (key.compare("RATE_AI") == 0)
			{
				std::string v;
				conf >> v;
				rate_ai = v;
				std::cout << "RATE_AI\t\t\t\t" << rate_ai << "\n";
			}
			else if (key.compare("RATE_HAI") == 0)
			{
				std::string v;
				conf >> v;
				rate_hai = v;
				std::cout << "RATE_HAI\t\t\t" << rate_hai << "\n";
			}
			else if (key.compare("ERROR_RATE_PER_LINK") == 0)
			{
				double v;
				conf >> v;
				error_rate_per_link = v;
				std::cout << "ERROR_RATE_PER_LINK\t\t" << error_rate_per_link << "\n";
			}
			else if (key.compare("CC_MODE") == 0){
				conf >> cc_mode;
				std::cout << "CC_MODE\t\t" << cc_mode << '\n';
			}else if (key.compare("RATE_DECREASE_INTERVAL") == 0){
				double v;
				conf >> v;
				rate_decrease_interval = v;
				std::cout << "RATE_DECREASE_INTERVAL\t\t" << rate_decrease_interval << "\n";
			}else if (key.compare("MIN_RATE") == 0){
				conf >> min_rate;
				std::cout << "MIN_RATE\t\t" << min_rate << "\n";
			}else if (key.compare("FCT_OUTPUT_FILE") == 0){
				conf >> fct_output_file;
				std::cout << "FCT_OUTPUT_FILE\t\t" << fct_output_file << '\n';
			}else if (key.compare("OPP_PROBE_DELAY_OUTPUT_FILE") == 0){
				conf >> opp_probe_delay_output_file;
				std::cout << "OPP_PROBE_DELAY_OUTPUT_FILE\t" << opp_probe_delay_output_file << '\n';
			}else if (key.compare("HAS_WIN") == 0){
				conf >> has_win;
				std::cout << "HAS_WIN\t\t" << has_win << "\n";
			}else if (key.compare("GLOBAL_T") == 0){
				conf >> global_t;
				std::cout << "GLOBAL_T\t\t" << global_t << '\n';
			}else if (key.compare("MI_THRESH") == 0){
				conf >> mi_thresh;
				std::cout << "MI_THRESH\t\t" << mi_thresh << '\n';
			}else if (key.compare("VAR_WIN") == 0){
				uint32_t v;
				conf >> v;
				var_win = v;
				std::cout << "VAR_WIN\t\t" << v << '\n';
			}else if (key.compare("FAST_REACT") == 0){
				uint32_t v;
				conf >> v;
				fast_react = v;
				std::cout << "FAST_REACT\t\t" << v << '\n';
			}else if (key.compare("U_TARGET") == 0){
				conf >> u_target;
				std::cout << "U_TARGET\t\t" << u_target << '\n';
			}else if (key.compare("INT_MULTI") == 0){
				conf >> int_multi;
				std::cout << "INT_MULTI\t\t\t\t" << int_multi << '\n';
			}else if (key.compare("RATE_BOUND") == 0){
				uint32_t v;
				conf >> v;
				rate_bound = v;
				std::cout << "RATE_BOUND\t\t" << rate_bound << '\n';
			}else if (key.compare("ACK_HIGH_PRIO") == 0){
				conf >> ack_high_prio;
				std::cout << "ACK_HIGH_PRIO\t\t" << ack_high_prio << '\n';
			}else if (key.compare("DCTCP_RATE_AI") == 0){
				conf >> dctcp_rate_ai;
				std::cout << "DCTCP_RATE_AI\t\t\t\t" << dctcp_rate_ai << "\n";
			}else if (key.compare("PFC_OUTPUT_FILE") == 0){
				conf >> pfc_output_file;
				std::cout << "PFC_OUTPUT_FILE\t\t\t\t" << pfc_output_file << '\n';
			}else if (key.compare("FLOW_COVERAGE_OUTPUT_FILE") == 0){
				conf >> flow_coverage_output_file;
				std::cout << "FLOW_COVERAGE_OUTPUT_FILE\t\t" << flow_coverage_output_file << '\n';
			}else if (key.compare("OPP_LOOPBACK_TIMESERIES_OUTPUT_FILE") == 0){
				conf >> opp_loopback_timeseries_output_file;
				std::cout << "OPP_LOOPBACK_TIMESERIES_OUTPUT_FILE\t" << opp_loopback_timeseries_output_file << '\n';
			}else if (key.compare("OPP_LOOPBACK_TIMESERIES_POD") == 0){
				conf >> opp_loopback_timeseries_pod;
				std::cout << "OPP_LOOPBACK_TIMESERIES_POD\t" << opp_loopback_timeseries_pod << '\n';
			}else if (key.compare("OPP_LOOPBACK_PEAK_BUCKET_OUTPUT_FILE") == 0){
				conf >> opp_loopback_peak_bucket_output_file;
				std::cout << "OPP_LOOPBACK_PEAK_BUCKET_OUTPUT_FILE\t" << opp_loopback_peak_bucket_output_file << '\n';
			}else if (key.compare("OPP_LOOPBACK_PEAK_BUCKET_INTERVAL_NS") == 0){
				conf >> opp_loopback_peak_bucket_interval_ns;
				std::cout << "OPP_LOOPBACK_PEAK_BUCKET_INTERVAL_NS\t" << opp_loopback_peak_bucket_interval_ns << '\n';
			}else if (key.compare("OPP_LOOPBACK_RATE_TRACE_OUTPUT_FILE") == 0){
				conf >> opp_loopback_rate_trace_output_file;
				std::cout << "OPP_LOOPBACK_RATE_TRACE_OUTPUT_FILE\t" << opp_loopback_rate_trace_output_file << '\n';
			}else if (key.compare("OPP_LOOPBACK_RATE_TRACE_NODE") == 0){
				conf >> opp_loopback_rate_trace_node;
				std::cout << "OPP_LOOPBACK_RATE_TRACE_NODE\t" << opp_loopback_rate_trace_node << '\n';
			}else if (key.compare("OPP_LOOPBACK_RATE_TRACE_START_NS") == 0 ||
					key.compare("OPP_LOOPBACK_RATE_TRACE_START_RELATIVE_NS") == 0){
				conf >> opp_loopback_rate_trace_start_relative_ns;
				std::cout << "OPP_LOOPBACK_RATE_TRACE_START_NS\t" << opp_loopback_rate_trace_start_relative_ns << '\n';
			}else if (key.compare("OPP_LOOPBACK_RATE_TRACE_END_NS") == 0 ||
					key.compare("OPP_LOOPBACK_RATE_TRACE_END_RELATIVE_NS") == 0){
				conf >> opp_loopback_rate_trace_end_relative_ns;
				std::cout << "OPP_LOOPBACK_RATE_TRACE_END_NS\t" << opp_loopback_rate_trace_end_relative_ns << '\n';
			}else if (key.compare("OPP_LOOPBACK_RATE_TRACE_INTERVAL_NS") == 0){
				conf >> opp_loopback_rate_trace_interval_ns;
				std::cout << "OPP_LOOPBACK_RATE_TRACE_INTERVAL_NS\t" << opp_loopback_rate_trace_interval_ns << '\n';
			}else if (key.compare("SWITCH_QLEN_TRACE_OUTPUT_FILE") == 0 || key.compare("QLEN_TRACE_OUTPUT_FILE") == 0){
				conf >> switch_qlen_trace_output_file;
				std::cout << "SWITCH_QLEN_TRACE_OUTPUT_FILE\t" << switch_qlen_trace_output_file << '\n';
			}else if (key.compare("SWITCH_QLEN_TRACE_NODE") == 0 || key.compare("QLEN_TRACE_NODE") == 0){
				conf >> switch_qlen_trace_node;
				std::cout << "SWITCH_QLEN_TRACE_NODE\t" << switch_qlen_trace_node << '\n';
			}else if (key.compare("SWITCH_QLEN_TRACE_DURATION_NS") == 0 || key.compare("QLEN_TRACE_DURATION_NS") == 0){
				conf >> switch_qlen_trace_duration_ns;
				std::cout << "SWITCH_QLEN_TRACE_DURATION_NS\t" << switch_qlen_trace_duration_ns << '\n';
			}else if (key.compare("SWITCH_QLEN_TRACE_INTERVAL_NS") == 0 || key.compare("QLEN_TRACE_INTERVAL_NS") == 0){
				conf >> switch_qlen_trace_interval_ns;
				std::cout << "SWITCH_QLEN_TRACE_INTERVAL_NS\t" << switch_qlen_trace_interval_ns << '\n';
			}else if (key.compare("OPP_USM_COPY_LATENCY_DETAIL") == 0 || key.compare("OPP_USM_COPY_LATENCY_LOG") == 0){
				conf >> opp_usm_copy_latency_detail;
				NS_ASSERT_MSG(opp_usm_copy_latency_detail <= 1, "OPP_USM_COPY_LATENCY_DETAIL must be 0 or 1");
				SwitchNode::SetOppUsmCopyLatencyDetailEnabled(opp_usm_copy_latency_detail != 0);
				std::cout << "OPP_USM_COPY_LATENCY_DETAIL\t" << opp_usm_copy_latency_detail << '\n';
			}else if (key.compare("ENABLE_RDPROBE_DIAG") == 0){
				conf >> enable_rdprobe_diag;
				std::cout << "ENABLE_RDPROBE_DIAG\t\t" << enable_rdprobe_diag << '\n';
			}else if (key.compare("RDPROBE_DIAG_OUTPUT_FILE") == 0){
				conf >> rdprobe_diag_output_file;
				std::cout << "RDPROBE_DIAG_OUTPUT_FILE\t\t" << rdprobe_diag_output_file << '\n';
			}else if (key.compare("RDPROBE_DIAG_RESULT_OUTPUT_FILE") == 0){
				conf >> rdprobe_diag_result_output_file;
				std::cout << "RDPROBE_DIAG_RESULT_OUTPUT_FILE\t\t" << rdprobe_diag_result_output_file << '\n';
			}else if (key.compare("ENABLE_RPINGMESH_DIAG") == 0){
				conf >> enable_rpingmesh_diag;
				std::cout << "ENABLE_RPINGMESH_DIAG\t\t" << enable_rpingmesh_diag << '\n';
			}else if (key.compare("RPINGMESH_DIAG_RESULT_OUTPUT_FILE") == 0){
				conf >> rpingmesh_diag_result_output_file;
				std::cout << "RPINGMESH_DIAG_RESULT_OUTPUT_FILE\t" << rpingmesh_diag_result_output_file << '\n';
			}else if (key.compare("ENABLE_OPP_DIAG") == 0){
				conf >> enable_opp_diag;
				std::cout << "ENABLE_OPP_DIAG\t\t\t" << enable_opp_diag << '\n';
			}else if (key.compare("OPP_DIAG_RESULT_OUTPUT_FILE") == 0){
				conf >> opp_diag_result_output_file;
				std::cout << "OPP_DIAG_RESULT_OUTPUT_FILE\t\t" << opp_diag_result_output_file << '\n';
			}else if (key.compare("FAT_TREE_FAULT_MODE") == 0){
				conf >> fat_tree_fault_mode;
				NS_ASSERT_MSG(fat_tree_fault_mode <= 4, "FAT_TREE_FAULT_MODE must be in [0, 4]");
				std::cout << "FAT_TREE_FAULT_MODE\t\t" << fat_tree_fault_mode << '\n';
			}else if (key.compare("FAT_TREE_FAULT_SEED") == 0){
				conf >> fat_tree_fault_seed;
				std::cout << "FAT_TREE_FAULT_SEED\t\t" << fat_tree_fault_seed << '\n';
			}else if (key.compare("LINK_DOWN") == 0){
				conf >> link_down_time >> link_down_A >> link_down_B;
				std::cout << "LINK_DOWN\t\t\t\t" << link_down_time << ' '<< link_down_A << ' ' << link_down_B << '\n';
			}else if (key.compare("ENABLE_TRACE") == 0){
				conf >> enable_trace;
				std::cout << "ENABLE_TRACE\t\t\t\t" << enable_trace << '\n';
			}else if (key.compare("INIT_DEBUG_LOG") == 0){
				conf >> init_debug_log;
				std::cout << "INIT_DEBUG_LOG\t\t\t\t" << init_debug_log << '\n';
			}else if (key.compare("INIT_LINK_DETAIL_LOG") == 0){
				conf >> init_link_detail_log;
				std::cout << "INIT_LINK_DETAIL_LOG\t\t\t" << init_link_detail_log << '\n';
			}else if (key.compare("SWITCH_INGRESS_PIPELINE_DELAY_NS") == 0){
				conf >> switch_ingress_pipeline_delay_ns;
				std::cout << "SWITCH_INGRESS_PIPELINE_DELAY_NS\t" << switch_ingress_pipeline_delay_ns << '\n';
			}else if (key.compare("SWITCH_EGRESS_PIPELINE_DELAY_NS") == 0){
				conf >> switch_egress_pipeline_delay_ns;
				std::cout << "SWITCH_EGRESS_PIPELINE_DELAY_NS\t\t" << switch_egress_pipeline_delay_ns << '\n';
			}else if (key.compare("OPP_TOKENS_PER_TOR") == 0){
				conf >> opp_tokens_per_tor;
				NS_ASSERT_MSG(opp_tokens_per_tor > 0, "OPP_TOKENS_PER_TOR must be positive");
				std::cout << "OPP_TOKENS_PER_TOR\t\t\t" << opp_tokens_per_tor << '\n';
			}else if (key.compare("OPP_FATTREE_M1") == 0){
				conf >> opp_fattree_m1;
				NS_ASSERT_MSG(opp_fattree_m1 > 0 && opp_fattree_m1 <= 256, "OPP_FATTREE_M1 must be in [1, 256]");
				std::cout << "OPP_FATTREE_M1\t\t\t" << opp_fattree_m1 << '\n';
			}else if (key.compare("OPP_FATTREE_M2") == 0){
				conf >> opp_fattree_m2;
				NS_ASSERT_MSG(opp_fattree_m2 > 0 && opp_fattree_m2 <= 256, "OPP_FATTREE_M2 must be in [1, 256]");
				std::cout << "OPP_FATTREE_M2\t\t\t" << opp_fattree_m2 << '\n';
			}else if (key.compare("OPP_FATTREE_M3") == 0){
				conf >> opp_fattree_m3;
				NS_ASSERT_MSG(opp_fattree_m3 > 0 && opp_fattree_m3 <= 256, "OPP_FATTREE_M3 must be in [1, 256]");
				std::cout << "OPP_FATTREE_M3\t\t\t" << opp_fattree_m3 << '\n';
			}else if (key.compare("OPP_INITIAL_INTERPOD_PROBE_SPREAD_NS") == 0){
				conf >> opp_initial_interpod_probe_spread_ns;
				std::cout << "OPP_INITIAL_INTERPOD_PROBE_SPREAD_NS\t" << opp_initial_interpod_probe_spread_ns << '\n';
			}else if (key.compare("OPP_LEAFSPINE_M1") == 0){
				conf >> opp_leafspine_m1;
				NS_ASSERT_MSG(opp_leafspine_m1 > 0 && opp_leafspine_m1 <= 256, "OPP_LEAFSPINE_M1 must be in [1, 256]");
				std::cout << "OPP_LEAFSPINE_M1\t\t\t" << opp_leafspine_m1 << '\n';
			}else if (key.compare("OPP_LEAFSPINE_M2") == 0){
				conf >> opp_leafspine_m2;
				NS_ASSERT_MSG(opp_leafspine_m2 > 0 && opp_leafspine_m2 <= 256, "OPP_LEAFSPINE_M2 must be in [1, 256]");
				std::cout << "OPP_LEAFSPINE_M2\t\t\t" << opp_leafspine_m2 << '\n';
			}else if (key.compare("OPP_PROBE_INSTANCE_COUNT") == 0 || key.compare("PROBE_INSTANCE_COUNT") == 0){
				conf >> opp_probe_instance_count;
				NS_ASSERT_MSG(opp_probe_instance_count > 0, "OPP_PROBE_INSTANCE_COUNT must be positive");
				std::cout << "OPP_PROBE_INSTANCE_COUNT\t\t" << opp_probe_instance_count << '\n';
			}else if (key.compare("KMAX_MAP") == 0){
				int n_k ;
				conf >> n_k;
				std::cout << "KMAX_MAP\t\t\t\t";
				for (int i = 0; i < n_k; i++){
					uint64_t rate;
					uint32_t k;
					conf >> rate >> k;
					rate2kmax[rate] = k;
					std::cout << ' ' << rate << ' ' << k;
				}
				std::cout<<'\n';
			}else if (key.compare("KMIN_MAP") == 0){
				int n_k ;
				conf >> n_k;
				std::cout << "KMIN_MAP\t\t\t\t";
				for (int i = 0; i < n_k; i++){
					uint64_t rate;
					uint32_t k;
					conf >> rate >> k;
					rate2kmin[rate] = k;
					std::cout << ' ' << rate << ' ' << k;
				}
				std::cout<<'\n';
			}else if (key.compare("PMAX_MAP") == 0){
				int n_k ;
				conf >> n_k;
				std::cout << "PMAX_MAP\t\t\t\t";
				for (int i = 0; i < n_k; i++){
					uint64_t rate;
					double p;
					conf >> rate >> p;
					rate2pmax[rate] = p;
					std::cout << ' ' << rate << ' ' << p;
				}
				std::cout<<'\n';
			}else if (key.compare("BUFFER_SIZE") == 0){
				conf >> buffer_size;
				std::cout << "BUFFER_SIZE\t\t\t\t" << buffer_size << '\n';
			}else if (key.compare("QLEN_MON_FILE") == 0){
				conf >> qlen_mon_file;
				std::cout << "QLEN_MON_FILE\t\t\t\t" << qlen_mon_file << '\n';
			}else if (key.compare("QLEN_MON_START") == 0){
				conf >> qlen_mon_start;
				std::cout << "QLEN_MON_START\t\t\t\t" << qlen_mon_start << '\n';
			}else if (key.compare("QLEN_MON_END") == 0){
				conf >> qlen_mon_end;
				std::cout << "QLEN_MON_END\t\t\t\t" << qlen_mon_end << '\n';
			}else if (key.compare("MULTI_RATE") == 0){
				int v;
				conf >> v;
				multi_rate = v;
				std::cout << "MULTI_RATE\t\t\t\t" << multi_rate << '\n';
			}else if (key.compare("SAMPLE_FEEDBACK") == 0){
				int v;
				conf >> v;
				sample_feedback = v;
				std::cout << "SAMPLE_FEEDBACK\t\t\t\t" << sample_feedback << '\n';
			}else if(key.compare("PINT_LOG_BASE") == 0){
				conf >> pint_log_base;
				std::cout << "PINT_LOG_BASE\t\t\t\t" << pint_log_base << '\n';
			}else if (key.compare("PINT_PROB") == 0){
				conf >> pint_prob;
				std::cout << "PINT_PROB\t\t\t\t" << pint_prob << '\n';
			}
			fflush(stdout);
		}
		conf.close();
		InitLog("config parsed");
	}
	else
	{
		std::cout << "Error: require a config file\n";
		fflush(stdout);
		return 1;
	}


	bool dynamicth = use_dynamic_pfc_threshold;
	if (enable_rdprobe_diag){
		NS_ASSERT_MSG(IsFaultDiagnosisTopology(), "RDPROBE diagnosis trace is only supported for FAT_TREE/LEAF_SPINE topology");
		NS_ASSERT_MSG(cc_mode == RDMA_CC_MODE_RDPROBE, "RDPROBE diagnosis trace requires CC_MODE 13");
		rdprobe_diag_file = fopen(rdprobe_diag_output_file.c_str(), "w");
		NS_ASSERT_MSG(rdprobe_diag_file != NULL, "failed to open RDPROBE_DIAG_OUTPUT_FILE");
	}
	if (enable_rpingmesh_diag){
		NS_ASSERT_MSG(IsFaultDiagnosisTopology(), "RPINGMESH diagnosis is only supported for FAT_TREE/LEAF_SPINE topology");
		NS_ASSERT_MSG(cc_mode == RDMA_CC_MODE_RPINGMESH, "RPINGMESH diagnosis requires CC_MODE 11");
		NS_ASSERT_MSG(fat_tree_fault_mode >= 1 && fat_tree_fault_mode <= 4,
			"RPINGMESH diagnosis requires FAT_TREE_FAULT_MODE in [1, 4]");
	}
	if (enable_opp_diag){
		NS_ASSERT_MSG(IsFaultDiagnosisTopology(), "OPP diagnosis is only supported for FAT_TREE/LEAF_SPINE topology");
		NS_ASSERT_MSG(cc_mode == RDMA_CC_MODE_OPP, "OPP diagnosis requires CC_MODE 12");
		NS_ASSERT_MSG(fat_tree_fault_mode >= 1 && fat_tree_fault_mode <= 4,
			"OPP diagnosis requires FAT_TREE_FAULT_MODE in [1, 4]");
	}
	if (opp_multicast_mode != OPP_MULTICAST_MODE_STANDARD_CONFIG)
		NS_ASSERT_MSG(cc_mode == RDMA_CC_MODE_OPP, "OPP_MULTICAST_MODE USM/SAMPLED requires CC_MODE 12");
	if (fat_tree_fault_mode != 0){
		NS_ASSERT_MSG(IsFaultDiagnosisTopology(), "FAT_TREE_FAULT_MODE requires TOPOLOGY_MODE FAT_TREE or LEAF_SPINE");
		NS_ASSERT_MSG(IsRpingmeshLikeCcMode(cc_mode), "FAT_TREE_FAULT_MODE requires CC_MODE 11, 12, or 13");
	}
	if (IsRpingmeshLikeCcMode(cc_mode) && !routing_mode_configured){
		switch_routing_mode = SWITCH_ROUTING_PACKET_SPRAY;
		std::cout << "ROUTING_MODE\t\t\tPACKET_SPRAY (default for RPINGMESH/OPP/RDPROBE)\n";
		fflush(stdout);
	}

	Config::SetDefault("ns3::QbbNetDevice::PauseTime", UintegerValue(pause_time));
	Config::SetDefault("ns3::QbbNetDevice::QcnEnabled", BooleanValue(enable_qcn));
	Config::SetDefault("ns3::QbbNetDevice::DynamicThreshold", BooleanValue(dynamicth));
	Config::SetDefault("ns3::QbbNetDevice::SwitchIngressPipelineDelay", TimeValue(NanoSeconds((int64_t)switch_ingress_pipeline_delay_ns)));
	Config::SetDefault("ns3::QbbNetDevice::SwitchEgressPipelineDelay", TimeValue(NanoSeconds((int64_t)switch_egress_pipeline_delay_ns)));

	// set int_multi
	IntHop::multi = int_multi;
	// IntHeader::mode
	if (cc_mode == 7) // timely, use ts
		IntHeader::mode = IntHeader::TS;
	else if (cc_mode == 3) // hpcc, use int
		IntHeader::mode = IntHeader::NORMAL;
	else if (cc_mode == 10) // hpcc-pint
		IntHeader::mode = IntHeader::PINT;
	else // others, no extra header
		IntHeader::mode = IntHeader::NONE;

	// Set Pint
	if (cc_mode == 10){
		Pint::set_log_base(pint_log_base);
		IntHeader::pint_bytes = Pint::get_n_bytes();
		printf("PINT bits: %d bytes: %d\n", Pint::get_n_bits(), Pint::get_n_bytes());
	}

	//SeedManager::SetSeed(time(NULL));

	topof.open(topology_file.c_str());
	flowf.open(flow_file.c_str());
	tracef.open(trace_file.c_str());
	InitLog("input files opened");
	uint32_t node_num, switch_num, link_num, trace_num;
	topof >> node_num >> switch_num >> link_num;
	flowf >> flow_num;
	tracef >> trace_num;
	if (init_debug_log){
		fprintf(stderr, "[INIT] topology header nodes=%u switches=%u links=%u flows=%u trace_nodes=%u\n", node_num, switch_num, link_num, flow_num, trace_num);
		fflush(stderr);
	}
	if (topology_mode == TOPOLOGY_FAT_TREE)
		InitFatTreeLayout(node_num, switch_num, link_num);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		InitLeafSpineLayout(node_num, switch_num, link_num);
	InitLog("topology layout initialized");


	//n.Create(node_num);
	std::vector<uint32_t> node_type(node_num, 0);
	for (uint32_t i = 0; i < switch_num; i++)
	{
		uint32_t sid;
		topof >> sid;
		node_type[sid] = 1;
	}
	if (topology_mode == TOPOLOGY_FAT_TREE)
		ValidateFatTreeNodeTypes(node_type);
	if (topology_mode == TOPOLOGY_LEAF_SPINE)
		ValidateLeafSpineNodeTypes(node_type);
	InitLog("node types loaded");
	for (uint32_t i = 0; i < node_num; i++){
		if (node_type[i] == 0){
			n.Add(CreateObject<Node>());
		}else{
			Ptr<SwitchNode> sw = CreateObject<SwitchNode>();
			n.Add(sw);
			sw->SetAttribute("EcnEnabled", BooleanValue(enable_qcn));
			sw->SetEcmpMode(switch_routing_mode);
			sw->SetOppMulticastMode(opp_multicast_mode);
		}
		if (InitShouldLogProgress(i + 1, node_num, 10000))
			InitLogProgress("create nodes", i + 1, node_num);
	}


	NS_LOG_INFO("Create nodes.");
	InitLog("nodes created");
	ifacePeer.resize(node_num);
	hostToTor.assign(node_num, 0xffffffffu);

	InternetStackHelper internet;
	InitLog("InternetStack install begin");
	internet.Install(n);
	InitLog("InternetStack install done");

	//
	// Assign IP to each server
	//
	for (uint32_t i = 0; i < node_num; i++){
		if (n.Get(i)->GetNodeType() == 0){ // is server
			serverAddress.resize(i + 1);
			serverAddress[i] = node_id_to_ip(i);
		}
		if (InitShouldLogProgress(i + 1, node_num, 10000))
			InitLogProgress("assign server IPs", i + 1, node_num);
	}
	InitLog("server IP assignment done");
	if (cc_mode == RDMA_CC_MODE_OPP)
		ValidateOppProbeInstanceCapacity();

	NS_LOG_INFO("Create channels.");

	//
	// Explicitly create the channels required by the topology.
	//

	Ptr<RateErrorModel> rem = CreateObject<RateErrorModel>();
	Ptr<UniformRandomVariable> uv = CreateObject<UniformRandomVariable>();
	rem->SetRandomVariable(uv);
	uv->SetStream(50);
	rem->SetAttribute("ErrorRate", DoubleValue(error_rate_per_link));
	rem->SetAttribute("ErrorUnit", StringValue("ERROR_UNIT_PACKET"));

	FILE *pfc_file = fopen(pfc_output_file.c_str(), "w");

	QbbHelper qbb;
	Ipv4AddressHelper ipv4;
	QbbHelper::SetInstallDebug(init_link_detail_log != 0);
	QbbHelper::ResetInstallDebugStats();
	Ipv4AddressHelper::SetAssignDebug(init_link_detail_log != 0);
	Ipv4AddressHelper::ResetAssignDebugStats();
	LinkInitStats linkStats;
	uint64_t linkSegmentStartUs = InitNowUs();
	uint32_t linkSegmentStart = 0;
	InitLog("link creation begin");
	for (uint32_t i = 0; i < link_num; i++)
	{
		uint64_t stepStart = init_link_detail_log ? InitNowUs() : 0;
		uint32_t src, dst;
		std::string data_rate, link_delay;
		double error_rate;
		topof >> src >> dst >> data_rate >> link_delay >> error_rate;
		LinkInitAdd(linkStats.readUs, stepStart);

		Ptr<Node> snode = n.Get(src), dnode = n.Get(dst);

		stepStart = init_link_detail_log ? InitNowUs() : 0;
		qbb.SetDeviceAttribute("DataRate", StringValue(data_rate));
		qbb.SetChannelAttribute("Delay", StringValue(link_delay));

		if (error_rate > 0)
		{
			Ptr<RateErrorModel> rem = CreateObject<RateErrorModel>();
			Ptr<UniformRandomVariable> uv = CreateObject<UniformRandomVariable>();
			rem->SetRandomVariable(uv);
			uv->SetStream(50);
			rem->SetAttribute("ErrorRate", DoubleValue(error_rate));
			rem->SetAttribute("ErrorUnit", StringValue("ERROR_UNIT_PACKET"));
			qbb.SetDeviceAttribute("ReceiveErrorModel", PointerValue(rem));
		}
		else
		{
			qbb.SetDeviceAttribute("ReceiveErrorModel", PointerValue(rem));
		}
		LinkInitAdd(linkStats.attrUs, stepStart);

		stepStart = init_link_detail_log ? InitNowUs() : 0;
		fflush(stdout);
		LinkInitAdd(linkStats.flushUs, stepStart);

		// Assigne server IP
		// Note: this should be before the automatic assignment below (ipv4.Assign(d)),
		// because we want our IP to be the primary IP (first in the IP address list),
		// so that the global routing is based on our IP
		stepStart = init_link_detail_log ? InitNowUs() : 0;
		NetDeviceContainer d = qbb.Install(snode, dnode);
		LinkInitAdd(linkStats.installUs, stepStart);

		stepStart = init_link_detail_log ? InitNowUs() : 0;
		if (snode->GetNodeType() == 0){
			Ptr<Ipv4> ipv4 = snode->GetObject<Ipv4>();
			ipv4->AddInterface(d.Get(0));
			ipv4->AddAddress(1, Ipv4InterfaceAddress(serverAddress[src], Ipv4Mask(0xff000000)));
		}
		if (dnode->GetNodeType() == 0){
			Ptr<Ipv4> ipv4 = dnode->GetObject<Ipv4>();
			ipv4->AddInterface(d.Get(1));
			ipv4->AddAddress(1, Ipv4InterfaceAddress(serverAddress[dst], Ipv4Mask(0xff000000)));
		}
		LinkInitAdd(linkStats.hostIpUs, stepStart);

		stepStart = init_link_detail_log ? InitNowUs() : 0;
		Ptr<QbbNetDevice> srcDev = DynamicCast<QbbNetDevice>(d.Get(0));
		Ptr<QbbNetDevice> dstDev = DynamicCast<QbbNetDevice>(d.Get(1));
		uint32_t srcIf = srcDev->GetIfIndex();
		uint32_t dstIf = dstDev->GetIfIndex();
		if (ifacePeer[src].size() <= srcIf)
			ifacePeer[src].resize(srcIf + 1, 0xffffffffu);
		if (ifacePeer[dst].size() <= dstIf)
			ifacePeer[dst].resize(dstIf + 1, 0xffffffffu);
		ifacePeer[src][srcIf] = dst;
		ifacePeer[dst][dstIf] = src;
		if (snode->GetNodeType() == 0 && dnode->GetNodeType() == 1)
			hostToTor[src] = dst;
		if (dnode->GetNodeType() == 0 && snode->GetNodeType() == 1)
			hostToTor[dst] = src;
		if (topology_mode == TOPOLOGY_FAT_TREE){
			SaveFatTreeInterfaces(src, dst, srcDev, dstDev);
		}else{
			// used to create a graph of the topology
			SaveInterface(nbr2if[snode][dnode], srcDev);
			SaveInterface(nbr2if[dnode][snode], dstDev);
		}
		LinkInitAdd(linkStats.saveInterfaceUs, stepStart);

		// This is just to set up the connectivity between nodes. Use /30 so large
		// topologies do not overflow dotted-decimal octets.
		stepStart = init_link_detail_log ? InitNowUs() : 0;
		uint64_t linkNetworkOffset = static_cast<uint64_t>(i) * 4;
		NS_ASSERT_MSG(linkNetworkOffset + 3 <= 0x00ffffffull, "Too many links for 10.0.0.0/8 /30 addressing");
		uint32_t linkNetwork = static_cast<uint32_t>(0x0a000000ull + linkNetworkOffset);
		Ipv4Mask linkMask(0xfffffffc);
		if (topology_mode == TOPOLOGY_FAT_TREE){
			LinkInitAdd(linkStats.ipv4SetBaseUs, stepStart);
			stepStart = init_link_detail_log ? InitNowUs() : 0;
			AssignIpv4AddressToDevice(snode, d.Get(0), Ipv4Address(linkNetwork + 1), linkMask);
			AssignIpv4AddressToDevice(dnode, d.Get(1), Ipv4Address(linkNetwork + 2), linkMask);
		}else{
			ipv4.SetBase(Ipv4Address(linkNetwork), linkMask);
			LinkInitAdd(linkStats.ipv4SetBaseUs, stepStart);

			stepStart = init_link_detail_log ? InitNowUs() : 0;
			ipv4.Assign(d);
		}
		LinkInitAdd(linkStats.ipv4AssignUs, stepStart);

		// setup PFC trace
		stepStart = init_link_detail_log ? InitNowUs() : 0;
		srcDev->TraceConnectWithoutContext("QbbPfc", MakeBoundCallback (&get_pfc, pfc_file, srcDev));
		dstDev->TraceConnectWithoutContext("QbbPfc", MakeBoundCallback (&get_pfc, pfc_file, dstDev));
		LinkInitAdd(linkStats.pfcTraceUs, stepStart);

		if (InitShouldLogProgress(i + 1, link_num, 10000)){
			InitLogProgress("create links", i + 1, link_num);
			InitLogLinkDetail(i + 1, link_num, linkSegmentStart, linkSegmentStartUs, linkStats);
			linkStats.Reset();
			linkSegmentStartUs = InitNowUs();
			linkSegmentStart = i + 1;
		}
	}
	QbbHelper::SetInstallDebug(false);
	Ipv4AddressHelper::SetAssignDebug(false);
	InitLog("link creation done");

	nic_rate = get_nic_rate(n);
	InitLog("nic rate discovered");
	if (opp_multicast_mode == OPP_MULTICAST_MODE_USM_CONFIG ||
			opp_multicast_mode == OPP_MULTICAST_MODE_SAMPLED_CONFIG){
		InstallOppLoopbackDevices(n);
		InitLog("OPP loopback devices installed");
	}

	// config switch
	InitLog("switch config begin");
	for (uint32_t i = 0; i < node_num; i++){
		if (n.Get(i)->GetNodeType() == 1){ // is switch
			Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
			sw->SetPacketForwardCallback(MakeCallback(&TrackFlowCoverageLink));
			if (!switch_qlen_trace_output_file.empty()){
				sw->SetPacketReceiveCallback(MakeCallback(&TraceSwitchQlenFirstPacket));
				sw->SetOppLoopbackTxCallback(MakeCallback(&TraceSwitchLoopbackTx));
			}
			if (rdprobe_diag_file != NULL)
				sw->SetRdProbeTraceCallback(MakeCallback(&TraceRdProbeDiag));
			uint32_t shift = 3; // by default 1/8
			for (uint32_t j = 1; j < sw->GetNDevices(); j++){
				Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(sw->GetDevice(j));
				// set ecn
				uint64_t rate = dev->GetDataRate().GetBitRate();
				NS_ASSERT_MSG(rate2kmin.find(rate) != rate2kmin.end(), "must set kmin for each link speed");
				NS_ASSERT_MSG(rate2kmax.find(rate) != rate2kmax.end(), "must set kmax for each link speed");
				NS_ASSERT_MSG(rate2pmax.find(rate) != rate2pmax.end(), "must set pmax for each link speed");
				sw->m_mmu->ConfigEcn(j, rate2kmin[rate], rate2kmax[rate], rate2pmax[rate]);
				// set pfc
				uint64_t delay = 0;
				if (!dev->IsSwitchLoopback()){
					Ptr<QbbChannel> channel = DynamicCast<QbbChannel>(dev->GetChannel());
					NS_ASSERT_MSG(channel != NULL, "physical QbbNetDevice must have a QbbChannel");
					delay = channel->GetDelay().GetTimeStep();
				}
				uint32_t headroom = rate * delay / 8 / 1000000000 * 3;
				sw->m_mmu->ConfigHdrm(j, headroom);

				// set pfc alpha, proportional to link bw
				sw->m_mmu->pfc_a_shift[j] = shift;
				while (rate > nic_rate && sw->m_mmu->pfc_a_shift[j] > 0){
					sw->m_mmu->pfc_a_shift[j]--;
					rate /= 2;
				}
			}
			sw->m_mmu->ConfigNPort(sw->GetNDevices()-1);
			sw->m_mmu->ConfigBufferSize(buffer_size* 1024 * 1024);
			sw->m_mmu->node_id = sw->GetId();
		}
		if (InitShouldLogProgress(i + 1, node_num, 10000))
			InitLogProgress("configure switches", i + 1, node_num);
	}
	InitLog("switch config done");

	#if ENABLE_QP
	FILE *fct_output = fopen(fct_output_file.c_str(), "w");
	if (cc_mode == RDMA_CC_MODE_OPP && !opp_probe_delay_output_file.empty()){
		opp_probe_delay_output = fopen(opp_probe_delay_output_file.c_str(), "w");
		NS_ASSERT_MSG(opp_probe_delay_output != NULL, "failed to open OPP_PROBE_DELAY_OUTPUT_FILE");
		fprintf(opp_probe_delay_output,
			"sip,dip,sport,dport,pg,size,first_probe_send_time_ns,last_opp_ack_time_ns,opp_probe_delay_ns,ack_observed\n");
		fflush(opp_probe_delay_output);
	}
	//
	// install RDMA driver
	//
	InitLog("RDMA driver install begin");
	for (uint32_t i = 0; i < node_num; i++){
		if (n.Get(i)->GetNodeType() == 0){ // is server
			// create RdmaHw
			Ptr<RdmaHw> rdmaHw = CreateObject<RdmaHw>();
			rdmaHw->SetAttribute("ClampTargetRate", BooleanValue(clamp_target_rate));
			rdmaHw->SetAttribute("AlphaResumInterval", DoubleValue(alpha_resume_interval));
			rdmaHw->SetAttribute("RPTimer", DoubleValue(rp_timer));
			rdmaHw->SetAttribute("FastRecoveryTimes", UintegerValue(fast_recovery_times));
			rdmaHw->SetAttribute("EwmaGain", DoubleValue(ewma_gain));
			rdmaHw->SetAttribute("RateAI", DataRateValue(DataRate(rate_ai)));
			rdmaHw->SetAttribute("RateHAI", DataRateValue(DataRate(rate_hai)));
			rdmaHw->SetAttribute("L2BackToZero", BooleanValue(l2_back_to_zero));
			rdmaHw->SetAttribute("L2ChunkSize", UintegerValue(l2_chunk_size));
			rdmaHw->SetAttribute("L2AckInterval", UintegerValue(l2_ack_interval));
			rdmaHw->SetAttribute("CcMode", UintegerValue(cc_mode));
			rdmaHw->SetAttribute("RateDecreaseInterval", DoubleValue(rate_decrease_interval));
			rdmaHw->SetAttribute("MinRate", DataRateValue(DataRate(min_rate)));
			rdmaHw->SetAttribute("Mtu", UintegerValue(packet_payload_size));
			rdmaHw->SetAttribute("MiThresh", UintegerValue(mi_thresh));
			rdmaHw->SetAttribute("VarWin", BooleanValue(var_win));
			rdmaHw->SetAttribute("FastReact", BooleanValue(fast_react));
			rdmaHw->SetAttribute("MultiRate", BooleanValue(multi_rate));
			rdmaHw->SetAttribute("SampleFeedback", BooleanValue(sample_feedback));
			rdmaHw->SetAttribute("TargetUtil", DoubleValue(u_target));
			rdmaHw->SetAttribute("RateBound", BooleanValue(rate_bound));
			rdmaHw->SetAttribute("DctcpRateAI", DataRateValue(DataRate(dctcp_rate_ai)));
			rdmaHw->SetPintSmplThresh(pint_prob);
			if (cc_mode == RDMA_CC_MODE_OPP){
				uint32_t torId = GetOppTokenTorForHost(i);
				for (uint32_t instanceId = 0; instanceId < opp_probe_instance_count; instanceId++)
					rdmaHw->SetOppTokenManager(instanceId, GetOppTokenManager(instanceId, torId), i);
				if (enable_opp_diag)
					rdmaHw->SetOppAckReceivedCallback(MakeCallback(&TraceOppAckReceived));
			}
			if (cc_mode == RDMA_CC_MODE_RPINGMESH && enable_rpingmesh_diag){
				rdmaHw->SetRpingmeshProbeSentCallback(MakeCallback(&TraceRpingmeshProbeSent));
				rdmaHw->SetRpingmeshProbeTtlCallback(MakeCallback(&CheckRpingmeshProbeTtl));
				rdmaHw->SetRpingmeshAckReceivedCallback(MakeCallback(&TraceRpingmeshAckReceived));
				if (topology_mode == TOPOLOGY_LEAF_SPINE)
					rdmaHw->SetRpingmeshTracerouteTtl(fat_tree_fault_mode == 2 ? 4 : 2);
				else
					rdmaHw->SetRpingmeshTracerouteTtl(RPINGMESH_TRACEROUTE_TTL);
				rdmaHw->SetRpingmeshTracerouteOnMissingAck(fat_tree_fault_mode == 1 || fat_tree_fault_mode == 3);
				rdmaHw->SetRpingmeshTracerouteOnLoopFault(fat_tree_fault_mode == 2);
				rdmaHw->SetRpingmeshTracerouteOnCongestionFault(fat_tree_fault_mode == 4);
			}
			// create and install RdmaDriver
			Ptr<RdmaDriver> rdma = CreateObject<RdmaDriver>();
			Ptr<Node> node = n.Get(i);
			rdma->SetNode(node);
			rdma->SetRdmaHw(rdmaHw);

			node->AggregateObject (rdma);
			rdma->Init();
			rdma->TraceConnectWithoutContext("QpComplete", MakeBoundCallback (qp_finish, fct_output));
		}
		if (InitShouldLogProgress(i + 1, node_num, 10000))
			InitLogProgress("install RDMA drivers", i + 1, node_num);
	}
	InitLog("RDMA driver install done");
	#endif

	// set ACK priority on hosts
	if (ack_high_prio)
		RdmaEgressQueue::ack_q_idx = 0;
	else
		RdmaEgressQueue::ack_q_idx = 3;

	// setup routing
	InitLog("routing setup begin");
	if (topology_mode == TOPOLOGY_FAT_TREE)
		SetFatTreeRoutingEntries();
	else if (topology_mode == TOPOLOGY_LEAF_SPINE)
		SetLeafSpineRoutingEntries();
	else{
		CalculateRoutes(n);
		SetRoutingEntries();
		coverageNextHop = nextHop;
	}
	InitLog("routing setup done");

	//
	// get BDP and delay
	//
	InitLog("pair metric setup begin");
	maxRtt = maxBdp = 0;
	if (topology_mode == TOPOLOGY_FAT_TREE){
		uint32_t dst = fatTree.hostCount > fatTree.hostsPerPod ? fatTree.hostsPerPod : (fatTree.hostCount - 1);
		PairInfo pair = GetFatTreePairInfo(0, dst);
		maxRtt = pair.rtt;
		maxBdp = pair.bdp;
	}else if (topology_mode == TOPOLOGY_LEAF_SPINE){
		uint32_t dst = leafSpine.hostCount > leafSpine.half ? leafSpine.half : (leafSpine.hostCount - 1);
		PairInfo pair = GetLeafSpinePairInfo(0, dst);
		maxRtt = pair.rtt;
		maxBdp = pair.bdp;
	}else{
		for (uint32_t i = 0; i < node_num; i++){
			if (n.Get(i)->GetNodeType() != 0)
				continue;
			for (uint32_t j = 0; j < node_num; j++){
				if (n.Get(j)->GetNodeType() != 0)
					continue;
				uint64_t delay = pairDelay[n.Get(i)][n.Get(j)];
				uint64_t txDelay = pairTxDelay[n.Get(i)][n.Get(j)];
				uint64_t rtt = delay * 2 + txDelay;
				uint64_t bw = pairBw[i][j];
				uint64_t bdp = rtt * bw / 1000000000/8;
				pairBdp[n.Get(i)][n.Get(j)] = bdp;
				pairRtt[i][j] = rtt;
				if (bdp > maxBdp)
					maxBdp = bdp;
				if (rtt > maxRtt)
					maxRtt = rtt;
			}
		}
	}
	printf("maxRtt=%lu maxBdp=%lu\n", maxRtt, maxBdp);
	InitLog("pair metric setup done");

	//
	// setup switch CC
	//
	InitLog("switch CC setup begin");
	for (uint32_t i = 0; i < node_num; i++){
		if (n.Get(i)->GetNodeType() == 1){ // switch
			Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
			sw->SetAttribute("CcMode", UintegerValue(cc_mode));
			sw->SetAttribute("MaxRtt", UintegerValue(maxRtt));
		}
		if (InitShouldLogProgress(i + 1, node_num, 10000))
			InitLogProgress("set switch CC", i + 1, node_num);
	}
	InitLog("switch CC setup done");

	//
	// add trace
	//
	InitLog("trace setup begin");

	NodeContainer trace_nodes;
	for (uint32_t i = 0; i < trace_num; i++)
	{
		uint32_t nid;
		tracef >> nid;
		if (nid >= n.GetN()){
			continue;
		}
		trace_nodes = NodeContainer(trace_nodes, n.Get(nid));
	}

	FILE *trace_output = fopen(trace_output_file.c_str(), "w");
	if (enable_trace)
		qbb.EnableTracing(trace_output, trace_nodes);
	InitLog("packet trace setup done");

	// dump link speed to trace file
	{
		InitLog("sim setting serialization begin");
		SimSetting sim_setting;
		if (topology_mode == TOPOLOGY_FAT_TREE){
			for (uint32_t i = 0; i < node_num; i++){
				for (uint32_t j = 1; j < n.Get(i)->GetNDevices(); j++){
					Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(n.Get(i)->GetDevice(j));
					if (dev != NULL)
						sim_setting.port_speed[i][j] = dev->GetDataRate().GetBitRate();
				}
			}
		}else{
			for (auto i: nbr2if){
				for (auto j : i.second){
					uint32_t node = i.first->GetId();
					uint8_t intf = j.second.idx;
					uint64_t bps = DynamicCast<QbbNetDevice>(i.first->GetDevice(j.second.idx))->GetDataRate().GetBitRate();
					sim_setting.port_speed[node][intf] = bps;
				}
			}
		}
		sim_setting.win = maxBdp;
		sim_setting.Serialize(trace_output);
	}
	InitLog("sim setting serialization done");

	if (UsesGeneralGraphRouting() && !IsRpingmeshLikeCcMode(cc_mode))
		Ipv4GlobalRoutingHelper::PopulateRoutingTables();
	InitLog("global routing setup done");

	NS_LOG_INFO("Create Applications.");

	Time interPacketInterval = Seconds(0.0000005 / 2);

	// Port numbers are initialized lazily per host pair in ScheduleFlowInputs().

	flow_input.idx = 0;
	if (flow_num > 0){
		ReadFlowInput();
		Simulator::Schedule(Seconds(flow_input.start_time)-Simulator::Now(), ScheduleFlowInputs);
	}
	InitLog("flow scheduling done");

	topof.close();
	tracef.close();

	// schedule link down
	if (link_down_time > 0){
		Simulator::Schedule(Seconds(2) + MicroSeconds(link_down_time), &TakeDownLink, n, n.Get(link_down_A), n.Get(link_down_B));
	}

	if (!switch_qlen_trace_output_file.empty()){
		NS_ASSERT_MSG(switch_qlen_trace_node >= 0, "SWITCH_QLEN_TRACE_NODE must be set when SWITCH_QLEN_TRACE_OUTPUT_FILE is set");
		NS_ASSERT_MSG((uint32_t)switch_qlen_trace_node < n.GetN(), "SWITCH_QLEN_TRACE_NODE is out of range");
		NS_ASSERT_MSG(n.Get((uint32_t)switch_qlen_trace_node)->GetNodeType() == 1, "SWITCH_QLEN_TRACE_NODE must be a switch");
		NS_ASSERT_MSG(switch_qlen_trace_interval_ns > 0, "SWITCH_QLEN_TRACE_INTERVAL_NS must be positive");
		Ptr<SwitchNode> traceSw = DynamicCast<SwitchNode>(n.Get((uint32_t)switch_qlen_trace_node));
		uint32_t loopbackPort = traceSw->GetOppLoopbackPort();
		NS_ASSERT_MSG(loopbackPort < traceSw->GetNDevices(), "SWITCH_QLEN_TRACE_NODE must have an OPP loopback port");
		Ptr<QbbNetDevice> loopbackDev = DynamicCast<QbbNetDevice>(traceSw->GetDevice(loopbackPort));
		NS_ASSERT_MSG(loopbackDev != NULL && loopbackDev->IsSwitchLoopback(), "SWITCH_QLEN_TRACE_NODE loopback port is invalid");
		switch_qlen_trace_output = fopen(switch_qlen_trace_output_file.c_str(), "w");
		NS_ASSERT_MSG(switch_qlen_trace_output != NULL, "failed to open SWITCH_QLEN_TRACE_OUTPUT_FILE");
		fprintf(switch_qlen_trace_output, "time_ns,relative_ns,relative_us,node,loopback_port,loopback_qlen_bytes,loopback_tx_bytes_700ns,loopback_tx_rate_700ns_gbps\n");
		fflush(switch_qlen_trace_output);
	}

	// schedule buffer monitor
	FILE* qlen_output = fopen(qlen_mon_file.c_str(), "w");
	Simulator::Schedule(NanoSeconds(qlen_mon_start), &monitor_buffer, qlen_output, &n);

	//
	// Now, do the actual simulation.
	//
	std::cout << "Running Simulation.\n";
	fflush(stdout);
	InitLog("Simulator::Run begin");
	NS_LOG_INFO("Run Simulation.");
	Simulator::Stop(Seconds(simulator_stop_time));
	Simulator::Run();
	InitLog("Simulator::Run done");
	DumpFlowCoverage();
	DumpSwitchProbeAckRxCounts();
	DumpOppLoopbackStats();
	DumpOppLoopbackPeakBuckets();
	DumpOppLoopbackRateTrace();
	DumpOppLoopbackTimeseries();
	DumpRdProbeDiagnosis();
	DumpRpingmeshDiagnosis();
	DumpOppDiagnosis();
	if (rdprobe_diag_file != NULL){
		fclose(rdprobe_diag_file);
		rdprobe_diag_file = NULL;
	}
	if (opp_probe_delay_output != NULL){
		fclose(opp_probe_delay_output);
		opp_probe_delay_output = NULL;
	}
	if (switch_qlen_trace_output != NULL){
		fclose(switch_qlen_trace_output);
		switch_qlen_trace_output = NULL;
	}
	Simulator::Destroy();
	NS_LOG_INFO("Done.");
	fclose(trace_output);

	endt = clock();
	std::cout << (double)(endt - begint) / CLOCKS_PER_SEC << "\n";

}
