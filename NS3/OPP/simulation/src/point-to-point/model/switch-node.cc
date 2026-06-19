#include "ns3/ipv4.h"
#include "ns3/packet.h"
#include "ns3/ipv4-header.h"
#include "ns3/udp-header.h"
#include "ns3/seq-ts-header.h"
#include "ns3/tag.h"
#include "ns3/tag-buffer.h"
#include "ns3/pause-header.h"
#include "ns3/flow-id-tag.h"
#include "ns3/boolean.h"
#include "ns3/uinteger.h"
#include "ns3/double.h"
#include "ns3/simulator.h"
#include "ns3/log.h"
#include "ns3/mac48-address.h"
#include "ns3/broadcom-egress-queue.h"
#include "ns3/opp-probe-header.h"
#include "ns3/opp-ack-header.h"
#include "switch-node.h"
#include "qbb-net-device.h"
#include "qbb-header.h"
#include "rdma-hw.h"
#include "ppp-header.h"
#include "ns3/int-header.h"
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>

NS_LOG_COMPONENT_DEFINE("SwitchNode");

namespace ns3 {

static const uint32_t FAT_TREE_FAULT_NONE = 0;
static const uint32_t FAT_TREE_FAULT_ACL_DROP = 1;
static const uint32_t FAT_TREE_FAULT_LOOP = 2;
static const uint32_t FAT_TREE_FAULT_LINK_FLAPPING = 3;
static const uint32_t FAT_TREE_FAULT_CONGESTION = 4;
static const uint32_t FAT_TREE_FAULT_TARGET_ANY = 0xffffffffu;
static const uint64_t OPP_LOOPBACK_TX_SAMPLE_INTERVAL_10US_NS = 10000;
static const uint64_t OPP_LOOPBACK_TX_SAMPLE_INTERVAL_1US_NS = 1000;
static const uint64_t OPP_LOOPBACK_TX_SAMPLE_INTERVAL_700NS_NS = 700;
static const uint64_t OPP_LOOPBACK_TX_SAMPLE_INTERVAL_100NS_NS = 100;

struct OppUsmCopyStatsRecord {
	uint64_t ingressTimeNs;
	uint32_t nodeId;
	uint32_t inDev;
	uint32_t candidateCount;
	uint64_t lastEgressDoneTimeNs;
	uint32_t remainingCopies;
	bool dispatchDone;

	OppUsmCopyStatsRecord()
		: ingressTimeNs(0),
		  nodeId(0),
		  inDev(0),
		  candidateCount(0),
		  lastEgressDoneTimeNs(0),
		  remainingCopies(0),
		  dispatchDone(false) {}
	OppUsmCopyStatsRecord(uint64_t ingressTimeNs, uint32_t nodeId, uint32_t inDev, uint32_t candidateCount)
		: ingressTimeNs(ingressTimeNs),
		  nodeId(nodeId),
		  inDev(inDev),
		  candidateCount(candidateCount),
		  lastEgressDoneTimeNs(0),
		  remainingCopies(0),
		  dispatchDone(false) {}
};

static uint64_t g_oppUsmCopyStatsNextId = 1;
static uint64_t g_oppUsmCopyStatsCount = 0;
static uint64_t g_oppUsmCopyStatsCompletedCount = 0;
static uint64_t g_oppUsmCopyStatsMinNs = std::numeric_limits<uint64_t>::max();
static uint64_t g_oppUsmCopyStatsMaxNs = 0;
static uint64_t g_oppUsmCopyStatsSumNs = 0;
static bool g_oppUsmCopyLatencyDetailEnabled = false;
static std::unordered_map<uint64_t, OppUsmCopyStatsRecord> g_oppUsmCopyStatsRecords;

class OppEgressTag : public Tag
{
public:
	static TypeId GetTypeId(void)
	{
		static TypeId tid = TypeId("ns3::OppEgressTag")
			.SetParent<Tag>()
			.AddConstructor<OppEgressTag>()
			;
		return tid;
	}

	OppEgressTag() : m_enqueueTimeNs(0), m_outDev(0) {}
	OppEgressTag(uint64_t enqueueTimeNs, uint32_t outDev)
		: m_enqueueTimeNs(enqueueTimeNs), m_outDev(outDev) {}

	virtual TypeId GetInstanceTypeId(void) const
	{
		return GetTypeId();
	}

	virtual uint32_t GetSerializedSize(void) const
	{
		return sizeof(m_enqueueTimeNs) + sizeof(m_outDev);
	}

	virtual void Serialize(TagBuffer i) const
	{
		i.WriteU64(m_enqueueTimeNs);
		i.WriteU32(m_outDev);
	}

	virtual void Deserialize(TagBuffer i)
	{
		m_enqueueTimeNs = i.ReadU64();
		m_outDev = i.ReadU32();
	}

	virtual void Print(std::ostream &os) const
	{
		os << "enqueue=" << m_enqueueTimeNs << ",outDev=" << m_outDev;
	}

	uint64_t GetEnqueueTimeNs(void) const { return m_enqueueTimeNs; }
	uint32_t GetOutDev(void) const { return m_outDev; }

private:
	uint64_t m_enqueueTimeNs;
	uint32_t m_outDev;
};

NS_OBJECT_ENSURE_REGISTERED(OppEgressTag);

class OppUsmCopyStatsTag : public Tag
{
public:
	static TypeId GetTypeId(void)
	{
		static TypeId tid = TypeId("ns3::OppUsmCopyStatsTag")
			.SetParent<Tag>()
			.AddConstructor<OppUsmCopyStatsTag>()
			;
		return tid;
	}

	OppUsmCopyStatsTag() : m_trackingId(0) {}
	OppUsmCopyStatsTag(uint64_t trackingId) : m_trackingId(trackingId) {}

	virtual TypeId GetInstanceTypeId(void) const
	{
		return GetTypeId();
	}

	virtual uint32_t GetSerializedSize(void) const
	{
		return sizeof(m_trackingId);
	}

	virtual void Serialize(TagBuffer i) const
	{
		i.WriteU64(m_trackingId);
	}

	virtual void Deserialize(TagBuffer i)
	{
		m_trackingId = i.ReadU64();
	}

	virtual void Print(std::ostream &os) const
	{
		os << "tracking_id=" << m_trackingId;
	}

	uint64_t GetTrackingId(void) const { return m_trackingId; }

private:
	uint64_t m_trackingId;
};

NS_OBJECT_ENSURE_REGISTERED(OppUsmCopyStatsTag);

class OppUsmTag : public Tag
{
public:
	static const uint32_t kWordCount = (SwitchMmu::pCnt + 31) / 32;

	static TypeId GetTypeId(void)
	{
		static TypeId tid = TypeId("ns3::OppUsmTag")
			.SetParent<Tag>()
			.AddConstructor<OppUsmTag>()
			;
		return tid;
	}

	OppUsmTag()
	{
		Reset();
	}

	virtual TypeId GetInstanceTypeId(void) const
	{
		return GetTypeId();
	}

	virtual uint32_t GetSerializedSize(void) const
	{
		return sizeof(m_candidatePorts) + sizeof(m_sentPorts);
	}

	virtual void Serialize(TagBuffer i) const
	{
		for (uint32_t word = 0; word < kWordCount; word++)
			i.WriteU32(m_candidatePorts[word]);
		for (uint32_t word = 0; word < kWordCount; word++)
			i.WriteU32(m_sentPorts[word]);
	}

	virtual void Deserialize(TagBuffer i)
	{
		for (uint32_t word = 0; word < kWordCount; word++)
			m_candidatePorts[word] = i.ReadU32();
		for (uint32_t word = 0; word < kWordCount; word++)
			m_sentPorts[word] = i.ReadU32();
	}

	virtual void Print(std::ostream &os) const
	{
		os << "candidates=" << CountCandidates()
		   << ",sent=" << CountSent()
		   << ",unsent=" << CountUnsent();
	}

	void Reset(void)
	{
		std::memset(m_candidatePorts, 0, sizeof(m_candidatePorts));
		std::memset(m_sentPorts, 0, sizeof(m_sentPorts));
	}

	void SetCandidate(uint32_t port)
	{
		NS_ASSERT_MSG(port < SwitchMmu::pCnt, "OPP USM candidate port exceeds tag bitmap size");
		m_candidatePorts[port / 32] |= (1u << (port % 32));
	}

	void SetSent(uint32_t port)
	{
		NS_ASSERT_MSG(port < SwitchMmu::pCnt, "OPP USM sent port exceeds tag bitmap size");
		m_sentPorts[port / 32] |= (1u << (port % 32));
	}

	bool IsCandidate(uint32_t port) const
	{
		if (port >= SwitchMmu::pCnt)
			return false;
		return (m_candidatePorts[port / 32] & (1u << (port % 32))) != 0;
	}

	bool IsSent(uint32_t port) const
	{
		if (port >= SwitchMmu::pCnt)
			return false;
		return (m_sentPorts[port / 32] & (1u << (port % 32))) != 0;
	}

	uint32_t CountCandidates(void) const
	{
		uint32_t count = 0;
		for (uint32_t port = 0; port < SwitchMmu::pCnt; port++){
			if (IsCandidate(port))
				count++;
		}
		return count;
	}

	uint32_t CountSent(void) const
	{
		uint32_t count = 0;
		for (uint32_t port = 0; port < SwitchMmu::pCnt; port++){
			if (IsCandidate(port) && IsSent(port))
				count++;
		}
		return count;
	}

	uint32_t CountUnsent(void) const
	{
		uint32_t count = 0;
		for (uint32_t port = 0; port < SwitchMmu::pCnt; port++){
			if (IsCandidate(port) && !IsSent(port))
				count++;
		}
		return count;
	}

	bool HasUnsent(void) const
	{
		return CountUnsent() > 0;
	}

	bool PickCandidateByHash(uint32_t hash, uint32_t &port) const
	{
		uint32_t candidateCount = CountCandidates();
		if (candidateCount == 0)
			return false;
		uint32_t target = hash % candidateCount;
		for (uint32_t i = 0; i < SwitchMmu::pCnt; i++){
			if (!IsCandidate(i))
				continue;
			if (target == 0){
				port = i;
				return true;
			}
			target--;
		}
		return false;
	}

private:
	uint32_t m_candidatePorts[kWordCount];
	uint32_t m_sentPorts[kWordCount];
};

NS_OBJECT_ENSURE_REGISTERED(OppUsmTag);

TypeId SwitchNode::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::SwitchNode")
    .SetParent<Node> ()
    .AddConstructor<SwitchNode> ()
	.AddAttribute("EcnEnabled",
			"Enable ECN marking.",
			BooleanValue(false),
			MakeBooleanAccessor(&SwitchNode::m_ecnEnabled),
			MakeBooleanChecker())
	.AddAttribute("CcMode",
			"CC mode.",
			UintegerValue(0),
			MakeUintegerAccessor(&SwitchNode::m_ccMode),
			MakeUintegerChecker<uint32_t>())
	.AddAttribute("AckHighPrio",
			"Set high priority for ACK/NACK or not",
			UintegerValue(0),
			MakeUintegerAccessor(&SwitchNode::m_ackHighPrio),
			MakeUintegerChecker<uint32_t>())
	.AddAttribute("MaxRtt",
			"Max Rtt of the network",
			UintegerValue(9000),
			MakeUintegerAccessor(&SwitchNode::m_maxRtt),
			MakeUintegerChecker<uint32_t>())
  ;
  return tid;
}

SwitchNode::SwitchNode(){
	m_ecmpSeed = m_id;
	m_ecmpMode = ECMP_MODE_FLOW_HASH;
	m_oppMulticastMode = OPP_MULTICAST_STANDARD;
	m_oppLoopbackPort = OPP_LOOPBACK_PORT_INVALID;
	m_oppLoopbackRxProbeCount = 0;
	m_oppLoopbackPeakTxRate10usBps = 0;
	m_oppLoopbackTxWindow10usStartNs = 0;
	m_oppLoopbackTxWindow10usBytes = 0;
	m_oppLoopbackTxWindow10usValid = false;
	m_oppLoopbackPeakTxRate1usBps = 0;
	m_oppLoopbackTxWindow1usStartNs = 0;
	m_oppLoopbackTxWindow1usBytes = 0;
	m_oppLoopbackTxWindow1usValid = false;
	m_oppLoopbackPeakTxRate700nsBps = 0;
	m_oppLoopbackTxWindow700nsStartNs = 0;
	m_oppLoopbackTxWindow700nsBytes = 0;
	m_oppLoopbackTxWindow700nsValid = false;
	m_oppLoopbackPeakTxRate100nsBps = 0;
	m_oppLoopbackTxWindow100nsStartNs = 0;
	m_oppLoopbackTxWindow100nsBytes = 0;
	m_oppLoopbackTxWindow100nsValid = false;
	m_oppLoopbackTxStatsFinalized = false;
	m_oppLoopbackPeakQueueBytes = 0;
	m_probeAckRxCount = 0;
	m_fatTreeFaultMode = FAT_TREE_FAULT_NONE;
	m_fatTreeFaultTargetOutDev = 0xffffffffu;
	m_fatTreeFaultTargetInDev = FAT_TREE_FAULT_TARGET_ANY;
	m_fatTreeFaultSeed = 1;
	m_fatTreeFaultRandomCounter = 0;
	m_fatTreeFaultCongestionDelayNs = 500000;
	m_node_type = 1;
	m_mmu = CreateObject<SwitchMmu>();
	for (uint32_t i = 0; i < pCnt; i++)
		for (uint32_t j = 0; j < pCnt; j++)
			for (uint32_t k = 0; k < qCnt; k++)
				m_bytes[i][j][k] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_txBytes[i] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_lastPktSize[i] = m_lastPktTs[i] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_u[i] = 0;
}

int SwitchNode::GetOutDev(Ptr<const Packet> p, CustomHeader &ch){
	if (m_routeProvider != NULL){
		int providerPort = m_routeProvider->GetOutDev(ch);
		if (providerPort == -2)
			return GetLiveEcmpPort(m_providerEcmpPorts, ch);
		if (providerPort >= 0 && m_devices[providerPort]->IsLinkUp())
			return providerPort;
		return -1;
	}

	// look up entries
	auto entry = m_rtTable.find(ch.dip);

	// no matching entry
	if (entry == m_rtTable.end())
		return -1;

	// entry found
	auto &nexthops = entry->second;

	// pick one next hop based on hash
	uint32_t idx = GetPacketHash(ch) % nexthops.size();
	return nexthops[idx];
}

uint32_t SwitchNode::GetPacketHash(CustomHeader &ch){
	union {
		uint8_t u8[4+4+2+2+8];
		uint32_t u32[5];
	} buf;
	buf.u32[0] = ch.sip;
	buf.u32[1] = ch.dip;
	buf.u32[2] = 0;
	if (ch.l3Prot == 0x6)
		buf.u32[2] = ch.tcp.sport | ((uint32_t)ch.tcp.dport << 16);
	else if (ch.l3Prot == 0x11)
		buf.u32[2] = ch.udp.sport | ((uint32_t)ch.udp.dport << 16);
	else if (ch.l3Prot == 0xFC || ch.l3Prot == 0xFD)
		buf.u32[2] = ch.ack.sport | ((uint32_t)ch.ack.dport << 16);
	size_t hashLen = 12;
	if (m_ecmpMode == ECMP_MODE_PACKET_SPRAY){
		uint64_t ts = Simulator::Now().GetTimeStep();
		std::memcpy(buf.u8 + hashLen, &ts, sizeof(ts));
		hashLen += sizeof(ts);
	}
	return EcmpHash(buf.u8, hashLen, m_ecmpSeed);
}

int SwitchNode::GetLiveEcmpPort(const std::vector<int> &ports, CustomHeader &ch){
	uint32_t liveCnt = 0;
	for (uint32_t i = 0; i < ports.size(); i++){
		int port = ports[i];
		if (port >= 0 && m_devices[port]->IsLinkUp())
			liveCnt++;
	}
	if (liveCnt == 0)
		return -1;
	uint32_t target = GetPacketHash(ch) % liveCnt;
	for (uint32_t i = 0; i < ports.size(); i++){
		int port = ports[i];
		if (port >= 0 && m_devices[port]->IsLinkUp()){
			if (target == 0)
				return port;
			target--;
		}
	}
	return -1;
}

void SwitchNode::CheckAndSendPfc(uint32_t inDev, uint32_t qIndex){
	Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
	if (m_mmu->CheckShouldPause(inDev, qIndex)){
		device->SendPfc(qIndex, 0);
		m_mmu->SetPause(inDev, qIndex);
	}
}
void SwitchNode::CheckAndSendResume(uint32_t inDev, uint32_t qIndex){
	Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
	if (m_mmu->CheckShouldResume(inDev, qIndex)){
		device->SendPfc(qIndex, 1);
		m_mmu->SetResume(inDev, qIndex);
	}
}

bool SwitchNode::EnqueueToDev(uint32_t inDev, uint32_t outDev, Ptr<Packet> p, CustomHeader &ch, bool oppProbe, uint64_t oppUsmStatsTrackingId){
	bool loopbackOut = IsOppLoopbackPort(outDev);
	if (loopbackOut){
		NS_ASSERT_MSG(oppProbe && m_ccMode == RDMA_CC_MODE_OPP && ch.l3Prot == 0x11,
			"Only OPP probes may use the switch loopback port");
	}
	NS_ASSERT_MSG(outDev < m_devices.size() && m_devices[outDev]->IsLinkUp(), "The routing table look up should return link that is up");
	if (!loopbackOut && ApplyFatTreeFaultBeforeEnqueue(inDev, outDev, ch))
		return true;
	NS_ASSERT_MSG(outDev < m_devices.size() && m_devices[outDev]->IsLinkUp(), "Fat-tree fault injection must keep output link valid");
	if (!loopbackOut && oppProbe && ch.l3Prot == 0x11 && ShouldDropOppProbeForDebug(outDev)){
		NS_LOG_INFO("node " << GetId() << " debug-drops OPP probe inDev "
			<< inDev << " outDev " << outDev);
		return true;
	}
	if (!DecrementRpingmeshProbeTtl(inDev, p, ch)){
		NS_LOG_INFO("node " << GetId() << " drops RPINGMESH probe because TTL expired inDev "
			<< inDev << " outDev " << outDev);
		return true;
	}

	// determine the qIndex
	uint32_t qIndex;
	if (ch.l3Prot == 0xFF || ch.l3Prot == 0xFE || (m_ackHighPrio && (ch.l3Prot == 0xFD || ch.l3Prot == 0xFC))){  //QCN or PFC or NACK, go highest priority
		qIndex = 0;
	}else{
		qIndex = (ch.l3Prot == 0x06 ? 1 : ch.udp.pg); // if TCP, put to queue 1
	}

	// admission control
	if (qIndex != 0){ //not highest priority
		if (m_mmu->CheckIngressAdmission(inDev, qIndex, p->GetSize()) && m_mmu->CheckEgressAdmission(outDev, qIndex, p->GetSize())){			// Admission control
			m_mmu->UpdateIngressAdmission(inDev, qIndex, p->GetSize());
			m_mmu->UpdateEgressAdmission(outDev, qIndex, p->GetSize());
		}else{
			return false; // Drop
		}
		if (!IsOppLoopbackPort(inDev))
			CheckAndSendPfc(inDev, qIndex);
	}
	if (!loopbackOut && ch.l3Prot == 0x11 && !m_packetForwardCallback.IsNull())
		m_packetForwardCallback(ch, GetId(), inDev, outDev);
	if (oppProbe && !loopbackOut)
		p->AddPacketTag(OppEgressTag(Simulator::Now().GetTimeStep(), outDev));
	if (loopbackOut){
		Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(m_devices[outDev]);
		if (dev != NULL && dev->GetQueue() != NULL){
			uint64_t queueBytes = (uint64_t)dev->GetQueue()->GetNBytesTotal() + p->GetSize();
			if (queueBytes > m_oppLoopbackPeakQueueBytes)
				m_oppLoopbackPeakQueueBytes = queueBytes;
		}
	}
	m_bytes[inDev][outDev][qIndex] += p->GetSize();
	if (oppUsmStatsTrackingId != 0)
		AccountOppUsmCopyEnqueue(oppUsmStatsTrackingId);
	m_devices[outDev]->SwitchSend(qIndex, p, ch);
	return true;
}

void SwitchNode::SendToDev(Ptr<Packet>p, CustomHeader &ch){
	int idx = GetOutDev(p, ch);
	if (idx >= 0){
		FlowIdTag t;
		p->PeekPacketTag(t);
		bool oppProbe = BypassOppCache() && m_ccMode == RDMA_CC_MODE_OPP && ch.l3Prot == 0x11;
		EnqueueToDev(t.GetFlowId(), idx, p, ch, oppProbe);
	}else
		return; // Drop
}

uint32_t SwitchNode::EcmpHash(const uint8_t* key, size_t len, uint32_t seed) {
  uint32_t h = seed;
  if (len > 3) {
    const uint32_t* key_x4 = (const uint32_t*) key;
    size_t i = len >> 2;
    do {
      uint32_t k = *key_x4++;
      k *= 0xcc9e2d51;
      k = (k << 15) | (k >> 17);
      k *= 0x1b873593;
      h ^= k;
      h = (h << 13) | (h >> 19);
      h += (h << 2) + 0xe6546b64;
    } while (--i);
    key = (const uint8_t*) key_x4;
  }
  if (len & 3) {
    size_t i = len & 3;
    uint32_t k = 0;
    key = &key[i - 1];
    do {
      k <<= 8;
      k |= *key--;
    } while (--i);
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    h ^= k;
  }
  h ^= len;
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

void SwitchNode::SetEcmpSeed(uint32_t seed){
	m_ecmpSeed = seed;
}

void SwitchNode::SetPacketForwardCallback(PacketForwardCallback cb){
	m_packetForwardCallback = cb;
}

void SwitchNode::SetPacketReceiveCallback(PacketReceiveCallback cb){
	m_packetReceiveCallback = cb;
}

void SwitchNode::SetOppLoopbackTxCallback(OppLoopbackTxCallback cb){
	m_oppLoopbackTxCallback = cb;
}

void SwitchNode::SetRdProbeTraceCallback(RdProbeTraceCallback cb){
	m_rdProbeTraceCallback = cb;
}

void SwitchNode::ConfigureFatTreeFault(uint32_t mode, uint32_t targetOutDev, uint32_t seed, uint32_t congestionDelayNs, uint32_t targetInDev){
	NS_ASSERT_MSG(mode <= FAT_TREE_FAULT_CONGESTION, "invalid fat-tree fault mode");
	m_fatTreeFaultMode = mode;
	m_fatTreeFaultTargetOutDev = targetOutDev;
	m_fatTreeFaultTargetInDev = targetInDev;
	m_fatTreeFaultSeed = seed;
	m_fatTreeFaultRandomCounter = 0;
	m_fatTreeFaultCongestionDelayNs = congestionDelayNs;
}

void SwitchNode::SetEcmpMode(uint32_t mode){
	NS_ASSERT_MSG(mode == ECMP_MODE_FLOW_HASH || mode == ECMP_MODE_PACKET_SPRAY, "invalid ECMP mode");
	m_ecmpMode = mode;
}

void SwitchNode::SetOppMulticastMode(uint32_t mode){
	NS_ASSERT_MSG(mode == OPP_MULTICAST_STANDARD || mode == OPP_MULTICAST_USM || mode == OPP_MULTICAST_SAMPLED,
		"invalid OPP multicast mode");
	m_oppMulticastMode = mode;
}

uint32_t SwitchNode::InstallOppLoopbackDevice(DataRate rate){
	if (m_oppLoopbackPort != OPP_LOOPBACK_PORT_INVALID)
		return m_oppLoopbackPort;
	Ptr<QbbNetDevice> dev = CreateObject<QbbNetDevice>();
	dev->SetAddress(Mac48Address::Allocate());
	dev->SetDataRate(rate);
	dev->SetQueue(CreateObject<BEgressQueue>());
	uint32_t port = AddDevice(dev);
	NS_ASSERT_MSG(port < pCnt, "OPP loopback port exceeds switch port bitmap size");
	dev->SetSwitchLoopback(true);
	m_oppLoopbackPort = port;
	NS_LOG_INFO("node " << GetId() << " installs OPP loopback port " << m_oppLoopbackPort
		<< " rate " << rate.GetBitRate());
	return m_oppLoopbackPort;
}

uint32_t SwitchNode::GetOppLoopbackPort(void) const{
	return m_oppLoopbackPort;
}

void SwitchNode::SetRouteProvider(Ptr<SwitchRouteProvider> routeProvider, const std::vector<int> &ecmpPorts){
	m_routeProvider = routeProvider;
	m_providerEcmpPorts = ecmpPorts;
}

void SwitchNode::AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx){
	uint32_t dip = dstAddr.Get();
	m_rtTable[dip].push_back(intf_idx);
}

void SwitchNode::ClearTable(){
	m_rtTable.clear();
}

uint64_t SwitchNode::GetProbeAckRxCount() const{
	return m_probeAckRxCount;
}

void SwitchNode::UpdateOppLoopbackPeakTxRateForWindow(
	uint64_t intervalNs, bool windowValid, uint64_t windowBytes, uint64_t &peakRateBps){
	if (!windowValid)
		return;
	long double rate = (long double)windowBytes * 8.0e9 / (long double)intervalNs;
	uint64_t rateBps = rate > (long double)std::numeric_limits<uint64_t>::max() ?
		std::numeric_limits<uint64_t>::max() : (uint64_t)rate;
	if (rateBps > peakRateBps)
		peakRateBps = rateBps;
}

void SwitchNode::AccountOppLoopbackTxForWindow(
	uint64_t now, uint32_t bytes, uint64_t intervalNs,
	bool &windowValid, uint64_t &windowStartNs, uint64_t &windowBytes,
	uint64_t &peakRateBps, std::vector<OppLoopbackTxWindowRecord> &records){
	uint64_t windowStart = (now / intervalNs) * intervalNs;
	if (!windowValid){
		windowValid = true;
		windowStartNs = windowStart;
		windowBytes = 0;
	}else if (windowStart != windowStartNs){
		UpdateOppLoopbackPeakTxRateForWindow(intervalNs, windowValid, windowBytes, peakRateBps);
		if (windowBytes > 0)
			records.push_back(OppLoopbackTxWindowRecord(windowStartNs, windowBytes));
		windowStartNs = windowStart;
		windowBytes = 0;
	}
	windowBytes += bytes;
}

void SwitchNode::FinalizeOppLoopbackTxForWindow(
	uint64_t intervalNs, bool windowValid, uint64_t windowStartNs,
	uint64_t windowBytes, uint64_t &peakRateBps,
	std::vector<OppLoopbackTxWindowRecord> &records){
	UpdateOppLoopbackPeakTxRateForWindow(intervalNs, windowValid, windowBytes, peakRateBps);
	if (windowValid && windowBytes > 0)
		records.push_back(OppLoopbackTxWindowRecord(windowStartNs, windowBytes));
}

void SwitchNode::AccountOppLoopbackTxForPeakOnlyWindow(
	uint64_t now, uint32_t bytes, uint64_t intervalNs,
	bool &windowValid, uint64_t &windowStartNs, uint64_t &windowBytes,
	uint64_t &peakRateBps){
	uint64_t windowStart = (now / intervalNs) * intervalNs;
	if (!windowValid){
		windowValid = true;
		windowStartNs = windowStart;
		windowBytes = 0;
	}else if (windowStart != windowStartNs){
		UpdateOppLoopbackPeakTxRateForWindow(intervalNs, windowValid, windowBytes, peakRateBps);
		windowStartNs = windowStart;
		windowBytes = 0;
	}
	windowBytes += bytes;
}

void SwitchNode::FinalizeOppLoopbackTxForPeakOnlyWindow(
	uint64_t intervalNs, bool windowValid, uint64_t windowBytes,
	uint64_t &peakRateBps){
	UpdateOppLoopbackPeakTxRateForWindow(intervalNs, windowValid, windowBytes, peakRateBps);
}

void SwitchNode::UpdateOppLoopbackPeakTxRate(void){
	UpdateOppLoopbackPeakTxRateForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_10US_NS,
		m_oppLoopbackTxWindow10usValid,
		m_oppLoopbackTxWindow10usBytes,
		m_oppLoopbackPeakTxRate10usBps);
	UpdateOppLoopbackPeakTxRateForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_1US_NS,
		m_oppLoopbackTxWindow1usValid,
		m_oppLoopbackTxWindow1usBytes,
		m_oppLoopbackPeakTxRate1usBps);
	UpdateOppLoopbackPeakTxRateForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_700NS_NS,
		m_oppLoopbackTxWindow700nsValid,
		m_oppLoopbackTxWindow700nsBytes,
		m_oppLoopbackPeakTxRate700nsBps);
	UpdateOppLoopbackPeakTxRateForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_100NS_NS,
		m_oppLoopbackTxWindow100nsValid,
		m_oppLoopbackTxWindow100nsBytes,
		m_oppLoopbackPeakTxRate100nsBps);
}

void SwitchNode::AccountOppLoopbackTx(uint32_t bytes){
	uint64_t now = Simulator::Now().GetTimeStep();
	AccountOppLoopbackTxForWindow(
		now, bytes, OPP_LOOPBACK_TX_SAMPLE_INTERVAL_10US_NS,
		m_oppLoopbackTxWindow10usValid,
		m_oppLoopbackTxWindow10usStartNs,
		m_oppLoopbackTxWindow10usBytes,
		m_oppLoopbackPeakTxRate10usBps,
		m_oppLoopbackTxWindows10us);
	AccountOppLoopbackTxForWindow(
		now, bytes, OPP_LOOPBACK_TX_SAMPLE_INTERVAL_1US_NS,
		m_oppLoopbackTxWindow1usValid,
		m_oppLoopbackTxWindow1usStartNs,
		m_oppLoopbackTxWindow1usBytes,
		m_oppLoopbackPeakTxRate1usBps,
		m_oppLoopbackTxWindows1us);
	AccountOppLoopbackTxForWindow(
		now, bytes, OPP_LOOPBACK_TX_SAMPLE_INTERVAL_700NS_NS,
		m_oppLoopbackTxWindow700nsValid,
		m_oppLoopbackTxWindow700nsStartNs,
		m_oppLoopbackTxWindow700nsBytes,
		m_oppLoopbackPeakTxRate700nsBps,
		m_oppLoopbackTxWindows700ns);
	AccountOppLoopbackTxForPeakOnlyWindow(
		now, bytes, OPP_LOOPBACK_TX_SAMPLE_INTERVAL_100NS_NS,
		m_oppLoopbackTxWindow100nsValid,
		m_oppLoopbackTxWindow100nsStartNs,
		m_oppLoopbackTxWindow100nsBytes,
		m_oppLoopbackPeakTxRate100nsBps);
}

void SwitchNode::FinalizeOppLoopbackStats(void){
	if (m_oppLoopbackTxStatsFinalized)
		return;
	FinalizeOppLoopbackTxForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_10US_NS,
		m_oppLoopbackTxWindow10usValid,
		m_oppLoopbackTxWindow10usStartNs,
		m_oppLoopbackTxWindow10usBytes,
		m_oppLoopbackPeakTxRate10usBps,
		m_oppLoopbackTxWindows10us);
	FinalizeOppLoopbackTxForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_1US_NS,
		m_oppLoopbackTxWindow1usValid,
		m_oppLoopbackTxWindow1usStartNs,
		m_oppLoopbackTxWindow1usBytes,
		m_oppLoopbackPeakTxRate1usBps,
		m_oppLoopbackTxWindows1us);
	FinalizeOppLoopbackTxForWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_700NS_NS,
		m_oppLoopbackTxWindow700nsValid,
		m_oppLoopbackTxWindow700nsStartNs,
		m_oppLoopbackTxWindow700nsBytes,
		m_oppLoopbackPeakTxRate700nsBps,
		m_oppLoopbackTxWindows700ns);
	FinalizeOppLoopbackTxForPeakOnlyWindow(
		OPP_LOOPBACK_TX_SAMPLE_INTERVAL_100NS_NS,
		m_oppLoopbackTxWindow100nsValid,
		m_oppLoopbackTxWindow100nsBytes,
		m_oppLoopbackPeakTxRate100nsBps);
	m_oppLoopbackTxStatsFinalized = true;
}

uint64_t SwitchNode::GetOppLoopbackRxProbeCount(void) const{
	return m_oppLoopbackRxProbeCount;
}

uint64_t SwitchNode::GetOppLoopbackPeakTxRateBps(void) const{
	return GetOppLoopbackPeakTxRate10usBps();
}

uint64_t SwitchNode::GetOppLoopbackPeakTxRate10usBps(void) const{
	return m_oppLoopbackPeakTxRate10usBps;
}

uint64_t SwitchNode::GetOppLoopbackPeakTxRate1usBps(void) const{
	return m_oppLoopbackPeakTxRate1usBps;
}

uint64_t SwitchNode::GetOppLoopbackPeakTxRate700nsBps(void) const{
	return m_oppLoopbackPeakTxRate700nsBps;
}

uint64_t SwitchNode::GetOppLoopbackPeakTxRate100nsBps(void) const{
	return m_oppLoopbackPeakTxRate100nsBps;
}

const std::vector<SwitchNode::OppLoopbackTxWindowRecord>&
SwitchNode::GetOppLoopbackTxWindows10us(void) const{
	return m_oppLoopbackTxWindows10us;
}

const std::vector<SwitchNode::OppLoopbackTxWindowRecord>&
SwitchNode::GetOppLoopbackTxWindows1us(void) const{
	return m_oppLoopbackTxWindows1us;
}

const std::vector<SwitchNode::OppLoopbackTxWindowRecord>&
SwitchNode::GetOppLoopbackTxWindows700ns(void) const{
	return m_oppLoopbackTxWindows700ns;
}

uint64_t SwitchNode::GetOppLoopbackPeakQueueBytes(void) const{
	return m_oppLoopbackPeakQueueBytes;
}

uint64_t SwitchNode::GetOppLoopbackTxSampleIntervalNs(void){
	return GetOppLoopbackTxSampleInterval10usNs();
}

uint64_t SwitchNode::GetOppLoopbackTxSampleInterval10usNs(void){
	return OPP_LOOPBACK_TX_SAMPLE_INTERVAL_10US_NS;
}

uint64_t SwitchNode::GetOppLoopbackTxSampleInterval1usNs(void){
	return OPP_LOOPBACK_TX_SAMPLE_INTERVAL_1US_NS;
}

uint64_t SwitchNode::GetOppLoopbackTxSampleInterval700nsNs(void){
	return OPP_LOOPBACK_TX_SAMPLE_INTERVAL_700NS_NS;
}

uint64_t SwitchNode::GetOppLoopbackTxSampleInterval100nsNs(void){
	return OPP_LOOPBACK_TX_SAMPLE_INTERVAL_100NS_NS;
}

uint64_t SwitchNode::GetOppUsmCopyStatsCount(void){
	return g_oppUsmCopyStatsCount;
}

uint64_t SwitchNode::GetOppUsmCopyStatsCompletedCount(void){
	return g_oppUsmCopyStatsCompletedCount;
}

uint64_t SwitchNode::GetOppUsmCopyStatsMinNs(void){
	return g_oppUsmCopyStatsCompletedCount == 0 ? 0 : g_oppUsmCopyStatsMinNs;
}

uint64_t SwitchNode::GetOppUsmCopyStatsMaxNs(void){
	return g_oppUsmCopyStatsMaxNs;
}

uint64_t SwitchNode::GetOppUsmCopyStatsSumNs(void){
	return g_oppUsmCopyStatsSumNs;
}

void SwitchNode::SetOppUsmCopyLatencyDetailEnabled(bool enabled){
	g_oppUsmCopyLatencyDetailEnabled = enabled;
}

bool SwitchNode::GetOppUsmCopyLatencyDetailEnabled(void){
	return g_oppUsmCopyLatencyDetailEnabled;
}

bool SwitchNode::BypassOppCache() const{
	return m_routeProvider != NULL && m_routeProvider->BypassOppCache();
}

bool SwitchNode::ShouldUseOppUsm(Ptr<Packet> packet) const{
	if (m_oppMulticastMode == OPP_MULTICAST_USM)
		return true;
	if (m_oppMulticastMode == OPP_MULTICAST_STANDARD)
		return false;

	OppMulticastDecisionTag tag;
	if (!packet->PeekPacketTag(tag)){
		NS_LOG_INFO("node " << GetId() << " sampled OPP probe has no multicast decision tag");
		return false;
	}
	return tag.GetUseUsm();
}

uint64_t SwitchNode::StartOppUsmCopyStats(uint64_t ingressTimeNs, uint32_t nodeId, uint32_t inDev, uint32_t candidateCount){
	uint64_t trackingId = g_oppUsmCopyStatsNextId++;
	g_oppUsmCopyStatsRecords[trackingId] = OppUsmCopyStatsRecord(ingressTimeNs, nodeId, inDev, candidateCount);
	g_oppUsmCopyStatsCount++;
	return trackingId;
}

void SwitchNode::AccountOppUsmCopyEnqueue(uint64_t trackingId){
	auto it = g_oppUsmCopyStatsRecords.find(trackingId);
	if (it == g_oppUsmCopyStatsRecords.end())
		return;
	it->second.remainingCopies++;
}

void SwitchNode::MarkOppUsmCopyDispatchDone(uint64_t trackingId){
	auto it = g_oppUsmCopyStatsRecords.find(trackingId);
	if (it != g_oppUsmCopyStatsRecords.end())
		it->second.dispatchDone = true;
	FinishOppUsmCopyStatsIfComplete(trackingId, Simulator::Now().GetTimeStep());
}

void SwitchNode::AccountOppUsmCopyEgress(uint64_t trackingId, uint64_t egressTimeNs){
	auto it = g_oppUsmCopyStatsRecords.find(trackingId);
	if (it == g_oppUsmCopyStatsRecords.end())
		return;
	if (egressTimeNs > it->second.lastEgressDoneTimeNs)
		it->second.lastEgressDoneTimeNs = egressTimeNs;
	if (it->second.remainingCopies > 0)
		it->second.remainingCopies--;
	FinishOppUsmCopyStatsIfComplete(trackingId, egressTimeNs);
}

void SwitchNode::FinishOppUsmCopyStatsIfComplete(uint64_t trackingId, uint64_t nowNs){
	auto it = g_oppUsmCopyStatsRecords.find(trackingId);
	if (it == g_oppUsmCopyStatsRecords.end())
		return;
	OppUsmCopyStatsRecord &record = it->second;
	if (!record.dispatchDone || record.remainingCopies != 0)
		return;
	uint64_t finishNs = record.lastEgressDoneTimeNs > nowNs ? record.lastEgressDoneTimeNs : nowNs;
	uint64_t latencyNs = finishNs >= record.ingressTimeNs ? finishNs - record.ingressTimeNs : 0;
	if (latencyNs < g_oppUsmCopyStatsMinNs)
		g_oppUsmCopyStatsMinNs = latencyNs;
	if (latencyNs > g_oppUsmCopyStatsMaxNs)
		g_oppUsmCopyStatsMaxNs = latencyNs;
	g_oppUsmCopyStatsSumNs += latencyNs;
	g_oppUsmCopyStatsCompletedCount++;
	if (g_oppUsmCopyLatencyDetailEnabled){
		std::cout << "OPP_USM_COPY_LATENCY"
			<< " tracking_id " << trackingId
			<< " node " << record.nodeId
			<< " in_dev " << record.inDev
			<< " candidate_count " << record.candidateCount
			<< " ingress_ns " << record.ingressTimeNs
			<< " finish_ns " << finishNs
			<< " latency_ns " << latencyNs
			<< "\n";
	}
	g_oppUsmCopyStatsRecords.erase(it);
}

uint32_t SwitchNode::GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId) const{
	uint32_t index = tokenId;
	if (m_routeProvider != NULL && m_routeProvider->GetOppCacheIndex(ch, tokenId, index))
		return index;
	return tokenId;
}

bool SwitchNode::ExtractOppProbe(Ptr<Packet> packet, OppProbeHeader &opph) const{
	uint32_t minSize = PppHeader::GetStaticSize() + 20 + 8 + SeqTsHeader::GetHeaderSize() + OppProbeHeader::GetStaticSize();
	if (packet->GetSize() < minSize)
		return false;

	Ptr<Packet> copy = packet->Copy();
	PppHeader ppp;
	Ipv4Header ip;
	UdpHeader udp;
	SeqTsHeader seqTs;
	copy->RemoveHeader(ppp);
	copy->RemoveHeader(ip);
	copy->RemoveHeader(udp);
	copy->RemoveHeader(seqTs);
	if (copy->GetSize() < OppProbeHeader::GetStaticSize())
		return false;
	copy->RemoveHeader(opph);
	return true;
}

bool SwitchNode::ExtractOppAck(Ptr<Packet> packet, OppAckHeader &ackh) const{
	uint32_t minSize = PppHeader::GetStaticSize() + 20 + qbbHeader::GetBaseSize() +
		IntHeader::GetStaticSize() + OppAckHeader::GetStaticSize();
	if (packet->GetSize() < minSize)
		return false;

	Ptr<Packet> copy = packet->Copy();
	PppHeader ppp;
	Ipv4Header ip;
	qbbHeader qbb;
	copy->RemoveHeader(ppp);
	copy->RemoveHeader(ip);
	copy->RemoveHeader(qbb);
	if (copy->GetSize() < OppAckHeader::GetStaticSize())
		return false;
	copy->RemoveHeader(ackh);
	return true;
}

bool SwitchNode::GetPeerNodeId(uint32_t devIdx, uint32_t &peerNodeId) const{
	if (devIdx >= m_devices.size())
		return false;
	Ptr<NetDevice> dev = m_devices[devIdx];
	Ptr<Channel> ch = dev->GetChannel();
	if (ch == NULL || ch->GetNDevices() != 2)
		return false;
	for (uint32_t i = 0; i < ch->GetNDevices(); i++){
		Ptr<NetDevice> peerDev = ch->GetDevice(i);
		if (peerDev != dev){
			peerNodeId = peerDev->GetNode()->GetId();
			return true;
		}
	}
	return false;
}

void SwitchNode::SendRpingmeshTracerouteAck(uint32_t inDev, CustomHeader &ch){
	if (inDev >= m_devices.size() || !m_devices[inDev]->IsLinkUp())
		return;

	qbbHeader seqh;
	seqh.SetSeq(GetId());
	seqh.SetPG(ch.udp.pg);
	seqh.SetSport(ch.udp.dport);
	seqh.SetDport(ch.udp.sport);
	seqh.SetIntHeader(ch.udp.ih);
	seqh.SetRpingmeshTraceroute();

	uint32_t headerSize = PppHeader::GetStaticSize() + 20 + seqh.GetSerializedSize();
	Ptr<Packet> ack = Create<Packet>(headerSize < RPINGMESH_PACKET_SIZE ? RPINGMESH_PACKET_SIZE - headerSize : 0);
	ack->AddHeader(seqh);

	Ipv4Header head;
	head.SetDestination(Ipv4Address(ch.sip));
	head.SetSource(Ipv4Address(ch.dip));
	head.SetProtocol(0xFC);
	head.SetTtl(64);
	head.SetPayloadSize(ack->GetSize());
	head.SetIdentification(0);
	ack->AddHeader(head);

	PppHeader ppp;
	ppp.SetProtocol(0x0021);
	ack->AddHeader(ppp);
	NS_ASSERT_MSG(ack->GetSize() == RPINGMESH_PACKET_SIZE,
		"RPINGMESH traceroute ACK packet size must stay 100 bytes");
	ack->AddPacketTag(FlowIdTag(inDev));

	CustomHeader ackCh(CustomHeader::L2_Header | CustomHeader::L3_Header | CustomHeader::L4_Header);
	ackCh.l3Prot = 0xFC;
	ackCh.sip = ch.dip;
	ackCh.dip = ch.sip;
	ackCh.ack.sport = ch.udp.dport;
	ackCh.ack.dport = ch.udp.sport;
	ackCh.ack.pg = ch.udp.pg;
	ackCh.ack.seq = GetId();
	ackCh.ack.flags = 1 << qbbHeader::FLAG_RPINGMESH_TRACEROUTE;
	ackCh.ack.ih = ch.udp.ih;
	NS_LOG_INFO("node " << GetId() << " sends RPINGMESH traceroute ACK inDev "
		<< inDev << " switch_id " << GetId());
	m_devices[inDev]->SwitchSend(0, ack, ackCh);
}

bool SwitchNode::DecrementRpingmeshProbeTtl(uint32_t inDev, Ptr<Packet> p, CustomHeader &ch){
	if (m_ccMode != RDMA_CC_MODE_RPINGMESH || ch.l3Prot != 0x11)
		return true;
	PppHeader ppp;
	Ipv4Header ip;
	p->RemoveHeader(ppp);
	p->RemoveHeader(ip);
	uint8_t ttl = ip.GetTtl();
	if (ttl <= 1){
		if (IsRpingmeshTracerouteProbe(ch))
			SendRpingmeshTracerouteAck(inDev, ch);
		p->AddHeader(ip);
		p->AddHeader(ppp);
		return false;
	}
	ip.SetTtl(ttl - 1);
	ch.m_ttl = ttl - 1;
	p->AddHeader(ip);
	p->AddHeader(ppp);
	return true;
}

bool SwitchNode::ShouldDropOppProbeForDebug(uint32_t outDev) const{
	const char *spec = std::getenv("OPP_DROP_LINK");
	if (spec == NULL || spec[0] == '\0')
		return false;

	char *end = NULL;
	uint32_t src = (uint32_t)std::strtoul(spec, &end, 10);
	if (end == spec || (*end != ':' && *end != ',' && *end != '-'))
		return false;
	uint32_t dst = (uint32_t)std::strtoul(end + 1, NULL, 10);
	if (GetId() != src)
		return false;

	uint32_t peerNodeId;
	return GetPeerNodeId(outDev, peerNodeId) && peerNodeId == dst;
}

bool SwitchNode::IsFatTreeFaultProbe(CustomHeader &ch) const{
	return ch.l3Prot == 0x11 && IsRpingmeshLikeCcMode(m_ccMode);
}

uint32_t SwitchNode::GetFatTreeFaultRandom(CustomHeader &ch, uint32_t salt){
	uint32_t key[8];
	key[0] = m_fatTreeFaultSeed;
	key[1] = GetId();
	key[2] = ch.sip;
	key[3] = ch.dip;
	key[4] = ((uint32_t)ch.udp.sport << 16) | ch.udp.dport;
	key[5] = ((uint32_t)ch.udp.pg << 16) | (ch.udp.seq & 0xffff);
	key[6] = (uint32_t)(m_fatTreeFaultRandomCounter++);
	key[7] = salt;
	return EcmpHash((const uint8_t*)key, sizeof(key), m_fatTreeFaultSeed);
}

bool SwitchNode::ApplyFatTreeFaultBeforeEnqueue(uint32_t inDev, uint32_t &outDev, CustomHeader &ch){
	if (m_fatTreeFaultMode == FAT_TREE_FAULT_NONE || m_fatTreeFaultMode == FAT_TREE_FAULT_CONGESTION)
		return false;
	if (m_fatTreeFaultTargetInDev != FAT_TREE_FAULT_TARGET_ANY &&
			inDev != m_fatTreeFaultTargetInDev)
		return false;
	if (m_fatTreeFaultTargetOutDev != FAT_TREE_FAULT_TARGET_ANY &&
			outDev != m_fatTreeFaultTargetOutDev)
		return false;
	if (!IsFatTreeFaultProbe(ch))
		return false;

	if (m_fatTreeFaultMode == FAT_TREE_FAULT_ACL_DROP){
		NS_LOG_INFO("node " << GetId() << " fat-tree ACL-drops probe inDev "
			<< inDev << " outDev " << outDev);
		return true;
	}
	if (m_fatTreeFaultMode == FAT_TREE_FAULT_LINK_FLAPPING){
		if (GetFatTreeFaultRandom(ch, FAT_TREE_FAULT_LINK_FLAPPING) % 100 < 20){
			NS_LOG_INFO("node " << GetId() << " fat-tree flapping-drops probe inDev "
				<< inDev << " outDev " << outDev);
			return true;
		}
		return false;
	}
	if (m_fatTreeFaultMode == FAT_TREE_FAULT_LOOP){
		if (m_ccMode == RDMA_CC_MODE_OPP)
			return false;
		if (GetFatTreeFaultRandom(ch, FAT_TREE_FAULT_LOOP) % 100 < 50){
			NS_ASSERT_MSG(inDev < m_devices.size() && m_devices[inDev]->IsLinkUp(), "fat-tree loop target ingress link must be up");
			NS_LOG_INFO("node " << GetId() << " fat-tree loops probe inDev "
				<< inDev << " originalOutDev " << outDev);
			outDev = inDev;
		}
		return false;
	}
	return false;
}

Time SwitchNode::GetFatTreeFaultEgressDelay(uint32_t ifIndex, Ptr<const Packet> p) const{
	if (m_fatTreeFaultMode != FAT_TREE_FAULT_CONGESTION || ifIndex != m_fatTreeFaultTargetOutDev)
		return TimeStep(0);

	CustomHeader ch(CustomHeader::L2_Header | CustomHeader::L3_Header | CustomHeader::L4_Header);
	ch.getInt = 1;
	p->PeekHeader(ch);
	if (!IsFatTreeFaultProbe(ch))
		return TimeStep(0);
	return NanoSeconds(m_fatTreeFaultCongestionDelayNs);
}

uint32_t SwitchNode::GetOppQDelayBiasForDebug(uint32_t outDev) const{
	const char *spec = std::getenv("OPP_QDELAY_BIAS_LINK");
	if (spec == NULL || spec[0] == '\0')
		return 0;

	char *end = NULL;
	uint32_t src = (uint32_t)std::strtoul(spec, &end, 10);
	if (end == spec || (*end != ':' && *end != ',' && *end != '-'))
		return 0;
	uint32_t dst = (uint32_t)std::strtoul(end + 1, &end, 10);
	if (*end != ':' && *end != ',' && *end != '-')
		return 0;
	uint32_t biasNs = (uint32_t)std::strtoul(end + 1, NULL, 10);
	if (GetId() != src || biasNs == 0)
		return 0;

	uint32_t peerNodeId;
	if (GetPeerNodeId(outDev, peerNodeId) && peerNodeId == dst)
		return biasNs;
	return 0;
}

uint32_t SwitchNode::GetInDevIndex(Ptr<NetDevice> device) const{
	uint32_t idx = device->GetIfIndex();
	if (idx < m_devices.size() && m_devices[idx] == device)
		return idx;
	for (uint32_t i = 0; i < m_devices.size(); i++){
		if (m_devices[i] == device)
			return i;
	}
	NS_ASSERT_MSG(false, "incoming switch device is not attached to this node");
	return 0;
}

bool SwitchNode::IsOppLoopbackPort(uint32_t port) const{
	return m_oppLoopbackPort != OPP_LOOPBACK_PORT_INVALID && port == m_oppLoopbackPort;
}

std::vector<int> SwitchNode::GetOppCandidatePorts(CustomHeader &ch, uint32_t inDev, bool useUsm){
	std::vector<int> ports;
	if (m_routeProvider != NULL){
		int providerPort = m_routeProvider->GetOutDev(ch);
		if (providerPort == -2){
			for (uint32_t i = 0; i < m_providerEcmpPorts.size(); i++){
				int port = m_providerEcmpPorts[i];
				if (port >= 0 && (uint32_t)port < m_devices.size() && (uint32_t)port != inDev && m_devices[port]->IsLinkUp())
					ports.push_back(port);
			}
		}else if (providerPort >= 0 && (uint32_t)providerPort < m_devices.size() && (uint32_t)providerPort != inDev && m_devices[providerPort]->IsLinkUp()){
			ports.push_back(providerPort);
		}
		if (m_ccMode == RDMA_CC_MODE_OPP && useUsm &&
				m_fatTreeFaultMode == FAT_TREE_FAULT_LOOP &&
				inDev < m_devices.size() && m_devices[inDev]->IsLinkUp()){
			bool hasFaultTarget = false;
			bool hasLoopPort = false;
			for (uint32_t i = 0; i < ports.size(); i++){
				if (m_fatTreeFaultTargetOutDev == FAT_TREE_FAULT_TARGET_ANY ||
						(uint32_t)ports[i] == m_fatTreeFaultTargetOutDev)
					hasFaultTarget = true;
				if ((uint32_t)ports[i] == inDev)
					hasLoopPort = true;
			}
			if (hasFaultTarget &&
					(m_fatTreeFaultTargetInDev == FAT_TREE_FAULT_TARGET_ANY ||
					 m_fatTreeFaultTargetInDev == inDev) &&
					!hasLoopPort){
				ports.push_back((int)inDev);
				NS_LOG_INFO("node " << GetId() << " adds OPP loop output port " << inDev
					<< " with fault target outDev " << m_fatTreeFaultTargetOutDev);
			}
		}
		return ports;
	}

	auto entry = m_rtTable.find(ch.dip);
	if (entry == m_rtTable.end())
		return ports;

	for (uint32_t i = 0; i < entry->second.size(); i++){
		int port = entry->second[i];
		if (port < 0 || (uint32_t)port >= m_devices.size() || (uint32_t)port == inDev || !m_devices[port]->IsLinkUp())
			continue;
		bool seen = false;
		for (uint32_t j = 0; j < ports.size(); j++){
			if (ports[j] == port){
				seen = true;
				break;
			}
		}
		if (!seen)
			ports.push_back(port);
	}
	if (m_ccMode == RDMA_CC_MODE_OPP && useUsm &&
			m_fatTreeFaultMode == FAT_TREE_FAULT_LOOP &&
			inDev < m_devices.size() && m_devices[inDev]->IsLinkUp()){
		bool hasFaultTarget = false;
		bool hasLoopPort = false;
		for (uint32_t i = 0; i < ports.size(); i++){
			if (m_fatTreeFaultTargetOutDev == FAT_TREE_FAULT_TARGET_ANY ||
					(uint32_t)ports[i] == m_fatTreeFaultTargetOutDev)
				hasFaultTarget = true;
			if ((uint32_t)ports[i] == inDev)
				hasLoopPort = true;
		}
		if (hasFaultTarget &&
				(m_fatTreeFaultTargetInDev == FAT_TREE_FAULT_TARGET_ANY ||
				 m_fatTreeFaultTargetInDev == inDev) &&
				!hasLoopPort){
			ports.push_back((int)inDev);
			NS_LOG_INFO("node " << GetId() << " adds OPP loop output port " << inDev
				<< " with fault target outDev " << m_fatTreeFaultTargetOutDev);
		}
	}
	return ports;
}

void SwitchNode::MulticastOppProbe(Ptr<Packet> packet, CustomHeader &ch, uint32_t inDev, const std::vector<int> &ports){
	if (ports.empty())
		return;

	for (uint32_t i = 0; i < ports.size(); i++){
		uint32_t outDev = (uint32_t)ports[i];
		NS_ASSERT_MSG(outDev < pCnt, "OPP multicast output port exceeds bitmap size");
		Ptr<Packet> copy = packet->Copy();
		if (EnqueueToDev(inDev, outDev, copy, ch, true)){
			NS_LOG_INFO("node " << GetId() << " multicasts OPP probe inDev " << inDev
				<< " outDev " << outDev << " sent " << (i + 1) << "/" << ports.size());
		}else{
			NS_LOG_INFO("node " << GetId() << " drops OPP probe copy by admission inDev "
				<< inDev << " outDev " << outDev);
		}
	}
}

void SwitchNode::UsmOppProbe(Ptr<Packet> packet, CustomHeader &ch, uint32_t inDev, OppUsmTag tag){
	if (tag.CountCandidates() == 0 || !tag.HasUnsent())
		return;
	NS_ASSERT_MSG(m_oppLoopbackPort != OPP_LOOPBACK_PORT_INVALID, "OPP USM requires an installed loopback port");
	NS_ASSERT_MSG(m_oppLoopbackPort < m_devices.size() && m_devices[m_oppLoopbackPort]->IsLinkUp(),
		"OPP USM loopback port must be installed and up");

	uint32_t selected = 0;
	if (!tag.PickCandidateByHash(GetPacketHash(ch), selected))
		return;

	if (tag.IsSent(selected)){
		Ptr<Packet> loopCopy = packet->Copy();
		OppUsmTag old;
		loopCopy->RemovePacketTag(old);
		loopCopy->AddPacketTag(tag);
		EnqueueToDev(inDev, m_oppLoopbackPort, loopCopy, ch, true);
		NS_LOG_INFO("node " << GetId() << " USM reschedules OPP probe through loopback"
			<< " inDev " << inDev << " selected_sent " << selected
			<< " remaining " << tag.CountUnsent());
		return;
	}

	Ptr<Packet> physicalCopy = packet->Copy();
	OppUsmTag old;
	physicalCopy->RemovePacketTag(old);
	OppUsmCopyStatsTag statsTag;
	bool hasStatsTag = physicalCopy->PeekPacketTag(statsTag);
	EnqueueToDev(inDev, selected, physicalCopy, ch, true,
		hasStatsTag ? statsTag.GetTrackingId() : 0);
	tag.SetSent(selected);
	NS_LOG_INFO("node " << GetId() << " USM sends OPP probe inDev " << inDev
		<< " outDev " << selected << " sent " << tag.CountSent()
		<< "/" << tag.CountCandidates());

	if (tag.HasUnsent()){
		Ptr<Packet> loopCopy = packet->Copy();
		OppUsmTag oldLoop;
		loopCopy->RemovePacketTag(oldLoop);
		loopCopy->AddPacketTag(tag);
		EnqueueToDev(inDev, m_oppLoopbackPort, loopCopy, ch, true);
		NS_LOG_INFO("node " << GetId() << " USM loops OPP probe inDev " << inDev
			<< " loopback " << m_oppLoopbackPort << " remaining " << tag.CountUnsent());
	}else if (hasStatsTag){
		MarkOppUsmCopyDispatchDone(statsTag.GetTrackingId());
	}
}

void SwitchNode::SendOppAck(uint32_t inDev, const OppCacheEntry &entry){
	if (IsOppLoopbackPort(inDev))
		return;
	if (inDev >= m_devices.size() || !m_devices[inDev]->IsLinkUp())
		return;

	OppAckHeader oppAck;
	PopulateOppAckFromCache(oppAck, entry);

	qbbHeader seqh;
	seqh.SetSeq(0);
	seqh.SetPG(entry.flowId.pg);
	seqh.SetSport(entry.flowId.dport);
	seqh.SetDport(entry.flowId.sport);
	IntHeader ih;
	seqh.SetIntHeader(ih);

	uint32_t headerSize = PppHeader::GetStaticSize() + 20 + seqh.GetSerializedSize() +
		OppAckHeader::GetStaticSize();
	Ptr<Packet> ack = Create<Packet>(headerSize < RPINGMESH_PACKET_SIZE ? RPINGMESH_PACKET_SIZE - headerSize : 0);
	ack->AddHeader(oppAck);
	ack->AddHeader(seqh);

	Ipv4Header head;
	head.SetDestination(Ipv4Address(entry.flowId.sip));
	head.SetSource(Ipv4Address(entry.flowId.dip));
	head.SetProtocol(0xFC);
	head.SetTtl(64);
	head.SetPayloadSize(ack->GetSize());
	head.SetIdentification(0);
	ack->AddHeader(head);

	PppHeader ppp;
	ppp.SetProtocol(0x0021);
	ack->AddHeader(ppp);
	NS_ASSERT_MSG(ack->GetSize() == RPINGMESH_PACKET_SIZE,
		"OPP ACK packet size must stay 100 bytes");
	ack->AddPacketTag(FlowIdTag(inDev));

	CustomHeader ackCh(CustomHeader::L2_Header | CustomHeader::L3_Header | CustomHeader::L4_Header);
	ackCh.l3Prot = 0xFC;
	ackCh.sip = entry.flowId.dip;
	ackCh.dip = entry.flowId.sip;
	ackCh.ack.sport = entry.flowId.dport;
	ackCh.ack.dport = entry.flowId.sport;
	ackCh.ack.pg = entry.flowId.pg;
	ackCh.ack.seq = 0;
	NS_LOG_INFO("node " << GetId() << " sends OPP ACK token " << entry.tokenId
		<< " seq " << entry.tokenSeqNo << " inDev " << inDev);
	m_devices[inDev]->SwitchSend(0, ack, ackCh);
}

void SwitchNode::SendOppAckToInputs(const OppCacheEntry &entry){
	uint32_t sentCnt = 0;
	for (uint32_t inDev = 0; inDev < pCnt; inDev++){
		if (!entry.inputPorts.test(inDev))
			continue;
		SendOppAck(inDev, entry);
		if (inDev < m_devices.size() && m_devices[inDev]->IsLinkUp())
			sentCnt++;
	}
	NS_LOG_INFO("node " << GetId() << " actively sends OPP ACK token "
		<< entry.tokenId << " seq " << entry.tokenSeqNo << " to "
		<< sentCnt << " input ports");
}

void SwitchNode::ScheduleOppCacheTimeout(uint32_t cacheIndex, uint32_t tokenId, uint16_t tokenSeqNo, uint8_t remainingHopCount){
	Time timeout = MilliSeconds((uint64_t)remainingHopCount);
	Simulator::Schedule(timeout, &SwitchNode::CheckOppCacheTimeout, this, cacheIndex, tokenId, tokenSeqNo);
	NS_LOG_INFO("node " << GetId() << " schedules OPP cache timeout token "
		<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo
		<< " delay " << timeout.GetTimeStep());
}

uint32_t SwitchNode::EncodeOppLink(uint32_t port) const{
	return ((GetId() & 0xffff) << 16) | (port & 0xffff);
}

void SwitchNode::CheckOppCacheTimeout(uint32_t cacheIndex, uint32_t tokenId, uint16_t tokenSeqNo){
	auto it = m_oppCache.find(cacheIndex);
	if (it == m_oppCache.end() || !it->second.valid)
		return;

	OppCacheEntry &entry = it->second;
	if (entry.tokenId != tokenId || entry.tokenSeqNo != tokenSeqNo || entry.outputPorts.none())
		return;

	uint32_t faultyPort = pCnt;
	for (uint32_t port = 0; port < pCnt; port++){
		if (entry.outputPorts.test(port)){
			faultyPort = port;
			break;
		}
	}
	if (faultyPort == pCnt)
		return;

	entry.faultyLink = EncodeOppLink(faultyPort);
	entry.outputPorts.reset();
	NS_LOG_INFO("node " << GetId() << " times out OPP cache token "
		<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo << " faulty_port "
		<< faultyPort << " faulty_link " << entry.faultyLink);
	SendOppAckToInputs(entry);
}

void SwitchNode::PopulateOppAckFromCache(OppAckHeader &ackh, const OppCacheEntry &entry) const{
	ackh.SetTokenIdA(entry.tokenId);
	ackh.SetTokenSeqNoA(entry.tokenSeqNo);
	ackh.SetEqualCostPathCountA(entry.equalCostPathCount);
	ackh.SetMaximumQDelayA(entry.maximumQDelay);
	ackh.SetCongestedLinkA(entry.congestedLink);
	ackh.SetFaultyLinkA(entry.faultyLink);
	ackh.SetIsLoopA(entry.isLoop ? 1 : 0);
}

bool SwitchNode::RewriteOppAckFromCache(Ptr<Packet> packet, const OppCacheEntry &entry) const{
	uint32_t minSize = PppHeader::GetStaticSize() + 20 + qbbHeader::GetBaseSize() +
		IntHeader::GetStaticSize() + OppAckHeader::GetStaticSize();
	if (packet->GetSize() < minSize)
		return false;

	uint32_t originalSize = packet->GetSize();
	PppHeader ppp;
	Ipv4Header ip;
	qbbHeader qbb;
	OppAckHeader ackh;
	packet->RemoveHeader(ppp);
	packet->RemoveHeader(ip);
	packet->RemoveHeader(qbb);
	if (packet->GetSize() < OppAckHeader::GetStaticSize()){
		packet->AddHeader(qbb);
		packet->AddHeader(ip);
		packet->AddHeader(ppp);
		return false;
	}
	packet->RemoveHeader(ackh);
	PopulateOppAckFromCache(ackh, entry);
	packet->AddHeader(ackh);
	packet->AddHeader(qbb);
	packet->AddHeader(ip);
	packet->AddHeader(ppp);
	NS_ASSERT_MSG(packet->GetSize() == originalSize, "OPP ACK rewrite must preserve packet size");
	return true;
}

void SwitchNode::ForwardOppAckToInputs(uint32_t inDev, Ptr<Packet> packet, CustomHeader &ch, const OppCacheEntry &entry){
	if (!RewriteOppAckFromCache(packet, entry)){
		NS_LOG_INFO("node " << GetId() << " drops OPP ACK token " << entry.tokenId
			<< " seq " << entry.tokenSeqNo << " because rewrite failed");
		return;
	}

	uint32_t sentCnt = 0;
	for (uint32_t outDev = 0; outDev < pCnt; outDev++){
		if (!entry.inputPorts.test(outDev))
			continue;
		if (outDev >= m_devices.size() || !m_devices[outDev]->IsLinkUp()){
			NS_LOG_INFO("node " << GetId() << " skips OPP ACK token " << entry.tokenId
				<< " seq " << entry.tokenSeqNo << " input port " << outDev
				<< " because link is down");
			continue;
		}
		Ptr<Packet> copy = packet->Copy();
		if (EnqueueToDev(inDev, outDev, copy, ch, false)){
			sentCnt++;
			NS_LOG_INFO("node " << GetId() << " forwards aggregated OPP ACK token "
				<< entry.tokenId << " seq " << entry.tokenSeqNo
				<< " inDev " << inDev << " outDev " << outDev);
		}else{
			NS_LOG_INFO("node " << GetId() << " drops aggregated OPP ACK token "
				<< entry.tokenId << " seq " << entry.tokenSeqNo
				<< " by admission inDev " << inDev << " outDev " << outDev);
		}
	}
	NS_LOG_INFO("node " << GetId() << " completed OPP ACK aggregation token "
		<< entry.tokenId << " seq " << entry.tokenSeqNo << " forwarded "
		<< sentCnt << " copies");
}

void SwitchNode::RewriteOppProbeOnEgress(uint32_t ifIndex, Ptr<Packet> p){
	if (m_ccMode != RDMA_CC_MODE_OPP)
		return;

	OppEgressTag tag;
	if (!p->RemovePacketTag(tag))
		return;

	uint32_t minSize = PppHeader::GetStaticSize() + 20 + 8 + SeqTsHeader::GetHeaderSize() + OppProbeHeader::GetStaticSize();
	if (p->GetSize() < minSize)
		return;

	uint32_t originalSize = p->GetSize();
	PppHeader ppp;
	Ipv4Header ip;
	UdpHeader udp;
	SeqTsHeader seqTs;
	OppProbeHeader opph;
	p->RemoveHeader(ppp);
	p->RemoveHeader(ip);
	p->RemoveHeader(udp);
	p->RemoveHeader(seqTs);
	if (p->GetSize() < OppProbeHeader::GetStaticSize()){
		p->AddHeader(seqTs);
		p->AddHeader(udp);
		p->AddHeader(ip);
		p->AddHeader(ppp);
		return;
	}
	p->RemoveHeader(opph);

	uint64_t now = Simulator::Now().GetTimeStep();
	uint64_t qdelay = now >= tag.GetEnqueueTimeNs() ? now - tag.GetEnqueueTimeNs() : 0;
	qdelay += GetOppQDelayBiasForDebug(ifIndex);
	uint32_t qdelay32 = qdelay > 0xffffffffull ? 0xffffffffu : (uint32_t)qdelay;
	uint32_t link = EncodeOppLink(ifIndex);
	uint8_t remaining = opph.GetRemainingHopCountP();
	if (remaining > 0)
		remaining--;
	if (qdelay32 > opph.GetPreviousHopQDelayP()){
		opph.SetPreviousHopQDelayP(qdelay32);
		opph.SetPreviousHopLinkP(link);
	}
	opph.SetRemainingHopCountP(remaining);

	p->AddHeader(opph);
	p->AddHeader(seqTs);
	p->AddHeader(udp);
	p->AddHeader(ip);
	p->AddHeader(ppp);
	NS_ASSERT_MSG(p->GetSize() == originalSize, "OPP egress rewrite must preserve packet size");
	NS_LOG_INFO("node " << GetId() << " rewrites OPP probe outDev " << ifIndex
		<< " tagOutDev " << tag.GetOutDev() << " qdelay " << qdelay32
		<< " link " << link << " remaining " << (uint32_t)remaining);
}

void SwitchNode::TraceRdProbeEgressOnDequeue(uint32_t ifIndex, Ptr<Packet> p){
	if (m_ccMode != RDMA_CC_MODE_RDPROBE || m_rdProbeTraceCallback.IsNull())
		return;

	CustomHeader ch(CustomHeader::L2_Header | CustomHeader::L3_Header | CustomHeader::L4_Header);
	ch.getInt = 1;
	p->PeekHeader(ch);
	if (ch.l3Prot == 0x11)
		m_rdProbeTraceCallback(ch, GetId(), ifIndex, Simulator::Now().GetTimeStep(), 1);
}

bool SwitchNode::ProcessOppProbe(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch){
	if (m_ccMode != RDMA_CC_MODE_OPP || ch.l3Prot != 0x11)
		return false;
	if (BypassOppCache())
		return false;
	return ProcessOppProbeFromPort(GetInDevIndex(device), packet, ch);
}

bool SwitchNode::ProcessOppProbeFromPort(uint32_t inDev, Ptr<Packet> packet, CustomHeader &ch){
	if (m_ccMode != RDMA_CC_MODE_OPP || ch.l3Prot != 0x11)
		return false;
	if (BypassOppCache())
		return false;

	OppProbeHeader opph;
	if (!ExtractOppProbe(packet, opph))
		return false;

	NS_ASSERT_MSG(inDev < pCnt, "OPP switch cache input port exceeds bitmap size");
	uint32_t tokenId = opph.GetTokenIdP();
	uint16_t tokenSeqNo = opph.GetTokenSeqNoP();
	uint32_t cacheIndex = GetOppCacheIndex(ch, tokenId);
	if (IsOppLoopbackPort(inDev)){
		m_oppLoopbackRxProbeCount++;
		OppUsmTag tag;
		if (!packet->RemovePacketTag(tag)){
			NS_LOG_INFO("node " << GetId() << " drops loopback OPP probe without USM tag token "
				<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo);
			return true;
		}
		UsmOppProbe(packet, ch, inDev, tag);
		return true;
	}

	OppCacheEntry &entry = m_oppCache[cacheIndex];

	if (entry.valid && tokenSeqNo < entry.tokenSeqNo){
		NS_LOG_INFO("node " << GetId() << " drops stale OPP probe token " << tokenId
			<< " cache_index " << cacheIndex << " seq " << tokenSeqNo
			<< " cached_seq " << entry.tokenSeqNo);
		return true;
	}
	if (entry.valid && tokenSeqNo == entry.tokenSeqNo){
		if (opph.GetRemainingHopCountP() < entry.remainingHopCount){
			entry.isLoop = true;
			NS_LOG_INFO("node " << GetId() << " marks OPP loop token "
				<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo
				<< " inDev " << inDev
				<< " remaining " << (uint32_t)opph.GetRemainingHopCountP()
				<< " cached_remaining " << (uint32_t)entry.remainingHopCount);
			return true;
		}
		entry.inputPorts.set(inDev);
		if (opph.GetPreviousHopQDelayP() > entry.maximumQDelay){
			entry.maximumQDelay = opph.GetPreviousHopQDelayP();
			entry.congestedLink = opph.GetPreviousHopLinkP();
		}
		if (ShouldUseOppUsm(packet)){
			std::vector<int> ports = GetOppCandidatePorts(ch, inDev, true);
			for (uint32_t i = 0; i < ports.size(); i++){
				uint32_t outDev = (uint32_t)ports[i];
				if (outDev != inDev || outDev >= pCnt || entry.outputPorts.test(outDev))
					continue;
				entry.outputPorts.set(outDev);
				Ptr<Packet> copy = packet->Copy();
				EnqueueToDev(inDev, outDev, copy, ch, true);
				NS_LOG_INFO("node " << GetId() << " adds duplicate OPP USM loop output token "
					<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo
					<< " inDev " << inDev);
			}
		}
		NS_LOG_INFO("node " << GetId() << " absorbs duplicate OPP probe token "
			<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo << " inDev " << inDev
			<< " remaining outputs " << entry.outputPorts.count()
			<< " max_qdelay " << entry.maximumQDelay
			<< " congested_link " << entry.congestedLink);
		if (entry.outputPorts.none() && !IsOppLoopbackPort(inDev))
			SendOppAck(inDev, entry);
		return true;
	}

	if (!entry.valid || tokenSeqNo > entry.tokenSeqNo){
		entry.valid = true;
		entry.tokenId = tokenId;
		entry.tokenSeqNo = tokenSeqNo;
		entry.flowId.sip = ch.sip;
		entry.flowId.dip = ch.dip;
		entry.flowId.sport = ch.udp.sport;
		entry.flowId.dport = ch.udp.dport;
		entry.flowId.pg = ch.udp.pg;
		entry.inputPorts.reset();
		entry.inputPorts.set(inDev);
		entry.outputPorts.reset();
		bool useUsm = ShouldUseOppUsm(packet);
		std::vector<int> ports = GetOppCandidatePorts(ch, inDev, useUsm);
		for (uint32_t i = 0; i < ports.size(); i++){
			NS_ASSERT_MSG((uint32_t)ports[i] < pCnt, "OPP switch cache output port exceeds bitmap size");
			entry.outputPorts.set((uint32_t)ports[i]);
		}
		entry.equalCostPathCount = 0;
		entry.maximumQDelay = opph.GetPreviousHopQDelayP();
		entry.congestedLink = opph.GetPreviousHopLinkP();
		entry.faultyLink = 0;
		entry.remainingHopCount = opph.GetRemainingHopCountP();
		entry.isLoop = false;
		NS_LOG_INFO("node " << GetId() << " initializes OPP cache token " << entry.tokenId
			<< " cache_index " << cacheIndex << " seq " << entry.tokenSeqNo << " inDev " << inDev
			<< " candidates " << ports.size());
		ScheduleOppCacheTimeout(cacheIndex, entry.tokenId, entry.tokenSeqNo, entry.remainingHopCount);
		if (useUsm){
			OppUsmTag tag;
			for (uint32_t i = 0; i < ports.size(); i++)
				tag.SetCandidate((uint32_t)ports[i]);
				if (ports.size() > 1){
					OppUsmCopyStatsTag oldStatsTag;
					packet->RemovePacketTag(oldStatsTag);
					uint64_t ingressStartNs = Simulator::Now().GetTimeStep();
					Ptr<QbbNetDevice> inQbb = DynamicCast<QbbNetDevice>(m_devices[inDev]);
					if (inQbb != NULL){
						uint64_t ingressDelayNs = inQbb->GetSwitchIngressPipelineDelay().GetTimeStep();
						if (ingressStartNs >= ingressDelayNs)
							ingressStartNs -= ingressDelayNs;
					}
					uint64_t trackingId = StartOppUsmCopyStats(ingressStartNs, GetId(), inDev, ports.size());
					packet->AddPacketTag(OppUsmCopyStatsTag(trackingId));
				}
			UsmOppProbe(packet, ch, inDev, tag);
		}else{
			MulticastOppProbe(packet, ch, inDev, ports);
		}
		return true;
	}
	return true;
}

bool SwitchNode::ProcessOppAck(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch){
	if (m_ccMode != RDMA_CC_MODE_OPP || ch.l3Prot != 0xFC)
		return false;
	if (BypassOppCache())
		return false;

	OppAckHeader ackh;
	if (!ExtractOppAck(packet, ackh)){
		NS_LOG_INFO("node " << GetId() << " drops malformed OPP ACK");
		return true;
	}

	uint32_t inDev = GetInDevIndex(device);
	NS_ASSERT_MSG(inDev < pCnt, "OPP switch ACK input port exceeds bitmap size");
	uint32_t tokenId = ackh.GetTokenIdA();
	uint16_t tokenSeqNo = ackh.GetTokenSeqNoA();
	uint32_t cacheIndex = GetOppCacheIndex(ch, tokenId);
	auto it = m_oppCache.find(cacheIndex);
	if (it == m_oppCache.end() || !it->second.valid){
		NS_LOG_INFO("node " << GetId() << " drops OPP ACK token " << tokenId
			<< " cache_index " << cacheIndex << " seq " << tokenSeqNo
			<< " because cache is missing");
		return true;
	}

	OppCacheEntry &entry = it->second;
	if (entry.tokenId != tokenId || tokenSeqNo != entry.tokenSeqNo){
		NS_LOG_INFO("node " << GetId() << " drops OPP ACK token " << tokenId
			<< " cache_index " << cacheIndex << " seq " << tokenSeqNo
			<< " cached_token " << entry.tokenId << " cached_seq " << entry.tokenSeqNo);
		return true;
	}
	if (!entry.outputPorts.test(inDev)){
		NS_LOG_INFO("node " << GetId() << " drops duplicate or unexpected OPP ACK token "
			<< tokenId << " cache_index " << cacheIndex << " seq " << tokenSeqNo << " inDev " << inDev);
		return true;
	}

	entry.outputPorts.reset(inDev);
	uint32_t pathCount = (uint32_t)entry.equalCostPathCount + ackh.GetEqualCostPathCountA();
	entry.equalCostPathCount = pathCount > 0xffffu ? 0xffffu : (uint16_t)pathCount;
	if (ackh.GetMaximumQDelayA() > entry.maximumQDelay){
		entry.maximumQDelay = ackh.GetMaximumQDelayA();
		entry.congestedLink = ackh.GetCongestedLinkA();
	}
	if (entry.faultyLink == 0 && ackh.GetFaultyLinkA() != 0)
		entry.faultyLink = ackh.GetFaultyLinkA();
	if (ackh.GetIsLoopA() != 0)
		entry.isLoop = true;

	NS_LOG_INFO("node " << GetId() << " aggregates OPP ACK token " << tokenId
		<< " cache_index " << cacheIndex << " seq " << tokenSeqNo << " inDev " << inDev
		<< " remaining outputs " << entry.outputPorts.count()
		<< " path_count " << entry.equalCostPathCount
		<< " max_qdelay " << entry.maximumQDelay
		<< " congested_link " << entry.congestedLink
		<< " faulty_link " << entry.faultyLink
		<< " is_loop " << entry.isLoop);

	if (entry.outputPorts.none())
		ForwardOppAckToInputs(inDev, packet, ch, entry);
	return true;
}

// This function can only be called in switch mode
bool SwitchNode::SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch){
	uint32_t inDev = GetInDevIndex(device);
	if (!m_packetReceiveCallback.IsNull())
		m_packetReceiveCallback(ch, GetId(), inDev, Simulator::Now().GetTimeStep());
	if (IsRpingmeshLikeCcMode(m_ccMode) && !IsOppLoopbackPort(inDev) && (ch.l3Prot == 0x11 || ch.l3Prot == 0xFC))
		m_probeAckRxCount++;
	if (m_ccMode == RDMA_CC_MODE_RDPROBE && ch.l3Prot == 0x11 && !m_rdProbeTraceCallback.IsNull()){
		m_rdProbeTraceCallback(ch, GetId(), inDev, Simulator::Now().GetTimeStep(), 0);
	}
	if (ProcessOppProbe(device, packet, ch))
		return true;
	if (ProcessOppAck(device, packet, ch))
		return true;
	SendToDev(packet, ch);
	return true;
}

void SwitchNode::SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p){
	FlowIdTag t;
	p->PeekPacketTag(t);
	RewriteOppProbeOnEgress(ifIndex, p);
	TraceRdProbeEgressOnDequeue(ifIndex, p);
	if (qIndex != 0){
		uint32_t inDev = t.GetFlowId();
		m_mmu->RemoveFromIngressAdmission(inDev, qIndex, p->GetSize());
		m_mmu->RemoveFromEgressAdmission(ifIndex, qIndex, p->GetSize());
		m_bytes[inDev][ifIndex][qIndex] -= p->GetSize();
		if (m_ecnEnabled){
			bool egressCongested = m_mmu->ShouldSendCN(ifIndex, qIndex);
			if (egressCongested){
				PppHeader ppp;
				Ipv4Header h;
				p->RemoveHeader(ppp);
				p->RemoveHeader(h);
				h.SetEcn((Ipv4Header::EcnType)0x03);
				p->AddHeader(h);
				p->AddHeader(ppp);
			}
		}
		//CheckAndSendPfc(inDev, qIndex);
		if (!IsOppLoopbackPort(inDev))
			CheckAndSendResume(inDev, qIndex);
	}
	if (1){
		uint8_t* buf = p->GetBuffer();
		if (buf[PppHeader::GetStaticSize() + 9] == 0x11){ // udp packet
			IntHeader *ih = (IntHeader*)&buf[PppHeader::GetStaticSize() + 20 + 8 + 6]; // ppp, ip, udp, SeqTs, INT
			Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(m_devices[ifIndex]);
			if (m_ccMode == 3){ // HPCC
				ih->PushHop(Simulator::Now().GetTimeStep(), m_txBytes[ifIndex], dev->GetQueue()->GetNBytesTotal(), dev->GetDataRate().GetBitRate());
			}else if (m_ccMode == 10){ // HPCC-PINT
				uint64_t t = Simulator::Now().GetTimeStep();
				uint64_t dt = t - m_lastPktTs[ifIndex];
				if (dt > m_maxRtt)
					dt = m_maxRtt;
				uint64_t B = dev->GetDataRate().GetBitRate() / 8; //Bps
				uint64_t qlen = dev->GetQueue()->GetNBytesTotal();
				double newU;

				/**************************
				 * approximate calc
				 *************************/
				int b = 20, m = 16, l = 20; // see log2apprx's paremeters
				int sft = logres_shift(b,l);
				double fct = 1<<sft; // (multiplication factor corresponding to sft)
				double log_T = log2(m_maxRtt)*fct; // log2(T)*fct
				double log_B = log2(B)*fct; // log2(B)*fct
				double log_1e9 = log2(1e9)*fct; // log2(1e9)*fct
				double qterm = 0;
				double byteTerm = 0;
				double uTerm = 0;
				if ((qlen >> 8) > 0){
					int log_dt = log2apprx(dt, b, m, l); // ~log2(dt)*fct
					int log_qlen = log2apprx(qlen >> 8, b, m, l); // ~log2(qlen / 256)*fct
					qterm = pow(2, (
								log_dt + log_qlen + log_1e9 - log_B - 2*log_T
								)/fct
							) * 256;
					// 2^((log2(dt)*fct+log2(qlen/256)*fct+log2(1e9)*fct-log2(B)*fct-2*log2(T)*fct)/fct)*256 ~= dt*qlen*1e9/(B*T^2)
				}
				if (m_lastPktSize[ifIndex] > 0){
					int byte = m_lastPktSize[ifIndex];
					int log_byte = log2apprx(byte, b, m, l);
					byteTerm = pow(2, (
								log_byte + log_1e9 - log_B - log_T
								)/fct
							);
					// 2^((log2(byte)*fct+log2(1e9)*fct-log2(B)*fct-log2(T)*fct)/fct) ~= byte*1e9 / (B*T)
				}
				if (m_maxRtt > dt && m_u[ifIndex] > 0){
					int log_T_dt = log2apprx(m_maxRtt - dt, b, m, l); // ~log2(T-dt)*fct
					int log_u = log2apprx(int(round(m_u[ifIndex] * 8192)), b, m, l); // ~log2(u*512)*fct
					uTerm = pow(2, (
								log_T_dt + log_u - log_T
								)/fct
							) / 8192;
					// 2^((log2(T-dt)*fct+log2(u*512)*fct-log2(T)*fct)/fct)/512 = (T-dt)*u/T
				}
				newU = qterm+byteTerm+uTerm;

				#if 0
				/**************************
				 * accurate calc
				 *************************/
				double weight_ewma = double(dt) / m_maxRtt;
				double u;
				if (m_lastPktSize[ifIndex] == 0)
					u = 0;
				else{
					double txRate = m_lastPktSize[ifIndex] / double(dt); // B/ns
					u = (qlen / m_maxRtt + txRate) * 1e9 / B;
				}
				newU = m_u[ifIndex] * (1 - weight_ewma) + u * weight_ewma;
				printf(" %lf\n", newU);
				#endif

				/************************
				 * update PINT header
				 ***********************/
				uint16_t power = Pint::encode_u(newU);
				if (power > ih->GetPower())
					ih->SetPower(power);

				m_u[ifIndex] = newU;
			}
		}
	}
	if (IsOppLoopbackPort(ifIndex)){
		AccountOppLoopbackTx(p->GetSize());
		if (!m_oppLoopbackTxCallback.IsNull())
			m_oppLoopbackTxCallback(GetId(), ifIndex, Simulator::Now().GetTimeStep(), p->GetSize());
	}else{
			OppUsmCopyStatsTag statsTag;
			if (p->RemovePacketTag(statsTag)){
				uint64_t egressDoneTimeNs = Simulator::Now().GetTimeStep();
				Ptr<QbbNetDevice> outDev = DynamicCast<QbbNetDevice>(m_devices[ifIndex]);
				if (outDev != NULL)
					egressDoneTimeNs += outDev->GetSwitchEgressPipelineDelay().GetTimeStep();
				AccountOppUsmCopyEgress(statsTag.GetTrackingId(), egressDoneTimeNs);
			}
		}
	m_txBytes[ifIndex] += p->GetSize();
	m_lastPktSize[ifIndex] = p->GetSize();
	m_lastPktTs[ifIndex] = Simulator::Now().GetTimeStep();
}

int SwitchNode::logres_shift(int b, int l){
	static int data[] = {0,0,1,2,2,3,3,3,3,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5};
	return l - data[b];
}

int SwitchNode::log2apprx(int x, int b, int m, int l){
	int x0 = x;
	int msb = int(log2(x)) + 1;
	if (msb > m){
		x = (x >> (msb - m) << (msb - m));
		#if 0
		x += + (1 << (msb - m - 1));
		#else
		int mask = (1 << (msb-m)) - 1;
		if ((x0 & mask) > (rand() & mask))
			x += 1<<(msb-m);
		#endif
	}
	return int(log2(x) * (1<<logres_shift(b, l)));
}

} /* namespace ns3 */
