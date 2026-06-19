#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include <bitset>
#include <unordered_map>
#include <vector>
#include <ns3/node.h>
#include <ns3/callback.h>
#include "qbb-net-device.h"
#include "switch-mmu.h"
#include "switch-route-provider.h"
#include "pint.h"

namespace ns3 {

class Packet;
class OppUsmTag;

class SwitchNode : public Node{
public:
	struct OppLoopbackTxWindowRecord {
		uint64_t startNs;
		uint64_t bytes;

		OppLoopbackTxWindowRecord() : startNs(0), bytes(0) {}
		OppLoopbackTxWindowRecord(uint64_t startNs, uint64_t bytes)
			: startNs(startNs), bytes(bytes) {}
	};

private:
	static const uint32_t pCnt = 257;	// Number of ports used
	static const uint32_t qCnt = 8;	// Number of queues/priorities used
	static const uint32_t ECMP_MODE_FLOW_HASH = 0;
	static const uint32_t ECMP_MODE_PACKET_SPRAY = 1;
	static const uint32_t OPP_LOOPBACK_PORT_INVALID = pCnt;
	static const uint32_t OPP_MULTICAST_STANDARD = 0;
	static const uint32_t OPP_MULTICAST_USM = 1;
	static const uint32_t OPP_MULTICAST_SAMPLED = 2;
	uint32_t m_ecmpSeed;
	uint32_t m_ecmpMode;
	uint32_t m_oppMulticastMode;
	uint32_t m_oppLoopbackPort;
	uint64_t m_oppLoopbackRxProbeCount;
	uint64_t m_oppLoopbackPeakTxRate10usBps;
	uint64_t m_oppLoopbackTxWindow10usStartNs;
	uint64_t m_oppLoopbackTxWindow10usBytes;
	bool m_oppLoopbackTxWindow10usValid;
	uint64_t m_oppLoopbackPeakTxRate1usBps;
	uint64_t m_oppLoopbackTxWindow1usStartNs;
	uint64_t m_oppLoopbackTxWindow1usBytes;
	bool m_oppLoopbackTxWindow1usValid;
	uint64_t m_oppLoopbackPeakTxRate700nsBps;
	uint64_t m_oppLoopbackTxWindow700nsStartNs;
	uint64_t m_oppLoopbackTxWindow700nsBytes;
	bool m_oppLoopbackTxWindow700nsValid;
	uint64_t m_oppLoopbackPeakTxRate100nsBps;
	uint64_t m_oppLoopbackTxWindow100nsStartNs;
	uint64_t m_oppLoopbackTxWindow100nsBytes;
	bool m_oppLoopbackTxWindow100nsValid;
	bool m_oppLoopbackTxStatsFinalized;
	std::vector<OppLoopbackTxWindowRecord> m_oppLoopbackTxWindows10us;
	std::vector<OppLoopbackTxWindowRecord> m_oppLoopbackTxWindows1us;
	std::vector<OppLoopbackTxWindowRecord> m_oppLoopbackTxWindows700ns;
	uint64_t m_oppLoopbackPeakQueueBytes;
	uint64_t m_probeAckRxCount;
	uint32_t m_fatTreeFaultMode;
	uint32_t m_fatTreeFaultTargetOutDev;
	uint32_t m_fatTreeFaultTargetInDev;
	uint32_t m_fatTreeFaultSeed;
	uint64_t m_fatTreeFaultRandomCounter;
	uint32_t m_fatTreeFaultCongestionDelayNs;
	std::unordered_map<uint32_t, std::vector<int> > m_rtTable; // map from ip address (u32) to possible ECMP port (index of dev)
	Ptr<SwitchRouteProvider> m_routeProvider;
	std::vector<int> m_providerEcmpPorts;

	struct OppFlowId {
		uint32_t sip;
		uint32_t dip;
		uint16_t sport;
		uint16_t dport;
		uint16_t pg;

		OppFlowId() : sip(0), dip(0), sport(0), dport(0), pg(0) {}
	};

	struct OppCacheEntry {
		bool valid;
		uint32_t tokenId;
		uint16_t tokenSeqNo;
		OppFlowId flowId;
		std::bitset<pCnt> inputPorts;
		std::bitset<pCnt> outputPorts;
		uint16_t equalCostPathCount;
		uint32_t maximumQDelay;
		uint32_t congestedLink;
		uint32_t faultyLink;
		uint8_t remainingHopCount;
		bool isLoop;

		OppCacheEntry()
			: valid(false),
			  tokenId(0),
			  tokenSeqNo(0),
			  equalCostPathCount(0),
			  maximumQDelay(0),
			  congestedLink(0),
			  faultyLink(0),
			  remainingHopCount(0),
			  isLoop(false) {}
	};
	std::unordered_map<uint32_t, OppCacheEntry> m_oppCache;

	// monitor of PFC
	uint32_t m_bytes[pCnt][pCnt][qCnt]; // m_bytes[inDev][outDev][qidx] is the bytes from inDev enqueued for outDev at qidx
	
	uint64_t m_txBytes[pCnt]; // counter of tx bytes

	uint32_t m_lastPktSize[pCnt];
	uint64_t m_lastPktTs[pCnt]; // ns
	double m_u[pCnt];

protected:
	bool m_ecnEnabled;
	uint32_t m_ccMode;
	uint64_t m_maxRtt;

	uint32_t m_ackHighPrio; // set high priority for ACK/NACK

public:
	typedef Callback<void, CustomHeader&, uint32_t, uint32_t, uint32_t> PacketForwardCallback;
	typedef Callback<void, CustomHeader&, uint32_t, uint32_t, uint64_t> PacketReceiveCallback;
	typedef Callback<void, uint32_t, uint32_t, uint64_t, uint32_t> OppLoopbackTxCallback;
	typedef Callback<void, CustomHeader&, uint32_t, uint32_t, uint64_t, uint32_t> RdProbeTraceCallback;

private:
	int GetOutDev(Ptr<const Packet>, CustomHeader &ch);
	int GetLiveEcmpPort(const std::vector<int> &ports, CustomHeader &ch);
	uint32_t GetPacketHash(CustomHeader &ch);
	void SendToDev(Ptr<Packet>p, CustomHeader &ch);
	bool EnqueueToDev(uint32_t inDev, uint32_t outDev, Ptr<Packet> p, CustomHeader &ch, bool oppProbe, uint64_t oppUsmStatsTrackingId = 0);
	bool ProcessOppProbe(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch);
	bool ProcessOppProbeFromPort(uint32_t inDev, Ptr<Packet> packet, CustomHeader &ch);
	bool ProcessOppAck(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch);
	bool IsOppLoopbackPort(uint32_t port) const;
	bool ExtractOppProbe(Ptr<Packet> packet, class OppProbeHeader &opph) const;
	bool ExtractOppAck(Ptr<Packet> packet, class OppAckHeader &ackh) const;
	bool BypassOppCache() const;
	bool ShouldUseOppUsm(Ptr<Packet> packet) const;
	uint32_t GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId) const;
	bool ShouldDropOppProbeForDebug(uint32_t outDev) const;
	bool IsFatTreeFaultProbe(CustomHeader &ch) const;
	uint32_t GetFatTreeFaultRandom(CustomHeader &ch, uint32_t salt);
	bool ApplyFatTreeFaultBeforeEnqueue(uint32_t inDev, uint32_t &outDev, CustomHeader &ch);
	uint32_t GetOppQDelayBiasForDebug(uint32_t outDev) const;
	bool GetPeerNodeId(uint32_t devIdx, uint32_t &peerNodeId) const;
	bool DecrementRpingmeshProbeTtl(uint32_t inDev, Ptr<Packet> p, CustomHeader &ch);
	void SendRpingmeshTracerouteAck(uint32_t inDev, CustomHeader &ch);
	std::vector<int> GetOppCandidatePorts(CustomHeader &ch, uint32_t inDev, bool useUsm);
	void MulticastOppProbe(Ptr<Packet> packet, CustomHeader &ch, uint32_t inDev, const std::vector<int> &ports);
	void UsmOppProbe(Ptr<Packet> packet, CustomHeader &ch, uint32_t inDev, class OppUsmTag tag);
	static uint64_t StartOppUsmCopyStats(uint64_t ingressTimeNs, uint32_t nodeId, uint32_t inDev, uint32_t candidateCount);
	static void AccountOppUsmCopyEnqueue(uint64_t trackingId);
	static void MarkOppUsmCopyDispatchDone(uint64_t trackingId);
	static void AccountOppUsmCopyEgress(uint64_t trackingId, uint64_t egressTimeNs);
	static void FinishOppUsmCopyStatsIfComplete(uint64_t trackingId, uint64_t nowNs);
	void AccountOppLoopbackTx(uint32_t bytes);
	static void UpdateOppLoopbackPeakTxRateForWindow(
		uint64_t intervalNs, bool windowValid, uint64_t windowBytes, uint64_t &peakRateBps);
	static void AccountOppLoopbackTxForWindow(
		uint64_t now, uint32_t bytes, uint64_t intervalNs,
		bool &windowValid, uint64_t &windowStartNs, uint64_t &windowBytes,
		uint64_t &peakRateBps, std::vector<OppLoopbackTxWindowRecord> &records);
	static void FinalizeOppLoopbackTxForWindow(
		uint64_t intervalNs, bool windowValid, uint64_t windowStartNs,
		uint64_t windowBytes, uint64_t &peakRateBps,
		std::vector<OppLoopbackTxWindowRecord> &records);
	static void AccountOppLoopbackTxForPeakOnlyWindow(
		uint64_t now, uint32_t bytes, uint64_t intervalNs,
		bool &windowValid, uint64_t &windowStartNs, uint64_t &windowBytes,
		uint64_t &peakRateBps);
	static void FinalizeOppLoopbackTxForPeakOnlyWindow(
		uint64_t intervalNs, bool windowValid, uint64_t windowBytes,
		uint64_t &peakRateBps);
	void UpdateOppLoopbackPeakTxRate(void);
	uint32_t GetInDevIndex(Ptr<NetDevice> device) const;
	void SendOppAck(uint32_t inDev, const OppCacheEntry &entry);
	void SendOppAckToInputs(const OppCacheEntry &entry);
	void ScheduleOppCacheTimeout(uint32_t cacheIndex, uint32_t tokenId, uint16_t tokenSeqNo, uint8_t remainingHopCount);
	void CheckOppCacheTimeout(uint32_t cacheIndex, uint32_t tokenId, uint16_t tokenSeqNo);
	uint32_t EncodeOppLink(uint32_t port) const;
	void PopulateOppAckFromCache(class OppAckHeader &ackh, const OppCacheEntry &entry) const;
	bool RewriteOppAckFromCache(Ptr<Packet> packet, const OppCacheEntry &entry) const;
	void ForwardOppAckToInputs(uint32_t inDev, Ptr<Packet> packet, CustomHeader &ch, const OppCacheEntry &entry);
	void RewriteOppProbeOnEgress(uint32_t ifIndex, Ptr<Packet> p);
	void TraceRdProbeEgressOnDequeue(uint32_t ifIndex, Ptr<Packet> p);
	static uint32_t EcmpHash(const uint8_t* key, size_t len, uint32_t seed);
	void CheckAndSendPfc(uint32_t inDev, uint32_t qIndex);
	void CheckAndSendResume(uint32_t inDev, uint32_t qIndex);
public:
	Ptr<SwitchMmu> m_mmu;
	PacketForwardCallback m_packetForwardCallback;
	PacketReceiveCallback m_packetReceiveCallback;
	OppLoopbackTxCallback m_oppLoopbackTxCallback;
	RdProbeTraceCallback m_rdProbeTraceCallback;

	static TypeId GetTypeId (void);
	SwitchNode();
	void SetPacketForwardCallback(PacketForwardCallback cb);
	void SetPacketReceiveCallback(PacketReceiveCallback cb);
	void SetOppLoopbackTxCallback(OppLoopbackTxCallback cb);
	void SetRdProbeTraceCallback(RdProbeTraceCallback cb);
	void ConfigureFatTreeFault(uint32_t mode, uint32_t targetOutDev, uint32_t seed, uint32_t congestionDelayNs, uint32_t targetInDev = 0xffffffffu);
	Time GetFatTreeFaultEgressDelay(uint32_t ifIndex, Ptr<const Packet> p) const;
	void SetEcmpSeed(uint32_t seed);
	void SetEcmpMode(uint32_t mode);
	void SetOppMulticastMode(uint32_t mode);
	uint32_t InstallOppLoopbackDevice(DataRate rate);
	uint32_t GetOppLoopbackPort(void) const;
	void FinalizeOppLoopbackStats(void);
	uint64_t GetOppLoopbackRxProbeCount(void) const;
	uint64_t GetOppLoopbackPeakTxRateBps(void) const;
	uint64_t GetOppLoopbackPeakTxRate10usBps(void) const;
	uint64_t GetOppLoopbackPeakTxRate1usBps(void) const;
	uint64_t GetOppLoopbackPeakTxRate700nsBps(void) const;
	uint64_t GetOppLoopbackPeakTxRate100nsBps(void) const;
	const std::vector<OppLoopbackTxWindowRecord>& GetOppLoopbackTxWindows10us(void) const;
	const std::vector<OppLoopbackTxWindowRecord>& GetOppLoopbackTxWindows1us(void) const;
	const std::vector<OppLoopbackTxWindowRecord>& GetOppLoopbackTxWindows700ns(void) const;
	uint64_t GetOppLoopbackPeakQueueBytes(void) const;
	static uint64_t GetOppLoopbackTxSampleIntervalNs(void);
	static uint64_t GetOppLoopbackTxSampleInterval10usNs(void);
	static uint64_t GetOppLoopbackTxSampleInterval1usNs(void);
	static uint64_t GetOppLoopbackTxSampleInterval700nsNs(void);
	static uint64_t GetOppLoopbackTxSampleInterval100nsNs(void);
	static uint64_t GetOppUsmCopyStatsCount(void);
	static uint64_t GetOppUsmCopyStatsCompletedCount(void);
	static uint64_t GetOppUsmCopyStatsMinNs(void);
	static uint64_t GetOppUsmCopyStatsMaxNs(void);
	static uint64_t GetOppUsmCopyStatsSumNs(void);
	static void SetOppUsmCopyLatencyDetailEnabled(bool enabled);
	static bool GetOppUsmCopyLatencyDetailEnabled(void);
	void SetRouteProvider(Ptr<SwitchRouteProvider> routeProvider, const std::vector<int> &ecmpPorts);
	void AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx);
	void ClearTable();
	uint64_t GetProbeAckRxCount() const;
	bool SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch);
	void SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p);

	// for approximate calc in PINT
	int logres_shift(int b, int l);
	int log2apprx(int x, int b, int m, int l); // given x of at most b bits, use most significant m bits of x, calc the result in l bits
};

} /* namespace ns3 */

#endif /* SWITCH_NODE_H */
