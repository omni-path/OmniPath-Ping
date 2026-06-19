#ifndef RDMA_HW_H
#define RDMA_HW_H

#include <ns3/rdma.h>
#include <ns3/rdma-queue-pair.h>
#include <ns3/opp-token-manager.h>
#include <ns3/node.h>
#include <ns3/custom-header.h>
#include "qbb-net-device.h"
#include <unordered_map>
#include <map>
#include "pint.h"

namespace ns3 {

enum {
	RDMA_CC_MODE_RPINGMESH = 11,
	RDMA_CC_MODE_OPP = 12,
	RDMA_CC_MODE_RDPROBE = 13
};

static inline bool IsRpingmeshLikeCcMode(uint32_t ccMode){
	return ccMode == RDMA_CC_MODE_RPINGMESH ||
		ccMode == RDMA_CC_MODE_OPP ||
		ccMode == RDMA_CC_MODE_RDPROBE;
}

static const uint8_t RPINGMESH_TRACEROUTE_TOS = 0x40;
static const uint8_t RPINGMESH_TRACEROUTE_TTL = 3;
static const uint32_t RPINGMESH_PACKET_SIZE = 100;

static inline bool IsRpingmeshTracerouteProbe(const CustomHeader &ch){
	return ch.l3Prot == 0x11 && ((ch.m_tos & RPINGMESH_TRACEROUTE_TOS) != 0);
}

struct RdmaInterfaceMgr{
	Ptr<QbbNetDevice> dev;
	Ptr<RdmaQueuePairGroup> qpGrp;

	RdmaInterfaceMgr() : dev(NULL), qpGrp(NULL) {}
	RdmaInterfaceMgr(Ptr<QbbNetDevice> _dev){
		dev = _dev;
	}
};

class RdmaHw : public Object {
public:

	static TypeId GetTypeId (void);
	RdmaHw();

	Ptr<Node> m_node;
	DataRate m_minRate;		//< Min sending rate
	uint32_t m_mtu;
	uint32_t m_cc_mode;
	double m_nack_interval;
	uint32_t m_chunk;
	uint32_t m_ack_interval;
	bool m_backto0;
	bool m_var_win, m_fast_react;
	bool m_rateBound;
	std::vector<RdmaInterfaceMgr> m_nic; // list of running nic controlled by this RdmaHw
	std::unordered_map<uint64_t, Ptr<RdmaQueuePair> > m_qpMap; // mapping from uint64_t to qp
	std::unordered_map<uint64_t, Ptr<RdmaRxQueuePair> > m_rxQpMap; // mapping from uint64_t to rx qp
	std::unordered_map<uint32_t, std::vector<int> > m_rtTable; // map from ip address (u32) to possible ECMP port (index of dev)
	std::vector<int> m_defaultRt; // default ECMP ports, used by compressed topologies
	std::map<uint32_t, Ptr<OppTokenManager> > m_oppTokenManagers;
	uint32_t m_oppServerId;
	bool m_rpingmeshTracerouteOnMissingAck;
	bool m_rpingmeshTracerouteOnLoopFault;
	bool m_rpingmeshTracerouteOnCongestionFault;
	uint8_t m_rpingmeshTracerouteTtl;

	// qp complete callback
	typedef Callback<void, Ptr<RdmaQueuePair> > QpCompleteCallback;
	QpCompleteCallback m_qpCompleteCallback;
	typedef Callback<void, uint32_t, uint32_t, uint16_t, uint16_t, uint16_t, uint32_t, uint64_t> RpingmeshProbeSentCallback;
	typedef Callback<bool, CustomHeader&, uint8_t> RpingmeshProbeTtlCallback;
	typedef Callback<void, uint32_t, uint32_t, uint16_t, uint16_t, uint16_t, uint32_t, uint64_t, uint32_t, uint32_t> RpingmeshAckReceivedCallback;
	typedef Callback<void, uint32_t, uint32_t, uint16_t, uint16_t, uint16_t, uint32_t, uint32_t, uint8_t> OppAckReceivedCallback;
	RpingmeshProbeSentCallback m_rpingmeshProbeSentCallback;
	RpingmeshProbeTtlCallback m_rpingmeshProbeTtlCallback;
	RpingmeshAckReceivedCallback m_rpingmeshAckReceivedCallback;
	OppAckReceivedCallback m_oppAckReceivedCallback;

	void SetNode(Ptr<Node> node);
	void SetOppTokenManager(uint32_t probeInstanceId, Ptr<OppTokenManager> manager, uint32_t serverId);
	void SetRpingmeshProbeSentCallback(RpingmeshProbeSentCallback cb);
	void SetRpingmeshProbeTtlCallback(RpingmeshProbeTtlCallback cb);
	void SetRpingmeshAckReceivedCallback(RpingmeshAckReceivedCallback cb);
	void SetRpingmeshTracerouteOnMissingAck(bool enable);
	void SetRpingmeshTracerouteOnLoopFault(bool enable);
	void SetRpingmeshTracerouteOnCongestionFault(bool enable);
	void SetRpingmeshTracerouteTtl(uint8_t ttl);
	void SetOppAckReceivedCallback(OppAckReceivedCallback cb);
	void Setup(QpCompleteCallback cb); // setup shared data and callbacks with the QbbNetDevice
	static uint64_t GetQpKey(uint32_t dip, uint16_t sport, uint16_t pg); // get the lookup key for m_qpMap
	Ptr<RdmaQueuePair> GetQp(uint32_t dip, uint16_t sport, uint16_t pg); // get the qp
	uint32_t GetNicIdxOfQp(Ptr<RdmaQueuePair> qp); // get the NIC index of the qp
	void AddQueuePair(uint64_t size, uint16_t pg, Ipv4Address _sip, Ipv4Address _dip, uint16_t _sport, uint16_t _dport, uint32_t win, uint64_t baseRtt, uint32_t oppRemainingHopCount, uint32_t oppTokenSourceTorId, uint32_t oppTokenDstPodId, uint32_t oppProbeInstanceId, Callback<void> notifyAppFinish); // add a new qp (new send)
	void DeleteQueuePair(Ptr<RdmaQueuePair> qp);

	Ptr<RdmaRxQueuePair> GetRxQp(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport, uint16_t pg, bool create); // get a rxQp
	uint32_t GetNicIdxOfRxQp(Ptr<RdmaRxQueuePair> q); // get the NIC index of the rxQp
	void DeleteRxQp(uint32_t dip, uint16_t pg, uint16_t dport);

	int ReceiveUdp(Ptr<Packet> p, CustomHeader &ch);
	int ReceiveCnp(Ptr<Packet> p, CustomHeader &ch);
	int ReceiveAck(Ptr<Packet> p, CustomHeader &ch); // handle both ACK and NACK
	int Receive(Ptr<Packet> p, CustomHeader &ch); // callback function that the QbbNetDevice should use when receive packets. Only NIC can call this function. And do not call this upon PFC

	void CheckandSendQCN(Ptr<RdmaRxQueuePair> q);
	int ReceiverCheckSeq(uint32_t seq, Ptr<RdmaRxQueuePair> q, uint32_t size);
	void AddHeader (Ptr<Packet> p, uint16_t protocolNumber);
	static uint16_t EtherToPpp (uint16_t protocol);

	void RecoverQueue(Ptr<RdmaQueuePair> qp);
	void QpComplete(Ptr<RdmaQueuePair> qp);
	void SetLinkDown(Ptr<QbbNetDevice> dev);
	void WakeupOppTokenSender(Ptr<RdmaQueuePair> qp);
	void OnOppTokenReleased(Ptr<RdmaQueuePair> qp);
	Ptr<OppTokenManager> GetOppTokenManagerForInstance(uint32_t probeInstanceId) const;
	Ptr<OppTokenManager> GetOppTokenManagerForQp(Ptr<RdmaQueuePair> qp) const;
	void CheckAndSendRpingmeshTraceroute(Ptr<RdmaQueuePair> qp);
	void SendRpingmeshTraceroutePacket(Ptr<RdmaQueuePair> qp, uint32_t seq);

	// call this function after the NIC is setup
	void AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx);
	void AddDefaultRoute(uint32_t intf_idx);
	void ClearTable();
	void RedistributeQp();
	uint32_t PickRoute(const std::vector<int> &ports, uint32_t hash);

	bool CanSendQp(Ptr<RdmaQueuePair> qp);
	Ptr<Packet> GetNxtPacket(Ptr<RdmaQueuePair> qp); // get next packet to send, inc snd_nxt
	void PktSent(Ptr<RdmaQueuePair> qp, Ptr<Packet> pkt, Time interframeGap);
	void UpdateNextAvail(Ptr<RdmaQueuePair> qp, Time interframeGap, uint32_t pkt_size);
	void ChangeRate(Ptr<RdmaQueuePair> qp, DataRate new_rate);
	/******************************
	 * Mellanox's version of DCQCN
	 *****************************/
	double m_g; //feedback weight
	double m_rateOnFirstCNP; // the fraction of line rate to set on first CNP
	bool m_EcnClampTgtRate;
	double m_rpgTimeReset;
	double m_rateDecreaseInterval;
	uint32_t m_rpgThreshold;
	double m_alpha_resume_interval;
	DataRate m_rai;		//< Rate of additive increase
	DataRate m_rhai;		//< Rate of hyper-additive increase

	// the Mellanox's version of alpha update:
	// every fixed time slot, update alpha.
	void UpdateAlphaMlx(Ptr<RdmaQueuePair> q);
	void ScheduleUpdateAlphaMlx(Ptr<RdmaQueuePair> q);

	// Mellanox's version of CNP receive
	void cnp_received_mlx(Ptr<RdmaQueuePair> q);

	// Mellanox's version of rate decrease
	// It checks every m_rateDecreaseInterval if CNP arrived (m_decrease_cnp_arrived).
	// If so, decrease rate, and reset all rate increase related things
	void CheckRateDecreaseMlx(Ptr<RdmaQueuePair> q);
	void ScheduleDecreaseRateMlx(Ptr<RdmaQueuePair> q, uint32_t delta);

	// Mellanox's version of rate increase
	void RateIncEventTimerMlx(Ptr<RdmaQueuePair> q);
	void RateIncEventMlx(Ptr<RdmaQueuePair> q);
	void FastRecoveryMlx(Ptr<RdmaQueuePair> q);
	void ActiveIncreaseMlx(Ptr<RdmaQueuePair> q);
	void HyperIncreaseMlx(Ptr<RdmaQueuePair> q);

	/***********************
	 * High Precision CC
	 ***********************/
	double m_targetUtil;
	double m_utilHigh;
	uint32_t m_miThresh;
	bool m_multipleRate;
	bool m_sampleFeedback; // only react to feedback every RTT, or qlen > 0
	void HandleAckHp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch);
	void UpdateRateHp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool fast_react);
	void UpdateRateHpTest(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool fast_react);
	void FastReactHp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch);

	/**********************
	 * TIMELY
	 *********************/
	double m_tmly_alpha, m_tmly_beta;
	uint64_t m_tmly_TLow, m_tmly_THigh, m_tmly_minRtt;
	void HandleAckTimely(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch);
	void UpdateRateTimely(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool us);
	void FastReactTimely(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch);

	/**********************
	 * DCTCP
	 *********************/
	DataRate m_dctcp_rai;
	void HandleAckDctcp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch);

	/*********************
	 * HPCC-PINT
	 ********************/
	uint32_t pint_smpl_thresh;
	void SetPintSmplThresh(double p);
	void HandleAckHpPint(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch);
	void UpdateRateHpPint(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool fast_react);
};

} /* namespace ns3 */

#endif /* RDMA_HW_H */
