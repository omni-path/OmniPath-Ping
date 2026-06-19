#ifndef OPP_TOKEN_MANAGER_H
#define OPP_TOKEN_MANAGER_H

#include <ns3/callback.h>
#include <ns3/event-id.h>
#include <ns3/object.h>
#include <ns3/ptr.h>
#include <ns3/rdma-queue-pair.h>
#include <stdint.h>
#include <map>
#include <set>
#include <vector>

namespace ns3 {

class OppTokenManager : public Object {
public:
	enum TokenState {
		TOKEN_FREE = 0,
		TOKEN_HELD = 1,
		TOKEN_OCCUPIED = 2
	};

	enum UsmSamplePolicy {
		USM_SAMPLE_POLICY_QP_ORDINAL = 0,
		USM_SAMPLE_POLICY_TOR_GROUP_INTERVAL = 1
	};

	struct TokenGrant {
		bool valid;
		uint32_t tokenId;
		uint16_t tokenSeqNo;
		bool useUsm;

		TokenGrant() : valid(false), tokenId(0), tokenSeqNo(0), useUsm(false) {}
		TokenGrant(uint32_t id, uint16_t seq, bool usm) : valid(true), tokenId(id), tokenSeqNo(seq), useUsm(usm) {}
	};

	static TypeId GetTypeId(void);
	OppTokenManager();

	typedef Callback<void, Ptr<RdmaQueuePair> > QpCallback;

	void Configure(uint32_t torId, uint32_t tokensPerServer);
	void ConfigureFatTree(uint32_t torId, uint32_t m1, uint32_t m2, uint32_t m3, uint32_t podTorCount);
	void ConfigureLeafSpine(uint32_t torId, uint32_t m1, uint32_t m2);
	void SetProbeInstance(uint32_t instanceId, uint32_t instanceCount);
	void SetLegacyTokenIdStride(uint32_t stride);
	void SetGroupedSourceIdStride(uint32_t stride);
	void SetUsmSamplePeriod(uint32_t period);
	void SetUsmSampleRate(uint32_t numerator, uint32_t denominator);
	void SetUsmSamplePolicy(uint32_t policy);
	void SetInitialInterPodProbeSpreadNs(uint64_t spreadNs);
	void RegisterServer(uint32_t serverId);
	void RegisterQueuePair(Ptr<RdmaQueuePair> qp, uint32_t serverId, QpCallback wakeupCb, QpCallback tokenReleaseCb);
	void UnregisterQueuePair(Ptr<RdmaQueuePair> qp);

	bool TryPrepareToSend(Ptr<RdmaQueuePair> qp);
	TokenGrant MarkPacketSent(Ptr<RdmaQueuePair> qp, uint32_t psn);
	bool ProbeAckToken(Ptr<RdmaQueuePair> qp, uint32_t ackPsn, uint32_t &tokenId, uint16_t &tokenSeqNo) const;
	bool OnOppAck(Ptr<RdmaQueuePair> qp, uint32_t tokenId, uint16_t tokenSeqNo);
	bool HasToken(Ptr<RdmaQueuePair> qp) const;

	uint32_t GetTorId() const;
	uint32_t GetTokensPerServer() const;

private:
	enum TokenMode {
		TOKEN_MODE_LEGACY = 0,
		TOKEN_MODE_FAT_TREE = 1,
		TOKEN_MODE_LEAF_SPINE = 2
	};

	struct Token {
		uint32_t id;
		uint16_t seqNo;
		uint32_t serverId;
		uint32_t sourceTorId;
		uint32_t dstPodId;
		TokenState state;
		Ptr<RdmaQueuePair> qp;
		uint32_t psn;
		bool useUsm;
		EventId timeoutEvent;

		Token() : id(0), seqNo(1), serverId(0), sourceTorId(0), dstPodId(0), state(TOKEN_FREE), qp(NULL), psn(0), useUsm(false) {}
		Token(uint32_t tokenId, uint32_t ownerServer)
			: id(tokenId), seqNo(1), serverId(ownerServer), sourceTorId(0), dstPodId(0), state(TOKEN_FREE), qp(NULL), psn(0), useUsm(false) {}
		Token(uint32_t tokenId, uint32_t sourceTor, uint32_t dstPod)
			: id(tokenId), seqNo(1), serverId(0), sourceTorId(sourceTor), dstPodId(dstPod), state(TOKEN_FREE), qp(NULL), psn(0), useUsm(false) {}
	};

	struct QpInfo {
		Ptr<RdmaQueuePair> qp;
		uint32_t serverId;
		QpCallback wakeupCb;
		QpCallback tokenReleaseCb;

		QpInfo() : qp(NULL), serverId(0) {}
		QpInfo(Ptr<RdmaQueuePair> q, uint32_t server, QpCallback wakeup, QpCallback release)
			: qp(q), serverId(server), wakeupCb(wakeup), tokenReleaseCb(release) {}
	};

	int FindQpInfo(Ptr<RdmaQueuePair> qp) const;
	int FindHeldToken(Ptr<RdmaQueuePair> qp) const;
	bool IsSameQp(Ptr<RdmaQueuePair> a, Ptr<RdmaQueuePair> b) const;
	bool IsFatTreeMode() const;
	bool IsLeafSpineMode() const;
	bool IsGroupedMode() const;
	bool QpMatchesToken(const Token &token, Ptr<RdmaQueuePair> qp) const;
	bool SameTokenGroup(uint32_t aSourceTorId, uint32_t aDstId, uint32_t bSourceTorId, uint32_t bDstId) const;
	uint32_t EncodeGroupedSourceId(uint32_t sourceTorId) const;
	uint32_t GetFatTreeTokenCount(uint32_t sourceTorId, uint32_t dstPodId) const;
	uint32_t EncodeFatTreeTokenId(uint32_t sourceTorId, uint32_t dstPodId, uint32_t tokenNo) const;
	void EnsureFatTreeTokenGroup(uint32_t sourceTorId, uint32_t dstPodId);
	uint32_t GetLeafSpineTokenCount(uint32_t sourceTorId, uint32_t dstTorId) const;
	uint32_t EncodeLeafSpineTokenId(uint32_t sourceTorId, uint32_t dstTorId, uint32_t tokenNo) const;
	void EnsureLeafSpineTokenGroup(uint32_t sourceTorId, uint32_t dstTorId);
	uintptr_t GetQpSampleKey(Ptr<RdmaQueuePair> qp) const;
	int FindQpInfoBySampleKey(uintptr_t sampleKey) const;
	uint32_t GetQpSampleOrdinal(Ptr<RdmaQueuePair> qp) const;
	uint32_t GetQpSampleSentCount(Ptr<RdmaQueuePair> qp) const;
	uint32_t GetSampleGroupKey(uint32_t sourceTorId, uint32_t dstId) const;
	uint32_t GetSampleGroupKey(const Token &token) const;
	uint32_t GetSampleGroupKey(Ptr<RdmaQueuePair> qp) const;
	uint64_t GetTorGroupSampleAccumulatorKey(uint32_t groupKey) const;
	bool PrepareTokenGrantQpOrdinal(Token &token, Ptr<RdmaQueuePair> qp);
	bool PrepareTokenGrantTorGroupInterval(Token &token);
	uint32_t GetInitialInterPodGroupKey(uint32_t sourceTorId, uint32_t dstPodId) const;
	bool IsInitialInterPodQp(Ptr<RdmaQueuePair> qp) const;
	void AddInitialInterPodQp(Ptr<RdmaQueuePair> qp);
	uint32_t GetInitialInterPodTaskCount(uint32_t sourceTorId, uint32_t dstPodId) const;
	uint32_t GetInitialInterPodDstPodRank(uint32_t sourceTorId, uint32_t dstPodId) const;
	bool IsInitialInterPodPacedToken(const Token &token) const;
	uint64_t GetInitialInterPodProbeSpacingNs() const;
	uint64_t GetInitialInterPodSourceTorOffsetNs(uint32_t sourceTorId) const;
	uint64_t GetInitialInterPodRegistrationSettleNs() const;
	bool InitialInterPodPacingReady(Token &token, Ptr<RdmaQueuePair> qp);
	void WakeInitialInterPodQp(uintptr_t sampleKey);
	void ClearInitialInterPodWakeup(uintptr_t sampleKey);
	bool PrepareTokenGrant(Token &token, Ptr<RdmaQueuePair> qp);
	void CommitTokenGrant(const Token &token);
	bool AssignTokenToNextQp(uint32_t tokenIdx);
	void WakeQp(Ptr<RdmaQueuePair> qp);
	void NotifyTokenReleased(Ptr<RdmaQueuePair> qp);
	void TokenTimeout(uint32_t tokenId, uint16_t tokenSeqNo, uint32_t psn, Ptr<RdmaQueuePair> qp);
	bool ReleaseOccupiedToken(uint32_t tokenId, uint16_t tokenSeqNo, Ptr<RdmaQueuePair> qp, bool cancelTimeout, const char *reason);

	uint32_t m_torId;
	uint32_t m_tokensPerServer;
	uint32_t m_fatTreeM1;
	uint32_t m_fatTreeM2;
	uint32_t m_fatTreeM3;
	uint32_t m_fatTreePodTorCount;
	uint32_t m_leafSpineM1;
	uint32_t m_leafSpineM2;
	uint32_t m_probeInstanceId;
	uint32_t m_probeInstanceCount;
	uint32_t m_legacyTokenIdStride;
	uint32_t m_groupedSourceIdStride;
	uint32_t m_usmSampleNumerator;
	uint32_t m_usmSampleDenominator;
	uint32_t m_usmSamplePolicy;
	uint32_t m_assignCursor;
	uint64_t m_initialInterPodProbeSpreadNs;
	TokenMode m_tokenMode;
	std::set<uint32_t> m_registeredServers;
	std::set<uint32_t> m_fatTreeGroups;
	std::set<uint32_t> m_leafSpineGroups;
	std::map<uint32_t, uint32_t> m_usmSampleCounters;
	std::map<uintptr_t, uint32_t> m_usmSampleQpOrdinals;
	std::map<uintptr_t, uint32_t> m_usmSampleQpSentCounts;
	std::map<uintptr_t, EventId> m_initialInterPodWakeups;
	std::map<uint32_t, uint64_t> m_initialInterPodBaseNs;
	std::set<uint32_t> m_initialInterPodBaseSet;
	std::map<uint32_t, uint32_t> m_initialInterPodTaskCounts;
	std::map<uint32_t, uint32_t> m_initialInterPodIssuedByGroup;
	std::map<uint32_t, uint32_t> m_usmSampleNextOrdinals;
	std::vector<Token> m_tokens;
	std::vector<QpInfo> m_qps;
};

} // namespace ns3

#endif /* OPP_TOKEN_MANAGER_H */
