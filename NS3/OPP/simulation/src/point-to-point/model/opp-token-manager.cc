#include "opp-token-manager.h"
#include <ns3/assert.h>
#include <ns3/log.h>
#include <ns3/simulator.h>
#include <cstdlib>
#include <iostream>

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("OppTokenManager");
NS_OBJECT_ENSURE_REGISTERED(OppTokenManager);

static std::map<uint64_t, uint32_t> g_torGroupUsmSampleAccumulators;

TypeId
OppTokenManager::GetTypeId(void)
{
	static TypeId tid = TypeId("ns3::OppTokenManager")
		.SetParent<Object>()
		.AddConstructor<OppTokenManager>()
		;
	return tid;
}

OppTokenManager::OppTokenManager()
	: m_torId(0),
	  m_tokensPerServer(1),
	  m_fatTreeM1(1),
	  m_fatTreeM2(1),
	  m_fatTreeM3(1),
	  m_fatTreePodTorCount(1),
	  m_leafSpineM1(1),
	  m_leafSpineM2(1),
	  m_probeInstanceId(0),
	  m_probeInstanceCount(1),
	  m_legacyTokenIdStride(0),
	  m_groupedSourceIdStride(0),
	  m_usmSampleNumerator(1),
	  m_usmSampleDenominator(1),
	  m_usmSamplePolicy(USM_SAMPLE_POLICY_QP_ORDINAL),
	  m_assignCursor(0),
	  m_initialInterPodProbeSpreadNs(0),
	  m_tokenMode(TOKEN_MODE_LEGACY)
{
}

void
OppTokenManager::Configure(uint32_t torId, uint32_t tokensPerServer)
{
	NS_ASSERT_MSG(tokensPerServer > 0, "OPP token count per server must be positive");
	m_torId = torId;
	m_tokensPerServer = tokensPerServer;
	m_tokenMode = TOKEN_MODE_LEGACY;
}

void
OppTokenManager::ConfigureFatTree(uint32_t torId, uint32_t m1, uint32_t m2, uint32_t m3, uint32_t podTorCount)
{
	NS_ASSERT_MSG(m1 > 0 && m1 <= 256, "OPP_FATTREE_M1 must be in [1, 256]");
	NS_ASSERT_MSG(m2 > 0 && m2 <= 256, "OPP_FATTREE_M2 must be in [1, 256]");
	NS_ASSERT_MSG(m3 > 0 && m3 <= 256, "OPP_FATTREE_M3 must be in [1, 256]");
	NS_ASSERT_MSG(podTorCount > 0 && podTorCount <= 256, "fat-tree pod ToR count must be in [1, 256]");
	m_torId = torId;
	m_fatTreeM1 = m1;
	m_fatTreeM2 = m2;
	m_fatTreeM3 = m3;
	m_fatTreePodTorCount = podTorCount;
	m_tokenMode = TOKEN_MODE_FAT_TREE;
}

void
OppTokenManager::ConfigureLeafSpine(uint32_t torId, uint32_t m1, uint32_t m2)
{
	NS_ASSERT_MSG(m1 > 0 && m1 <= 256, "OPP_LEAFSPINE_M1 must be in [1, 256]");
	NS_ASSERT_MSG(m2 > 0 && m2 <= 256, "OPP_LEAFSPINE_M2 must be in [1, 256]");
	m_torId = torId;
	m_leafSpineM1 = m1;
	m_leafSpineM2 = m2;
	m_tokenMode = TOKEN_MODE_LEAF_SPINE;
}

void
OppTokenManager::SetProbeInstance(uint32_t instanceId, uint32_t instanceCount)
{
	NS_ASSERT_MSG(instanceCount > 0, "OPP probe instance count must be positive");
	NS_ASSERT_MSG(instanceId < instanceCount, "OPP probe instance ID must be smaller than instance count");
	m_probeInstanceId = instanceId;
	m_probeInstanceCount = instanceCount;
}

void
OppTokenManager::SetLegacyTokenIdStride(uint32_t stride)
{
	NS_ASSERT_MSG(stride > 0, "legacy OPP token ID stride must be positive");
	m_legacyTokenIdStride = stride;
}

void
OppTokenManager::SetGroupedSourceIdStride(uint32_t stride)
{
	NS_ASSERT_MSG(stride > 0, "grouped OPP source ID stride must be positive");
	m_groupedSourceIdStride = stride;
}

void
OppTokenManager::SetUsmSamplePeriod(uint32_t period)
{
	NS_ASSERT_MSG(period > 0, "OPP_USM_SAMPLE_PERIOD must be positive");
	SetUsmSampleRate(1, period);
}

void
OppTokenManager::SetUsmSampleRate(uint32_t numerator, uint32_t denominator)
{
	NS_ASSERT_MSG(denominator > 0, "OPP USM sample-rate denominator must be positive");
	NS_ASSERT_MSG(numerator <= denominator, "OPP USM sample-rate numerator must not exceed denominator");
	m_usmSampleNumerator = numerator;
	m_usmSampleDenominator = denominator;
}

void
OppTokenManager::SetUsmSamplePolicy(uint32_t policy)
{
	NS_ASSERT_MSG(policy == USM_SAMPLE_POLICY_QP_ORDINAL ||
		policy == USM_SAMPLE_POLICY_TOR_GROUP_INTERVAL,
		"OPP USM sample policy must be 0/QP_ORDINAL or 1/TOR_GROUP_INTERVAL");
	m_usmSamplePolicy = policy;
}

void
OppTokenManager::SetInitialInterPodProbeSpreadNs(uint64_t spreadNs)
{
	m_initialInterPodProbeSpreadNs = spreadNs;
	for (std::map<uintptr_t, EventId>::iterator it = m_initialInterPodWakeups.begin();
			it != m_initialInterPodWakeups.end(); ++it){
		if (!it->second.IsExpired())
			Simulator::Cancel(it->second);
	}
	m_initialInterPodWakeups.clear();
	m_initialInterPodBaseNs.clear();
	m_initialInterPodBaseSet.clear();
	m_initialInterPodIssuedByGroup.clear();
}

void
OppTokenManager::RegisterServer(uint32_t serverId)
{
	if (m_registeredServers.find(serverId) != m_registeredServers.end())
		return;

	m_registeredServers.insert(serverId);
	if (IsGroupedMode())
		return;

	if (m_probeInstanceCount > 1)
		NS_ASSERT_MSG(m_legacyTokenIdStride > 0, "legacy OPP multi-instance mode requires a token ID stride");
	uint64_t base64 = (uint64_t)serverId * (uint64_t)m_tokensPerServer;
	if (m_probeInstanceCount > 1)
		base64 += (uint64_t)m_probeInstanceId * (uint64_t)m_legacyTokenIdStride;
	NS_ASSERT_MSG(base64 + m_tokensPerServer - 1 <= 0xffffffffull,
		"legacy OPP token ID exceeds 32 bits");
	uint32_t base = (uint32_t)base64;
	for (uint32_t i = 0; i < m_tokensPerServer; i++)
		m_tokens.push_back(Token(base + i, serverId));
}

int
OppTokenManager::FindQpInfo(Ptr<RdmaQueuePair> qp) const
{
	for (uint32_t i = 0; i < m_qps.size(); i++){
		if (IsSameQp(m_qps[i].qp, qp))
			return (int)i;
	}
	return -1;
}

void
OppTokenManager::RegisterQueuePair(Ptr<RdmaQueuePair> qp, uint32_t serverId, QpCallback wakeupCb, QpCallback tokenReleaseCb)
{
	RegisterServer(serverId);
	if (FindQpInfo(qp) >= 0)
		return;
	uint32_t groupKey = GetSampleGroupKey(qp);
	m_usmSampleQpOrdinals[GetQpSampleKey(qp)] = m_usmSampleNextOrdinals[groupKey]++;
	m_usmSampleQpSentCounts[GetQpSampleKey(qp)] = 0;
	m_qps.push_back(QpInfo(qp, serverId, wakeupCb, tokenReleaseCb));
	AddInitialInterPodQp(qp);
}

bool
OppTokenManager::IsSameQp(Ptr<RdmaQueuePair> a, Ptr<RdmaQueuePair> b) const
{
	return PeekPointer(a) == PeekPointer(b);
}

bool
OppTokenManager::IsFatTreeMode() const
{
	return m_tokenMode == TOKEN_MODE_FAT_TREE;
}

bool
OppTokenManager::IsLeafSpineMode() const
{
	return m_tokenMode == TOKEN_MODE_LEAF_SPINE;
}

bool
OppTokenManager::IsGroupedMode() const
{
	return IsFatTreeMode() || IsLeafSpineMode();
}

bool
OppTokenManager::SameTokenGroup(uint32_t aSourceTorId, uint32_t aDstId, uint32_t bSourceTorId, uint32_t bDstId) const
{
	return aSourceTorId == bSourceTorId && aDstId == bDstId;
}

uint32_t
OppTokenManager::EncodeGroupedSourceId(uint32_t sourceTorId) const
{
	if (m_probeInstanceCount <= 1)
		return sourceTorId;
	NS_ASSERT_MSG(m_groupedSourceIdStride > 0, "grouped OPP multi-instance mode requires a source ID stride");
	uint64_t encoded = (uint64_t)m_probeInstanceId * (uint64_t)m_groupedSourceIdStride + sourceTorId;
	NS_ASSERT_MSG(encoded <= 0xffffull, "grouped OPP encoded source ID exceeds 16 bits");
	return (uint32_t)encoded;
}

bool
OppTokenManager::QpMatchesToken(const Token &token, Ptr<RdmaQueuePair> qp) const
{
	int qpi = FindQpInfo(qp);
	if (qpi < 0)
		return false;
	if (IsGroupedMode())
		return SameTokenGroup(token.sourceTorId, token.dstPodId, qp->m_oppTokenSourceTorId, qp->m_oppTokenDstPodId);
	return m_qps[qpi].serverId == token.serverId;
}

uint32_t
OppTokenManager::GetFatTreeTokenCount(uint32_t sourceTorId, uint32_t dstPodId) const
{
	if (sourceTorId == 0){
		NS_ASSERT_MSG(dstPodId == 0, "fat-tree ToR-internal token group must have dstPodId 0");
		return m_fatTreeM1;
	}
	if (dstPodId == 0)
		return m_fatTreeM2;
	return m_fatTreeM3;
}

uint32_t
OppTokenManager::EncodeFatTreeTokenId(uint32_t sourceTorId, uint32_t dstPodId, uint32_t tokenNo) const
{
	NS_ASSERT_MSG(sourceTorId <= 0xffff, "fat-tree OPP source ToR ID exceeds 16 bits");
	NS_ASSERT_MSG(dstPodId <= 0xff, "fat-tree OPP destination PoD ID exceeds 8 bits");
	NS_ASSERT_MSG(tokenNo <= 0xff, "fat-tree OPP token number exceeds 8 bits");
	uint32_t encodedSourceTorId = EncodeGroupedSourceId(sourceTorId);
	return (encodedSourceTorId << 16) | (dstPodId << 8) | tokenNo;
}

void
OppTokenManager::EnsureFatTreeTokenGroup(uint32_t sourceTorId, uint32_t dstPodId)
{
	NS_ASSERT_MSG(IsFatTreeMode(), "fat-tree token groups require fat-tree token manager mode");
	NS_ASSERT_MSG(sourceTorId <= 0xffff, "fat-tree OPP source ToR ID exceeds 16 bits");
	NS_ASSERT_MSG(dstPodId <= 0xff, "fat-tree OPP destination PoD ID exceeds 8 bits");
	uint32_t groupKey = (sourceTorId << 8) | dstPodId;
	if (m_fatTreeGroups.find(groupKey) != m_fatTreeGroups.end())
		return;

	uint32_t tokenCount = GetFatTreeTokenCount(sourceTorId, dstPodId);
	for (uint32_t tokenNo = 0; tokenNo < tokenCount; tokenNo++)
		m_tokens.push_back(Token(EncodeFatTreeTokenId(sourceTorId, dstPodId, tokenNo), sourceTorId, dstPodId));
	m_fatTreeGroups.insert(groupKey);
	NS_LOG_INFO("tor " << m_torId << " creates fat-tree OPP token group source_tor "
		<< sourceTorId << " dst_pod " << dstPodId << " tokens " << tokenCount);
}

uint32_t
OppTokenManager::GetLeafSpineTokenCount(uint32_t sourceTorId, uint32_t dstTorId) const
{
	NS_ASSERT_MSG(sourceTorId > 0, "leaf-spine OPP source ToR ID must be positive");
	if (dstTorId == 0)
		return m_leafSpineM1;
	return m_leafSpineM2;
}

uint32_t
OppTokenManager::EncodeLeafSpineTokenId(uint32_t sourceTorId, uint32_t dstTorId, uint32_t tokenNo) const
{
	NS_ASSERT_MSG(sourceTorId > 0 && sourceTorId <= 0xffff, "leaf-spine OPP source ToR ID exceeds 16 bits");
	NS_ASSERT_MSG(dstTorId <= 0xff, "leaf-spine OPP destination ToR ID exceeds 8 bits");
	NS_ASSERT_MSG(tokenNo <= 0xff, "leaf-spine OPP token number exceeds 8 bits");
	uint32_t encodedSourceTorId = EncodeGroupedSourceId(sourceTorId);
	return (encodedSourceTorId << 16) | (dstTorId << 8) | tokenNo;
}

void
OppTokenManager::EnsureLeafSpineTokenGroup(uint32_t sourceTorId, uint32_t dstTorId)
{
	NS_ASSERT_MSG(IsLeafSpineMode(), "leaf-spine token groups require leaf-spine token manager mode");
	NS_ASSERT_MSG(sourceTorId > 0 && sourceTorId <= 0xffff, "leaf-spine OPP source ToR ID exceeds 16 bits");
	NS_ASSERT_MSG(dstTorId <= 0xff, "leaf-spine OPP destination ToR ID exceeds 8 bits");
	uint32_t groupKey = (sourceTorId << 8) | dstTorId;
	if (m_leafSpineGroups.find(groupKey) != m_leafSpineGroups.end())
		return;

	uint32_t tokenCount = GetLeafSpineTokenCount(sourceTorId, dstTorId);
	for (uint32_t tokenNo = 0; tokenNo < tokenCount; tokenNo++)
		m_tokens.push_back(Token(EncodeLeafSpineTokenId(sourceTorId, dstTorId, tokenNo), sourceTorId, dstTorId));
	m_leafSpineGroups.insert(groupKey);
	NS_LOG_INFO("tor " << m_torId << " creates leaf-spine OPP token group source_tor "
		<< sourceTorId << " dst_tor " << dstTorId << " tokens " << tokenCount);
}

int
OppTokenManager::FindHeldToken(Ptr<RdmaQueuePair> qp) const
{
	for (uint32_t i = 0; i < m_tokens.size(); i++){
		if (m_tokens[i].state == TOKEN_HELD && IsSameQp(m_tokens[i].qp, qp))
			return (int)i;
	}
	return -1;
}

uintptr_t
OppTokenManager::GetQpSampleKey(Ptr<RdmaQueuePair> qp) const
{
	return (uintptr_t)PeekPointer(qp);
}

int
OppTokenManager::FindQpInfoBySampleKey(uintptr_t sampleKey) const
{
	for (uint32_t i = 0; i < m_qps.size(); i++){
		Ptr<RdmaQueuePair> qp = m_qps[i].qp;
		if (qp != NULL && GetQpSampleKey(qp) == sampleKey)
			return (int)i;
	}
	return -1;
}

uint32_t
OppTokenManager::GetQpSampleOrdinal(Ptr<RdmaQueuePair> qp) const
{
	std::map<uintptr_t, uint32_t>::const_iterator it = m_usmSampleQpOrdinals.find(GetQpSampleKey(qp));
	NS_ASSERT_MSG(it != m_usmSampleQpOrdinals.end(), "OPP QP has no sampled-USM ordinal");
	return it->second;
}

uint32_t
OppTokenManager::GetQpSampleSentCount(Ptr<RdmaQueuePair> qp) const
{
	std::map<uintptr_t, uint32_t>::const_iterator it = m_usmSampleQpSentCounts.find(GetQpSampleKey(qp));
	NS_ASSERT_MSG(it != m_usmSampleQpSentCounts.end(), "OPP QP has no sampled-USM sent count");
	return it->second;
}

uint32_t
OppTokenManager::GetSampleGroupKey(uint32_t sourceTorId, uint32_t dstId) const
{
	if (IsFatTreeMode()){
		if (sourceTorId == 0)
			return 1;
		if (dstId == 0)
			return 2;
		return 3;
	}
	if (IsLeafSpineMode())
		return dstId == 0 ? 1 : 2;
	return 0;
}

uint32_t
OppTokenManager::GetSampleGroupKey(const Token &token) const
{
	return GetSampleGroupKey(token.sourceTorId, token.dstPodId);
}

uint32_t
OppTokenManager::GetSampleGroupKey(Ptr<RdmaQueuePair> qp) const
{
	if (IsGroupedMode())
		return GetSampleGroupKey(qp->m_oppTokenSourceTorId, qp->m_oppTokenDstPodId);
	return 0;
}

uint64_t
OppTokenManager::GetTorGroupSampleAccumulatorKey(uint32_t groupKey) const
{
	return ((uint64_t)m_torId << 32) | (uint64_t)groupKey;
}

bool
OppTokenManager::PrepareTokenGrantQpOrdinal(Token &token, Ptr<RdmaQueuePair> qp)
{
	uint32_t ordinal = GetQpSampleOrdinal(qp);
	uint32_t sentCount = GetQpSampleSentCount(qp);
	uint32_t slot = (ordinal + sentCount) % m_usmSampleDenominator;
	token.useUsm = slot < m_usmSampleNumerator;
	return true;
}

bool
OppTokenManager::PrepareTokenGrantTorGroupInterval(Token &token)
{
	uint32_t groupKey = GetSampleGroupKey(token);
	uint64_t accumulatorKey = GetTorGroupSampleAccumulatorKey(groupKey);
	uint32_t &accumulator = g_torGroupUsmSampleAccumulators[accumulatorKey];
	if (accumulator < m_usmSampleNumerator){
		accumulator += m_usmSampleDenominator - m_usmSampleNumerator;
		token.useUsm = true;
	}else{
		accumulator -= m_usmSampleNumerator;
		token.useUsm = false;
	}
	return true;
}

uint32_t
OppTokenManager::GetInitialInterPodGroupKey(uint32_t sourceTorId, uint32_t dstPodId) const
{
	NS_ASSERT_MSG(sourceTorId <= 0xffff, "initial inter-pod source ToR ID exceeds 16 bits");
	NS_ASSERT_MSG(dstPodId <= 0xff, "initial inter-pod destination PoD ID exceeds 8 bits");
	return (sourceTorId << 8) | dstPodId;
}

bool
OppTokenManager::IsInitialInterPodQp(Ptr<RdmaQueuePair> qp) const
{
	return IsFatTreeMode() && qp != NULL &&
		qp->m_oppTokenSourceTorId != 0 && qp->m_oppTokenDstPodId != 0;
}

void
OppTokenManager::AddInitialInterPodQp(Ptr<RdmaQueuePair> qp)
{
	if (!IsInitialInterPodQp(qp))
		return;
	uint32_t groupKey = GetInitialInterPodGroupKey(qp->m_oppTokenSourceTorId, qp->m_oppTokenDstPodId);
	m_initialInterPodTaskCounts[groupKey]++;
}

uint32_t
OppTokenManager::GetInitialInterPodTaskCount(uint32_t sourceTorId, uint32_t dstPodId) const
{
	uint32_t groupKey = GetInitialInterPodGroupKey(sourceTorId, dstPodId);
	std::map<uint32_t, uint32_t>::const_iterator selfIt = m_initialInterPodTaskCounts.find(groupKey);
	if (selfIt != m_initialInterPodTaskCounts.end())
		return selfIt->second;
	return 0;
}

uint32_t
OppTokenManager::GetInitialInterPodDstPodRank(uint32_t sourceTorId, uint32_t dstPodId) const
{
	uint32_t taskCount = GetInitialInterPodTaskCount(sourceTorId, dstPodId);
	uint32_t rank = 0;
	for (std::map<uint32_t, uint32_t>::const_iterator it = m_initialInterPodTaskCounts.begin();
			it != m_initialInterPodTaskCounts.end(); ++it){
		uint32_t otherSourceTorId = it->first >> 8;
		uint32_t otherDstPodId = it->first & 0xff;
		if (otherSourceTorId != sourceTorId || otherDstPodId == dstPodId)
			continue;
		if (it->second > taskCount || (it->second == taskCount && otherDstPodId < dstPodId))
			rank++;
	}
	return rank;
}

bool
OppTokenManager::IsInitialInterPodPacedToken(const Token &token) const
{
	return IsFatTreeMode() && m_initialInterPodProbeSpreadNs > 0 &&
		token.sourceTorId != 0 && token.dstPodId != 0 && token.seqNo == 1;
}

uint64_t
OppTokenManager::GetInitialInterPodProbeSpacingNs() const
{
	uint64_t denominator = 4ull * (uint64_t)m_fatTreeM3;
	NS_ASSERT_MSG(denominator > 0, "initial inter-pod probe pacing denominator must be positive");
	uint64_t spacing = (m_initialInterPodProbeSpreadNs + denominator - 1) / denominator;
	return spacing == 0 ? 1 : spacing;
}

uint64_t
OppTokenManager::GetInitialInterPodSourceTorOffsetNs(uint32_t sourceTorId) const
{
	if (sourceTorId == 0 || (m_fatTreePodTorCount <= 1 && m_probeInstanceCount <= 1))
		return 0;
	uint32_t podLocalTorId = (sourceTorId - 1) % m_fatTreePodTorCount;
	uint64_t denominator = 4ull * (uint64_t)m_fatTreeM3 *
		(uint64_t)m_fatTreePodTorCount * (uint64_t)m_probeInstanceCount;
	NS_ASSERT_MSG(denominator > 0, "initial inter-pod ToR/instance offset denominator must be positive");
	uint64_t stepNs = (m_initialInterPodProbeSpreadNs + denominator - 1) / denominator;
	if (stepNs == 0)
		stepNs = 1;
	uint64_t offsetRank = (uint64_t)podLocalTorId * (uint64_t)m_probeInstanceCount +
		(uint64_t)m_probeInstanceId;
	return offsetRank * stepNs;
}

uint64_t
OppTokenManager::GetInitialInterPodRegistrationSettleNs() const
{
	return 1;
}

void
OppTokenManager::WakeInitialInterPodQp(uintptr_t sampleKey)
{
	m_initialInterPodWakeups.erase(sampleKey);
	int qpi = FindQpInfoBySampleKey(sampleKey);
	if (qpi < 0)
		return;
	WakeQp(m_qps[qpi].qp);
}

void
OppTokenManager::ClearInitialInterPodWakeup(uintptr_t sampleKey)
{
	std::map<uintptr_t, EventId>::iterator it = m_initialInterPodWakeups.find(sampleKey);
	if (it == m_initialInterPodWakeups.end())
		return;
	if (!it->second.IsExpired())
		Simulator::Cancel(it->second);
	m_initialInterPodWakeups.erase(it);
}

bool
OppTokenManager::InitialInterPodPacingReady(Token &token, Ptr<RdmaQueuePair> qp)
{
	if (!IsInitialInterPodPacedToken(token))
		return true;

	uint64_t nowNs = Simulator::Now().GetTimeStep();
	if (m_initialInterPodBaseSet.find(token.sourceTorId) == m_initialInterPodBaseSet.end()){
		m_initialInterPodBaseNs[token.sourceTorId] =
			qp->startTime.GetTimeStep() + GetInitialInterPodRegistrationSettleNs();
		m_initialInterPodBaseSet.insert(token.sourceTorId);
	}

	uint32_t groupKey = GetInitialInterPodGroupKey(token.sourceTorId, token.dstPodId);
	uint32_t taskCount = GetInitialInterPodTaskCount(token.sourceTorId, token.dstPodId);
	uint32_t dstPodRank = GetInitialInterPodDstPodRank(token.sourceTorId, token.dstPodId);
	uint32_t issued = m_initialInterPodIssuedByGroup[groupKey];
	uint64_t slot = (uint64_t)dstPodRank * (uint64_t)m_fatTreeM3 + (uint64_t)issued;
	uint64_t spacingNs = GetInitialInterPodProbeSpacingNs();
	uint64_t torOffsetNs = GetInitialInterPodSourceTorOffsetNs(token.sourceTorId);
	uint64_t eligibleNs = m_initialInterPodBaseNs[token.sourceTorId] +
		torOffsetNs + slot * spacingNs;
	if (nowNs >= eligibleNs){
		m_initialInterPodIssuedByGroup[groupKey] = issued + 1;
		ClearInitialInterPodWakeup(GetQpSampleKey(qp));
		if (std::getenv("OPP_INITIAL_INTERPOD_PACING_LOG") != NULL){
			uint32_t localTorId = token.sourceTorId == 0 ? 0 :
				(token.sourceTorId - 1) % m_fatTreePodTorCount;
			uint64_t offsetRank = (uint64_t)localTorId * (uint64_t)m_probeInstanceCount +
				(uint64_t)m_probeInstanceId;
			std::cout << "OPP_INITIAL_INTERPOD_PACING"
				<< " time_ns " << nowNs
				<< " start_ns " << qp->startTime.GetTimeStep()
				<< " base_ns " << m_initialInterPodBaseNs[token.sourceTorId]
				<< " source_tor " << token.sourceTorId
				<< " local_tor " << localTorId
				<< " instance_id " << m_probeInstanceId
				<< " instance_count " << m_probeInstanceCount
				<< " dst_pod " << token.dstPodId
				<< " task_count " << taskCount
				<< " rank " << dstPodRank
				<< " issued " << issued
				<< " m3 " << m_fatTreeM3
				<< " spacing_ns " << spacingNs
				<< " offset_rank " << offsetRank
				<< " tor_offset_ns " << torOffsetNs
				<< " slot " << slot
				<< " eligible_ns " << eligibleNs
				<< " token " << token.id
				<< " token_seq " << token.seqNo
				<< std::endl;
		}
		return true;
	}

	uintptr_t sampleKey = GetQpSampleKey(qp);
	std::map<uintptr_t, EventId>::iterator wakeIt = m_initialInterPodWakeups.find(sampleKey);
	if (wakeIt == m_initialInterPodWakeups.end() || wakeIt->second.IsExpired()){
		uint64_t delayNs = eligibleNs - nowNs;
		m_initialInterPodWakeups[sampleKey] = Simulator::Schedule(
			NanoSeconds(delayNs), &OppTokenManager::WakeInitialInterPodQp, this, sampleKey);
	}
	return false;
}

bool
OppTokenManager::PrepareTokenGrant(Token &token, Ptr<RdmaQueuePair> qp)
{
	token.useUsm = false;
	if (m_usmSampleNumerator == 0)
		return true;
	if (m_usmSampleNumerator >= m_usmSampleDenominator){
		token.useUsm = true;
		return true;
	}

	if (m_usmSamplePolicy == USM_SAMPLE_POLICY_TOR_GROUP_INTERVAL)
		return PrepareTokenGrantTorGroupInterval(token);
	return PrepareTokenGrantQpOrdinal(token, qp);
}

void
OppTokenManager::CommitTokenGrant(const Token &token)
{
	uint32_t groupKey = GetSampleGroupKey(token);
	m_usmSampleCounters[groupKey]++;
	uintptr_t sampleKey = GetQpSampleKey(token.qp);
	m_usmSampleQpSentCounts[sampleKey]++;
}

bool
OppTokenManager::HasToken(Ptr<RdmaQueuePair> qp) const
{
	for (uint32_t i = 0; i < m_tokens.size(); i++){
		if (m_tokens[i].state != TOKEN_FREE && IsSameQp(m_tokens[i].qp, qp))
			return true;
	}
	return false;
}

bool
OppTokenManager::TryPrepareToSend(Ptr<RdmaQueuePair> qp)
{
	int qpi = FindQpInfo(qp);
	NS_ASSERT_MSG(qpi >= 0, "OPP QP is not registered with its ToR token manager");

	if (qp->GetBytesLeft() == 0)
		return false;
	if (FindHeldToken(qp) >= 0)
		return true;
	if (HasToken(qp))
		return false;

	if (IsFatTreeMode()){
		EnsureFatTreeTokenGroup(qp->m_oppTokenSourceTorId, qp->m_oppTokenDstPodId);
		for (uint32_t i = 0; i < m_tokens.size(); i++){
			Token &token = m_tokens[i];
			if (token.state != TOKEN_FREE || !QpMatchesToken(token, qp) ||
					!InitialInterPodPacingReady(token, qp) || !PrepareTokenGrant(token, qp))
				continue;
			token.state = TOKEN_HELD;
			token.qp = qp;
			token.psn = 0;
			NS_LOG_INFO("tor " << m_torId << " holds fat-tree token "
				<< token.id << " seq " << token.seqNo << " source_tor "
				<< token.sourceTorId << " dst_pod " << token.dstPodId);
			return true;
		}
		return false;
	}
	if (IsLeafSpineMode()){
		EnsureLeafSpineTokenGroup(qp->m_oppTokenSourceTorId, qp->m_oppTokenDstPodId);
		for (uint32_t i = 0; i < m_tokens.size(); i++){
			Token &token = m_tokens[i];
			if (token.state != TOKEN_FREE || !QpMatchesToken(token, qp) || !PrepareTokenGrant(token, qp))
				continue;
			token.state = TOKEN_HELD;
			token.qp = qp;
			token.psn = 0;
			NS_LOG_INFO("tor " << m_torId << " holds leaf-spine token "
				<< token.id << " seq " << token.seqNo << " source_tor "
				<< token.sourceTorId << " dst_tor " << token.dstPodId);
			return true;
		}
		return false;
	}

	uint32_t serverId = m_qps[qpi].serverId;
	for (uint32_t i = 0; i < m_tokens.size(); i++){
		Token &token = m_tokens[i];
		if (token.serverId != serverId || token.state != TOKEN_FREE || !PrepareTokenGrant(token, qp))
			continue;
		token.state = TOKEN_HELD;
		token.qp = qp;
		token.psn = 0;
		NS_LOG_INFO("tor " << m_torId << " server " << serverId << " holds token "
			<< token.id << " seq " << token.seqNo);
		return true;
	}
	return false;
}

OppTokenManager::TokenGrant
OppTokenManager::MarkPacketSent(Ptr<RdmaQueuePair> qp, uint32_t psn)
{
	int tokenIdx = FindHeldToken(qp);
	NS_ASSERT_MSG(tokenIdx >= 0, "OPP packet is being built without a held token");

	Token &token = m_tokens[tokenIdx];
	token.state = TOKEN_OCCUPIED;
	token.psn = psn;
	Time timeout = MilliSeconds((uint64_t)qp->m_oppRemainingHopCount + 1);
	token.timeoutEvent = Simulator::Schedule(timeout, &OppTokenManager::TokenTimeout, this, token.id, token.seqNo, psn, qp);
	bool useUsm = token.useUsm;
	CommitTokenGrant(token);
	if (std::getenv("OPP_USM_DECISION_LOG") != NULL){
		std::cout << "OPP_USM_DECISION time_ns " << Simulator::Now().GetTimeStep()
			<< " sip " << qp->sip.Get()
			<< " dip " << qp->dip.Get()
			<< " sport " << qp->sport
			<< " dport " << qp->dport
			<< " pg " << qp->m_pg
			<< " psn " << psn
			<< " token " << token.id
			<< " token_seq " << token.seqNo
			<< " use_usm " << (useUsm ? 1 : 0)
			<< std::endl;
	}
	NS_LOG_INFO("tor " << m_torId << " token " << token.id << " seq " << token.seqNo
		<< " occupied by psn " << psn << " timeout " << timeout.GetTimeStep()
		<< " multicast " << (useUsm ? "USM" : "STANDARD"));
	return TokenGrant(token.id, token.seqNo, useUsm);
}

bool
OppTokenManager::ProbeAckToken(Ptr<RdmaQueuePair> qp, uint32_t ackPsn, uint32_t &tokenId, uint16_t &tokenSeqNo) const
{
	for (uint32_t i = 0; i < m_tokens.size(); i++){
		const Token &token = m_tokens[i];
		if (token.state == TOKEN_OCCUPIED && token.psn == ackPsn && IsSameQp(token.qp, qp)){
			tokenId = token.id;
			tokenSeqNo = token.seqNo;
			return true;
		}
	}
	return false;
}

bool
OppTokenManager::OnOppAck(Ptr<RdmaQueuePair> qp, uint32_t tokenId, uint16_t tokenSeqNo)
{
	bool released = ReleaseOccupiedToken(tokenId, tokenSeqNo, qp, true, "ack");
	if (!released)
		NS_LOG_INFO("tor " << m_torId << " ack token " << tokenId << " seq "
			<< tokenSeqNo << " has no matching occupied token");
	return released;
}

void
OppTokenManager::WakeQp(Ptr<RdmaQueuePair> qp)
{
	int qpi = FindQpInfo(qp);
	if (qpi < 0)
		return;
	if (!m_qps[qpi].wakeupCb.IsNull())
			m_qps[qpi].wakeupCb(qp);
}

void
OppTokenManager::NotifyTokenReleased(Ptr<RdmaQueuePair> qp)
{
	int qpi = FindQpInfo(qp);
	if (qpi < 0)
		return;
	if (!m_qps[qpi].tokenReleaseCb.IsNull())
		m_qps[qpi].tokenReleaseCb(qp);
}

bool
OppTokenManager::AssignTokenToNextQp(uint32_t tokenIdx)
{
	NS_ASSERT_MSG(tokenIdx < m_tokens.size(), "OPP token assignment index out of range");
	Token &token = m_tokens[tokenIdx];
	if (token.state != TOKEN_FREE || m_qps.empty())
		return false;

	uint32_t n = m_qps.size();
	for (uint32_t step = 0; step < n; step++){
		uint32_t idx = (m_assignCursor + step) % n;
		Ptr<RdmaQueuePair> qp = m_qps[idx].qp;
		if (!QpMatchesToken(token, qp))
			continue;
		if (qp->GetBytesLeft() == 0 || HasToken(qp))
			continue;
		if (!InitialInterPodPacingReady(token, qp))
			continue;
		if (!PrepareTokenGrant(token, qp))
			continue;
		token.state = TOKEN_HELD;
		token.qp = qp;
		token.psn = 0;
		m_assignCursor = (idx + 1) % n;
		NS_LOG_INFO("tor " << m_torId << " reassigns token "
			<< token.id << " seq " << token.seqNo);
		WakeQp(qp);
		return true;
	}
	return false;
}

void
OppTokenManager::TokenTimeout(uint32_t tokenId, uint16_t tokenSeqNo, uint32_t psn, Ptr<RdmaQueuePair> qp)
{
	NS_LOG_INFO("tor " << m_torId << " timeout event token " << tokenId
		<< " seq " << tokenSeqNo << " psn " << psn);
	ReleaseOccupiedToken(tokenId, tokenSeqNo, qp, false, "timeout");
}

bool
OppTokenManager::ReleaseOccupiedToken(uint32_t tokenId, uint16_t tokenSeqNo, Ptr<RdmaQueuePair> qp, bool cancelTimeout, const char *reason)
{
	for (uint32_t i = 0; i < m_tokens.size(); i++){
		Token &token = m_tokens[i];
		if (token.id != tokenId || token.seqNo != tokenSeqNo)
			continue;
		if (token.state != TOKEN_OCCUPIED || !IsSameQp(token.qp, qp))
			return false;

		uint32_t psn = token.psn;
		if (cancelTimeout && !token.timeoutEvent.IsExpired())
			Simulator::Cancel(token.timeoutEvent);
		token.state = TOKEN_FREE;
		token.qp = NULL;
		token.psn = 0;
		token.seqNo++;
		NS_LOG_INFO("tor " << m_torId << " token " << tokenId << " seq " << tokenSeqNo
			<< " " << reason << " release psn " << psn << " next_seq " << token.seqNo);

		NotifyTokenReleased(qp);
		if (i < m_tokens.size())
			AssignTokenToNextQp(i);
		return true;
	}
	return false;
}

void
OppTokenManager::UnregisterQueuePair(Ptr<RdmaQueuePair> qp)
{
	uintptr_t sampleKey = GetQpSampleKey(qp);
	ClearInitialInterPodWakeup(sampleKey);
	m_usmSampleQpOrdinals.erase(sampleKey);
	m_usmSampleQpSentCounts.erase(sampleKey);

	for (uint32_t i = 0; i < m_tokens.size(); i++){
		Token &token = m_tokens[i];
		if (!IsSameQp(token.qp, qp))
			continue;
		if (!token.timeoutEvent.IsExpired())
			Simulator::Cancel(token.timeoutEvent);
		if (token.state == TOKEN_HELD){
			token.state = TOKEN_FREE;
			token.psn = 0;
		}else if (token.state == TOKEN_OCCUPIED){
			token.state = TOKEN_FREE;
			token.psn = 0;
			token.seqNo++;
		}
		token.qp = NULL;
	}

	uint32_t next = 0;
	for (uint32_t i = 0; i < m_qps.size(); i++){
		if (IsSameQp(m_qps[i].qp, qp))
			continue;
		if (next != i)
			m_qps[next] = m_qps[i];
		next++;
	}
	m_qps.resize(next);
}

uint32_t
OppTokenManager::GetTorId() const
{
	return m_torId;
}

uint32_t
OppTokenManager::GetTokensPerServer() const
{
	return m_tokensPerServer;
}

} // namespace ns3
