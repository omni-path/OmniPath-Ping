#include "switch-route-provider.h"
#include <ns3/assert.h>

namespace ns3 {

TypeId SwitchRouteProvider::GetTypeId(void)
{
	static TypeId tid = TypeId("ns3::SwitchRouteProvider")
		.SetParent<Object> ()
	;
	return tid;
}

bool SwitchRouteProvider::BypassOppCache() const
{
	return false;
}

bool SwitchRouteProvider::GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId, uint32_t &index) const
{
	(void)ch;
	(void)tokenId;
	(void)index;
	return false;
}

TypeId FatTreeSwitchRouteProvider::GetTypeId(void)
{
	static TypeId tid = TypeId("ns3::FatTreeSwitchRouteProvider")
		.SetParent<SwitchRouteProvider> ()
		.AddConstructor<FatTreeSwitchRouteProvider> ()
	;
	return tid;
}

TypeId LeafSpineSwitchRouteProvider::GetTypeId(void)
{
	static TypeId tid = TypeId("ns3::LeafSpineSwitchRouteProvider")
		.SetParent<SwitchRouteProvider> ()
		.AddConstructor<LeafSpineSwitchRouteProvider> ()
	;
	return tid;
}

FatTreeSwitchRouteProvider::FatTreeSwitchRouteProvider()
	: m_k(0), m_role(0), m_pod(0), m_index(0),
	  m_oppM1(1), m_oppM2(1), m_oppM3(1),
	  m_oppProbeInstanceCount(1)
{
}

void FatTreeSwitchRouteProvider::Configure(uint32_t k, uint32_t role, uint32_t pod, uint32_t index, const std::vector<int> &downPorts, const std::vector<int> &upPorts, uint32_t oppM1, uint32_t oppM2, uint32_t oppM3, uint32_t oppProbeInstanceCount)
{
	m_k = k;
	m_role = role;
	m_pod = pod;
	m_index = index;
	m_downPorts = downPorts;
	m_upPorts = upPorts;
	m_oppM1 = oppM1;
	m_oppM2 = oppM2;
	m_oppM3 = oppM3;
	m_oppProbeInstanceCount = oppProbeInstanceCount;
}

int FatTreeSwitchRouteProvider::GetSinglePort(const std::vector<int> &ports, uint32_t idx)
{
	if (idx >= ports.size())
		return -1;
	return ports[idx];
}

int FatTreeSwitchRouteProvider::GetOutDev(CustomHeader &ch)
{
	uint32_t half = m_k / 2;
	uint32_t hostsPerEdge = half;
	uint32_t hostsPerPod = half * half;
	uint32_t dstHostId = (ch.dip >> 8) & 0xffff;
	uint32_t dstPod = dstHostId / hostsPerPod;
	uint32_t dstEdge = (dstHostId % hostsPerPod) / hostsPerEdge;
	uint32_t dstHost = dstHostId % hostsPerEdge;

	if (m_role == 1){
		if (dstPod == m_pod && dstEdge == m_index)
			return GetSinglePort(m_downPorts, dstHost);
		return -2;
	}else if (m_role == 2){
		if (dstPod == m_pod)
			return GetSinglePort(m_downPorts, dstEdge);
		return -2;
	}else if (m_role == 3){
		return GetSinglePort(m_downPorts, dstPod);
	}
	return -1;
}

bool FatTreeSwitchRouteProvider::BypassOppCache() const
{
	return m_role == 3;
}

uint32_t FatTreeSwitchRouteProvider::GetSourceTorPodBase(uint32_t sourceTorId) const
{
	uint32_t half = m_k / 2;
	return ((sourceTorId - 1) / half) * half + 1;
}

uint32_t FatTreeSwitchRouteProvider::GetFlowDstHostId(CustomHeader &ch) const
{
	uint32_t ip = ch.dip;
	if (ch.l3Prot == 0xFC || ch.l3Prot == 0xFD)
		ip = ch.sip;
	return (ip >> 8) & 0xffff;
}

void FatTreeSwitchRouteProvider::DecodeOppSourceId(uint32_t encodedSourceTorId, uint32_t &instanceId, uint32_t &sourceTorId) const
{
	uint32_t half = m_k / 2;
	uint32_t edgeCount = m_k * half;
	if (m_oppProbeInstanceCount <= 1){
		instanceId = 0;
		sourceTorId = encodedSourceTorId;
		return;
	}
	uint32_t sourceStride = edgeCount + 1;
	instanceId = encodedSourceTorId / sourceStride;
	sourceTorId = encodedSourceTorId % sourceStride;
	NS_ASSERT_MSG(instanceId < m_oppProbeInstanceCount, "fat-tree OPP token instance ID exceeds OPP_PROBE_INSTANCE_COUNT");
	NS_ASSERT_MSG(sourceTorId <= edgeCount, "fat-tree decoded source ToR id exceeds edge switch count");
}

uint32_t FatTreeSwitchRouteProvider::GetOppCacheInstanceStride() const
{
	uint32_t half = m_k / 2;
	uint32_t edgeCount = m_k * half;
	uint64_t stride = 1;
	if (m_role == 1)
		stride = (uint64_t)m_oppM1 + (uint64_t)m_oppM2 * half + (uint64_t)m_oppM3 * (edgeCount + m_k);
	else if (m_role == 2)
		stride = (uint64_t)m_oppM2 * half + (uint64_t)m_oppM3 * (edgeCount + half * m_k);
	NS_ASSERT_MSG(stride > 0 && stride <= 0xffffffffull, "fat-tree OPP cache instance stride exceeds 32 bits");
	return (uint32_t)stride;
}

bool FatTreeSwitchRouteProvider::GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId, uint32_t &index) const
{
	if (m_role == 3)
		return false;

	NS_ASSERT_MSG(m_k > 0 && (m_k % 2) == 0, "fat-tree OPP cache index requires an even k");
	NS_ASSERT_MSG(m_oppM1 > 0 && m_oppM1 <= 256, "OPP_FATTREE_M1 must be in [1, 256]");
	NS_ASSERT_MSG(m_oppM2 > 0 && m_oppM2 <= 256, "OPP_FATTREE_M2 must be in [1, 256]");
	NS_ASSERT_MSG(m_oppM3 > 0 && m_oppM3 <= 256, "OPP_FATTREE_M3 must be in [1, 256]");

	uint32_t half = m_k / 2;
	uint32_t hostsPerEdge = half;
	uint32_t hostsPerPod = half * half;
	uint32_t edgeCount = m_k * half;
	uint32_t encodedX = (tokenId >> 16) & 0xffff;
	uint32_t y = (tokenId >> 8) & 0xff;
	uint32_t z = tokenId & 0xff;
	uint32_t instanceId = 0;
	uint32_t x = 0;
	DecodeOppSourceId(encodedX, instanceId, x);
	uint32_t baseIndex = 0;

	if (m_role == 1){
		if (x == 0){
			NS_ASSERT_MSG(y == 0, "fat-tree ToR-local OPP token must use destination pod id 0");
			NS_ASSERT_MSG(z < m_oppM1, "fat-tree ToR-local OPP token number exceeds OPP_FATTREE_M1");
			baseIndex = z;
			index = instanceId * GetOppCacheInstanceStride() + baseIndex;
			return true;
		}

		NS_ASSERT_MSG(x <= edgeCount, "fat-tree source ToR id exceeds edge switch count");
		uint32_t beta = GetSourceTorPodBase(x);
		if (y == 0){
			NS_ASSERT_MSG(z < m_oppM2, "fat-tree same-pod OPP token number exceeds OPP_FATTREE_M2");
			baseIndex = z + m_oppM1 + m_oppM2 * (x - beta);
			index = instanceId * GetOppCacheInstanceStride() + baseIndex;
			return true;
		}

		NS_ASSERT_MSG(y <= m_k, "fat-tree destination pod id exceeds k");
		NS_ASSERT_MSG(z < m_oppM3, "fat-tree cross-pod OPP token number exceeds OPP_FATTREE_M3");
		uint32_t dstHostId = GetFlowDstHostId(ch);
		uint32_t dstPod = dstHostId / hostsPerPod;
		uint32_t dstEdge = (dstHostId % hostsPerPod) / hostsPerEdge;
		bool isDstTor = m_pod == dstPod && m_index == dstEdge;
		if (isDstTor)
			baseIndex = z + m_oppM1 + m_oppM2 * half + m_oppM3 * (x - 1);
		else
			baseIndex = z + m_oppM1 + m_oppM2 * half + m_oppM3 * (edgeCount + y - 1);
		index = instanceId * GetOppCacheInstanceStride() + baseIndex;
		return true;
	}else if (m_role == 2){
		NS_ASSERT_MSG(x > 0 && x <= edgeCount, "fat-tree agg OPP cache requires a non-local source ToR id");
		uint32_t beta = GetSourceTorPodBase(x);
		if (y == 0){
			NS_ASSERT_MSG(z < m_oppM2, "fat-tree same-pod OPP token number exceeds OPP_FATTREE_M2");
			baseIndex = z + m_oppM2 * (x - beta);
			index = instanceId * GetOppCacheInstanceStride() + baseIndex;
			return true;
		}

		NS_ASSERT_MSG(y <= m_k, "fat-tree destination pod id exceeds k");
		NS_ASSERT_MSG(z < m_oppM3, "fat-tree cross-pod OPP token number exceeds OPP_FATTREE_M3");
		bool isDstAgg = m_pod == y - 1;
		if (isDstAgg)
			baseIndex = z + m_oppM2 * half + m_oppM3 * (x - 1);
		else
			baseIndex = z + m_oppM2 * half + m_oppM3 * (edgeCount + (x - beta) * m_k + y - 1);
		index = instanceId * GetOppCacheInstanceStride() + baseIndex;
		return true;
	}

	return false;
}

LeafSpineSwitchRouteProvider::LeafSpineSwitchRouteProvider()
	: m_k(0), m_role(0), m_index(0),
	  m_oppM1(1), m_oppM2(1),
	  m_oppProbeInstanceCount(1)
{
}

void LeafSpineSwitchRouteProvider::Configure(uint32_t k, uint32_t role, uint32_t index, const std::vector<int> &downPorts, const std::vector<int> &upPorts, uint32_t oppM1, uint32_t oppM2, uint32_t oppProbeInstanceCount)
{
	m_k = k;
	m_role = role;
	m_index = index;
	m_downPorts = downPorts;
	m_upPorts = upPorts;
	m_oppM1 = oppM1;
	m_oppM2 = oppM2;
	m_oppProbeInstanceCount = oppProbeInstanceCount;
}

int LeafSpineSwitchRouteProvider::GetSinglePort(const std::vector<int> &ports, uint32_t idx)
{
	if (idx >= ports.size())
		return -1;
	return ports[idx];
}

int LeafSpineSwitchRouteProvider::GetOutDev(CustomHeader &ch)
{
	uint32_t half = m_k / 2;
	uint32_t dstHostId = (ch.dip >> 8) & 0xffff;
	uint32_t dstLeaf = dstHostId / half;
	uint32_t dstHost = dstHostId % half;

	if (m_role == 1){
		if (dstLeaf == m_index)
			return GetSinglePort(m_downPorts, dstHost);
		return -2;
	}else if (m_role == 2){
		return GetSinglePort(m_downPorts, dstLeaf);
	}
	return -1;
}

bool LeafSpineSwitchRouteProvider::BypassOppCache() const
{
	return m_role == 2;
}

void LeafSpineSwitchRouteProvider::DecodeOppSourceId(uint32_t encodedSourceTorId, uint32_t &instanceId, uint32_t &sourceTorId) const
{
	if (m_oppProbeInstanceCount <= 1){
		instanceId = 0;
		sourceTorId = encodedSourceTorId;
		return;
	}
	uint32_t sourceStride = m_k + 1;
	instanceId = encodedSourceTorId / sourceStride;
	sourceTorId = encodedSourceTorId % sourceStride;
	NS_ASSERT_MSG(instanceId < m_oppProbeInstanceCount, "leaf-spine OPP token instance ID exceeds OPP_PROBE_INSTANCE_COUNT");
	NS_ASSERT_MSG(sourceTorId > 0 && sourceTorId <= m_k, "leaf-spine decoded source ToR ID exceeds k");
}

uint32_t LeafSpineSwitchRouteProvider::GetOppCacheInstanceStride() const
{
	uint64_t stride = (uint64_t)m_oppM1 + 2ull * (uint64_t)m_oppM2 * (uint64_t)m_k;
	NS_ASSERT_MSG(stride > 0 && stride <= 0xffffffffull, "leaf-spine OPP cache instance stride exceeds 32 bits");
	return (uint32_t)stride;
}

bool LeafSpineSwitchRouteProvider::GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId, uint32_t &index) const
{
	(void)ch;
	if (m_role == 2)
		return false;

	NS_ASSERT_MSG(m_k > 0, "leaf-spine OPP cache index requires k");
	NS_ASSERT_MSG(m_oppM1 > 0 && m_oppM1 <= 256, "OPP_LEAFSPINE_M1 must be in [1, 256]");
	NS_ASSERT_MSG(m_oppM2 > 0 && m_oppM2 <= 256, "OPP_LEAFSPINE_M2 must be in [1, 256]");

	uint32_t encodedX = (tokenId >> 16) & 0xffff;
	uint32_t y = (tokenId >> 8) & 0xff;
	uint32_t z = tokenId & 0xff;
	uint32_t instanceId = 0;
	uint32_t x = 0;
	DecodeOppSourceId(encodedX, instanceId, x);
	NS_ASSERT_MSG(x > 0 && x <= m_k, "leaf-spine source ToR ID exceeds k");
	NS_ASSERT_MSG(y <= m_k, "leaf-spine destination ToR ID exceeds k");
	uint32_t baseIndex = 0;

	if (y == 0){
		NS_ASSERT_MSG(z < m_oppM1, "leaf-spine intra-rack OPP token number exceeds OPP_LEAFSPINE_M1");
		baseIndex = z;
		index = instanceId * GetOppCacheInstanceStride() + baseIndex;
		return true;
	}

	NS_ASSERT_MSG(z < m_oppM2, "leaf-spine inter-rack OPP token number exceeds OPP_LEAFSPINE_M2");
	bool isDstTor = m_index == y - 1;
	if (isDstTor)
		baseIndex = z + m_oppM1 + m_oppM2 * (x - 1);
	else
		baseIndex = z + m_oppM1 + m_oppM2 * (m_k + y - 1);
	index = instanceId * GetOppCacheInstanceStride() + baseIndex;
	return true;
}

} /* namespace ns3 */
