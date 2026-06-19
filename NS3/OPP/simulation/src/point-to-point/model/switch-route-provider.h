#ifndef SWITCH_ROUTE_PROVIDER_H
#define SWITCH_ROUTE_PROVIDER_H

#include <vector>
#include <ns3/object.h>
#include <ns3/custom-header.h>

namespace ns3 {

class SwitchRouteProvider : public Object {
public:
	static TypeId GetTypeId(void);
	virtual int GetOutDev(CustomHeader &ch) = 0;
	virtual bool BypassOppCache() const;
	virtual bool GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId, uint32_t &index) const;
};

class FatTreeSwitchRouteProvider : public SwitchRouteProvider {
public:
	static TypeId GetTypeId(void);
	FatTreeSwitchRouteProvider();

	void Configure(uint32_t k, uint32_t role, uint32_t pod, uint32_t index, const std::vector<int> &downPorts, const std::vector<int> &upPorts, uint32_t oppM1 = 1, uint32_t oppM2 = 1, uint32_t oppM3 = 1, uint32_t oppProbeInstanceCount = 1);
	int GetOutDev(CustomHeader &ch);
	bool BypassOppCache() const;
	bool GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId, uint32_t &index) const;

private:
	int GetSinglePort(const std::vector<int> &ports, uint32_t idx);
	uint32_t GetSourceTorPodBase(uint32_t sourceTorId) const;
	uint32_t GetFlowDstHostId(CustomHeader &ch) const;
	void DecodeOppSourceId(uint32_t encodedSourceTorId, uint32_t &instanceId, uint32_t &sourceTorId) const;
	uint32_t GetOppCacheInstanceStride() const;

	uint32_t m_k, m_role, m_pod, m_index;
	uint32_t m_oppM1, m_oppM2, m_oppM3;
	uint32_t m_oppProbeInstanceCount;
	std::vector<int> m_downPorts, m_upPorts;
};

class LeafSpineSwitchRouteProvider : public SwitchRouteProvider {
public:
	static TypeId GetTypeId(void);
	LeafSpineSwitchRouteProvider();

	void Configure(uint32_t k, uint32_t role, uint32_t index, const std::vector<int> &downPorts, const std::vector<int> &upPorts, uint32_t oppM1 = 1, uint32_t oppM2 = 1, uint32_t oppProbeInstanceCount = 1);
	int GetOutDev(CustomHeader &ch);
	bool BypassOppCache() const;
	bool GetOppCacheIndex(CustomHeader &ch, uint32_t tokenId, uint32_t &index) const;

private:
	int GetSinglePort(const std::vector<int> &ports, uint32_t idx);
	void DecodeOppSourceId(uint32_t encodedSourceTorId, uint32_t &instanceId, uint32_t &sourceTorId) const;
	uint32_t GetOppCacheInstanceStride() const;

	uint32_t m_k, m_role, m_index;
	uint32_t m_oppM1, m_oppM2;
	uint32_t m_oppProbeInstanceCount;
	std::vector<int> m_downPorts, m_upPorts;
};

} /* namespace ns3 */

#endif /* SWITCH_ROUTE_PROVIDER_H */
