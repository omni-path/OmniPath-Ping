/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
#ifndef OPP_ACK_HEADER_H
#define OPP_ACK_HEADER_H

#include "ns3/header.h"
#include <stdint.h>

namespace ns3 {

class OppAckHeader : public Header
{
public:
  OppAckHeader ();

  static TypeId GetTypeId (void);
  virtual TypeId GetInstanceTypeId (void) const;
  virtual void Print (std::ostream &os) const;
  virtual uint32_t GetSerializedSize (void) const;
  virtual void Serialize (Buffer::Iterator start) const;
  virtual uint32_t Deserialize (Buffer::Iterator start);

  static uint32_t GetStaticSize (void);

  void SetTokenIdA (uint32_t value);
  void SetTokenSeqNoA (uint16_t value);
  void SetEqualCostPathCountA (uint16_t value);
  void SetMaximumQDelayA (uint32_t value);
  void SetCongestedLinkA (uint32_t value);
  void SetFaultyLinkA (uint32_t value);
  void SetIsLoopA (uint8_t value);

  uint32_t GetTokenIdA (void) const;
  uint16_t GetTokenSeqNoA (void) const;
  uint16_t GetEqualCostPathCountA (void) const;
  uint32_t GetMaximumQDelayA (void) const;
  uint32_t GetCongestedLinkA (void) const;
  uint32_t GetFaultyLinkA (void) const;
  uint8_t GetIsLoopA (void) const;

private:
  uint32_t m_tokenIdA;
  uint16_t m_tokenSeqNoA;
  uint16_t m_equalCostPathCountA;
  uint32_t m_maximumQDelayA;
  uint32_t m_congestedLinkA;
  uint32_t m_faultyLinkA;
  uint8_t m_isLoopA;
};

} // namespace ns3

#endif /* OPP_ACK_HEADER_H */
