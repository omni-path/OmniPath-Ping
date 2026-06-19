/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
#ifndef OPP_PROBE_HEADER_H
#define OPP_PROBE_HEADER_H

#include "ns3/header.h"
#include "ns3/tag.h"
#include <stdint.h>

namespace ns3 {

class OppProbeHeader : public Header
{
public:
  OppProbeHeader ();

  static TypeId GetTypeId (void);
  virtual TypeId GetInstanceTypeId (void) const;
  virtual void Print (std::ostream &os) const;
  virtual uint32_t GetSerializedSize (void) const;
  virtual void Serialize (Buffer::Iterator start) const;
  virtual uint32_t Deserialize (Buffer::Iterator start);

  static uint32_t GetStaticSize (void);

  void SetTokenIdP (uint32_t value);
  void SetTokenSeqNoP (uint16_t value);
  void SetRemainingHopCountP (uint8_t value);
  void SetPreviousHopQDelayP (uint32_t value);
  void SetPreviousHopLinkP (uint32_t value);

  uint32_t GetTokenIdP (void) const;
  uint16_t GetTokenSeqNoP (void) const;
  uint8_t GetRemainingHopCountP (void) const;
  uint32_t GetPreviousHopQDelayP (void) const;
  uint32_t GetPreviousHopLinkP (void) const;

private:
  uint32_t m_tokenIdP;
  uint16_t m_tokenSeqNoP;
  uint8_t m_remainingHopCountP;
  uint32_t m_previousHopQDelayP;
  uint32_t m_previousHopLinkP;
};

class OppMulticastDecisionTag : public Tag
{
public:
  OppMulticastDecisionTag ();
  explicit OppMulticastDecisionTag (bool useUsm);

  static TypeId GetTypeId (void);
  virtual TypeId GetInstanceTypeId (void) const;
  virtual uint32_t GetSerializedSize (void) const;
  virtual void Serialize (TagBuffer i) const;
  virtual void Deserialize (TagBuffer i);
  virtual void Print (std::ostream &os) const;

  void SetUseUsm (bool value);
  bool GetUseUsm (void) const;

private:
  uint8_t m_useUsm;
};

} // namespace ns3

#endif /* OPP_PROBE_HEADER_H */
