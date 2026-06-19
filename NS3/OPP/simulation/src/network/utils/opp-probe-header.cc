/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
#include "opp-probe-header.h"

namespace ns3 {

NS_OBJECT_ENSURE_REGISTERED (OppProbeHeader);
NS_OBJECT_ENSURE_REGISTERED (OppMulticastDecisionTag);

OppProbeHeader::OppProbeHeader ()
  : m_tokenIdP (0),
    m_tokenSeqNoP (0),
    m_remainingHopCountP (0),
    m_previousHopQDelayP (0),
    m_previousHopLinkP (0)
{
}

TypeId
OppProbeHeader::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::OppProbeHeader")
    .SetParent<Header> ()
    .AddConstructor<OppProbeHeader> ()
  ;
  return tid;
}

TypeId
OppProbeHeader::GetInstanceTypeId (void) const
{
  return GetTypeId ();
}

void
OppProbeHeader::Print (std::ostream &os) const
{
  os << "opp_probe:"
     << "token_id=" << m_tokenIdP
     << ",token_seq=" << m_tokenSeqNoP
     << ",remaining_hop=" << (uint32_t)m_remainingHopCountP
     << ",prev_qdelay=" << m_previousHopQDelayP
     << ",prev_link=" << m_previousHopLinkP;
}

uint32_t
OppProbeHeader::GetSerializedSize (void) const
{
  return GetStaticSize ();
}

uint32_t
OppProbeHeader::GetStaticSize (void)
{
  return sizeof (uint32_t) + sizeof (uint16_t) + sizeof (uint8_t) +
         sizeof (uint32_t) + sizeof (uint32_t);
}

void
OppProbeHeader::Serialize (Buffer::Iterator start) const
{
  Buffer::Iterator i = start;
  i.WriteHtonU32 (m_tokenIdP);
  i.WriteHtonU16 (m_tokenSeqNoP);
  i.WriteU8 (m_remainingHopCountP);
  i.WriteHtonU32 (m_previousHopQDelayP);
  i.WriteHtonU32 (m_previousHopLinkP);
}

uint32_t
OppProbeHeader::Deserialize (Buffer::Iterator start)
{
  Buffer::Iterator i = start;
  m_tokenIdP = i.ReadNtohU32 ();
  m_tokenSeqNoP = i.ReadNtohU16 ();
  m_remainingHopCountP = i.ReadU8 ();
  m_previousHopQDelayP = i.ReadNtohU32 ();
  m_previousHopLinkP = i.ReadNtohU32 ();
  return GetSerializedSize ();
}

void
OppProbeHeader::SetTokenIdP (uint32_t value)
{
  m_tokenIdP = value;
}

void
OppProbeHeader::SetTokenSeqNoP (uint16_t value)
{
  m_tokenSeqNoP = value;
}

void
OppProbeHeader::SetRemainingHopCountP (uint8_t value)
{
  m_remainingHopCountP = value;
}

void
OppProbeHeader::SetPreviousHopQDelayP (uint32_t value)
{
  m_previousHopQDelayP = value;
}

void
OppProbeHeader::SetPreviousHopLinkP (uint32_t value)
{
  m_previousHopLinkP = value;
}

uint32_t
OppProbeHeader::GetTokenIdP (void) const
{
  return m_tokenIdP;
}

uint16_t
OppProbeHeader::GetTokenSeqNoP (void) const
{
  return m_tokenSeqNoP;
}

uint8_t
OppProbeHeader::GetRemainingHopCountP (void) const
{
  return m_remainingHopCountP;
}

uint32_t
OppProbeHeader::GetPreviousHopQDelayP (void) const
{
  return m_previousHopQDelayP;
}

uint32_t
OppProbeHeader::GetPreviousHopLinkP (void) const
{
  return m_previousHopLinkP;
}

OppMulticastDecisionTag::OppMulticastDecisionTag ()
  : m_useUsm (0)
{
}

OppMulticastDecisionTag::OppMulticastDecisionTag (bool useUsm)
  : m_useUsm (useUsm ? 1 : 0)
{
}

TypeId
OppMulticastDecisionTag::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::OppMulticastDecisionTag")
    .SetParent<Tag> ()
    .AddConstructor<OppMulticastDecisionTag> ()
  ;
  return tid;
}

TypeId
OppMulticastDecisionTag::GetInstanceTypeId (void) const
{
  return GetTypeId ();
}

uint32_t
OppMulticastDecisionTag::GetSerializedSize (void) const
{
  return sizeof (m_useUsm);
}

void
OppMulticastDecisionTag::Serialize (TagBuffer i) const
{
  i.WriteU8 (m_useUsm);
}

void
OppMulticastDecisionTag::Deserialize (TagBuffer i)
{
  m_useUsm = i.ReadU8 ();
}

void
OppMulticastDecisionTag::Print (std::ostream &os) const
{
  os << "use_usm=" << (uint32_t)m_useUsm;
}

void
OppMulticastDecisionTag::SetUseUsm (bool value)
{
  m_useUsm = value ? 1 : 0;
}

bool
OppMulticastDecisionTag::GetUseUsm (void) const
{
  return m_useUsm != 0;
}

} // namespace ns3
