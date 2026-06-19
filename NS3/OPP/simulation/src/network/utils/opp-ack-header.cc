/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
#include "opp-ack-header.h"

namespace ns3 {

NS_OBJECT_ENSURE_REGISTERED (OppAckHeader);

OppAckHeader::OppAckHeader ()
  : m_tokenIdA (0),
    m_tokenSeqNoA (0),
    m_equalCostPathCountA (0),
    m_maximumQDelayA (0),
    m_congestedLinkA (0),
    m_faultyLinkA (0),
    m_isLoopA (0)
{
}

TypeId
OppAckHeader::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::OppAckHeader")
    .SetParent<Header> ()
    .AddConstructor<OppAckHeader> ()
  ;
  return tid;
}

TypeId
OppAckHeader::GetInstanceTypeId (void) const
{
  return GetTypeId ();
}

void
OppAckHeader::Print (std::ostream &os) const
{
  os << "opp_ack:"
     << "token_id=" << m_tokenIdA
     << ",token_seq=" << m_tokenSeqNoA
     << ",ecmp_count=" << m_equalCostPathCountA
     << ",max_qdelay=" << m_maximumQDelayA
     << ",congested_link=" << m_congestedLinkA
     << ",faulty_link=" << m_faultyLinkA
     << ",is_loop=" << (uint32_t)m_isLoopA;
}

uint32_t
OppAckHeader::GetSerializedSize (void) const
{
  return GetStaticSize ();
}

uint32_t
OppAckHeader::GetStaticSize (void)
{
  return sizeof (uint32_t) + sizeof (uint16_t) + sizeof (uint16_t) +
         sizeof (uint32_t) + sizeof (uint32_t) + sizeof (uint32_t) +
         sizeof (uint8_t);
}

void
OppAckHeader::Serialize (Buffer::Iterator start) const
{
  Buffer::Iterator i = start;
  i.WriteHtonU32 (m_tokenIdA);
  i.WriteHtonU16 (m_tokenSeqNoA);
  i.WriteHtonU16 (m_equalCostPathCountA);
  i.WriteHtonU32 (m_maximumQDelayA);
  i.WriteHtonU32 (m_congestedLinkA);
  i.WriteHtonU32 (m_faultyLinkA);
  i.WriteU8 (m_isLoopA);
}

uint32_t
OppAckHeader::Deserialize (Buffer::Iterator start)
{
  Buffer::Iterator i = start;
  m_tokenIdA = i.ReadNtohU32 ();
  m_tokenSeqNoA = i.ReadNtohU16 ();
  m_equalCostPathCountA = i.ReadNtohU16 ();
  m_maximumQDelayA = i.ReadNtohU32 ();
  m_congestedLinkA = i.ReadNtohU32 ();
  m_faultyLinkA = i.ReadNtohU32 ();
  m_isLoopA = i.ReadU8 ();
  return GetSerializedSize ();
}

void
OppAckHeader::SetTokenIdA (uint32_t value)
{
  m_tokenIdA = value;
}

void
OppAckHeader::SetTokenSeqNoA (uint16_t value)
{
  m_tokenSeqNoA = value;
}

void
OppAckHeader::SetEqualCostPathCountA (uint16_t value)
{
  m_equalCostPathCountA = value;
}

void
OppAckHeader::SetMaximumQDelayA (uint32_t value)
{
  m_maximumQDelayA = value;
}

void
OppAckHeader::SetCongestedLinkA (uint32_t value)
{
  m_congestedLinkA = value;
}

void
OppAckHeader::SetFaultyLinkA (uint32_t value)
{
  m_faultyLinkA = value;
}

void
OppAckHeader::SetIsLoopA (uint8_t value)
{
  m_isLoopA = value;
}

uint32_t
OppAckHeader::GetTokenIdA (void) const
{
  return m_tokenIdA;
}

uint16_t
OppAckHeader::GetTokenSeqNoA (void) const
{
  return m_tokenSeqNoA;
}

uint16_t
OppAckHeader::GetEqualCostPathCountA (void) const
{
  return m_equalCostPathCountA;
}

uint32_t
OppAckHeader::GetMaximumQDelayA (void) const
{
  return m_maximumQDelayA;
}

uint32_t
OppAckHeader::GetCongestedLinkA (void) const
{
  return m_congestedLinkA;
}

uint32_t
OppAckHeader::GetFaultyLinkA (void) const
{
  return m_faultyLinkA;
}

uint8_t
OppAckHeader::GetIsLoopA (void) const
{
  return m_isLoopA;
}

} // namespace ns3
