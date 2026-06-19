/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2008 University of Washington
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "ns3/assert.h"
#include "ns3/log.h"
#include "ns3/ptr.h"
#include "ns3/node.h"
#include "ns3/net-device.h"
#include "ns3/ipv4.h"
#include "ns3/ipv4-address-generator.h"
#include "ns3/simulator.h"
#include "ipv4-address-helper.h"
#include <sys/time.h>

NS_LOG_COMPONENT_DEFINE ("Ipv4AddressHelper");

namespace ns3 {

namespace {
bool g_ipv4AssignDebug = false;
Ipv4AddressHelper::AssignDebugStats g_ipv4AssignDebugStats;

uint64_t
Ipv4AssignDebugNowUs ()
{
  struct timeval tv;
  gettimeofday (&tv, 0);
  return static_cast<uint64_t> (tv.tv_sec) * 1000000ull + static_cast<uint64_t> (tv.tv_usec);
}

void
Ipv4AssignDebugAdd (uint64_t &field, uint64_t start)
{
  if (g_ipv4AssignDebug)
    {
      field += Ipv4AssignDebugNowUs () - start;
    }
}
}

Ipv4AddressHelper::AssignDebugStats::AssignDebugStats ()
{
  Reset ();
}

void
Ipv4AddressHelper::AssignDebugStats::Reset ()
{
  containers = 0;
  devices = 0;
  createdInterfaces = 0;
  containerGetUs = 0;
  getNodeUs = 0;
  getIpv4Us = 0;
  getInterfaceForDeviceUs = 0;
  addInterfaceUs = 0;
  newAddressUs = 0;
  addAddressUs = 0;
  setMetricUpUs = 0;
  retvalAddUs = 0;
}

void
Ipv4AddressHelper::SetAssignDebug (bool enabled)
{
  g_ipv4AssignDebug = enabled;
}

void
Ipv4AddressHelper::ResetAssignDebugStats ()
{
  g_ipv4AssignDebugStats.Reset ();
}

Ipv4AddressHelper::AssignDebugStats
Ipv4AddressHelper::GetAndResetAssignDebugStats ()
{
  AssignDebugStats stats = g_ipv4AssignDebugStats;
  g_ipv4AssignDebugStats.Reset ();
  return stats;
}

Ipv4AddressHelper::Ipv4AddressHelper () 
{
  NS_LOG_FUNCTION_NOARGS ();

//
// Set the default values to an illegal state.  Do this so the client is 
// forced to think at least briefly about what addresses get used and what
// is going on here.
//
  m_network = 0xffffffff;
  m_mask = 0;
  m_address = 0xffffffff;
  m_base = 0xffffffff;
  m_shift = 0xffffffff;
  m_max = 0xffffffff;
}

Ipv4AddressHelper::Ipv4AddressHelper (
  const Ipv4Address network, 
  const Ipv4Mask    mask,
  const Ipv4Address address)
{
  NS_LOG_FUNCTION_NOARGS ();
  SetBase (network, mask, address);
}

void
Ipv4AddressHelper::SetBase (
  const Ipv4Address network, 
  const Ipv4Mask mask,
  const Ipv4Address address)
{
  NS_LOG_FUNCTION_NOARGS ();

  m_network = network.Get ();
  m_mask = mask.Get ();
  m_base = m_address = address.Get ();

//
// Some quick reasonableness testing.
//
  NS_ASSERT_MSG ((m_network & ~m_mask) == 0,
                 "Ipv4AddressHelper::SetBase(): Inconsistent network and mask");

//
// Figure out how much to shift network numbers to get them aligned, and what
// the maximum allowed address is with respect to the current mask.
//
  m_shift = NumAddressBits (m_mask);
  m_max = (1 << m_shift) - 2;

  NS_ASSERT_MSG (m_shift <= 32,
                 "Ipv4AddressHelper::SetBase(): Unreasonable address length");

//
// Shift the network down into the normalized position.
//
  m_network >>= m_shift;

  NS_LOG_LOGIC ("m_network == " << m_network);
  NS_LOG_LOGIC ("m_mask == " << m_mask);
  NS_LOG_LOGIC ("m_address == " << m_address);
}

Ipv4Address
Ipv4AddressHelper::NewAddress (void)
{
//
// The way this is expected to be used is that an address and network number
// are initialized, and then NewAddress() is called repeatedly to allocate and
// get new addresses on a given subnet.  The client will expect that the first
// address she gets back is the one she used to initialize the generator with.
// This implies that this operation is a post-increment.
//
  NS_ASSERT_MSG (m_address <= m_max,
                 "Ipv4AddressHelper::NewAddress(): Address overflow");

  Ipv4Address addr ((m_network << m_shift) | m_address);
  ++m_address;
//
// The Ipv4AddressGenerator allows us to keep track of the addresses we have
// allocated and will assert if we accidentally generate a duplicate.  This
// avoids some really hard to debug problems.
//
  Ipv4AddressGenerator::AddAllocated (addr);
  return addr;
}

Ipv4Address
Ipv4AddressHelper::NewNetwork (void)
{
  NS_LOG_FUNCTION_NOARGS ();
  ++m_network;
  m_address = m_base;
  return Ipv4Address (m_network << m_shift);
}

Ipv4InterfaceContainer
Ipv4AddressHelper::Assign (const NetDeviceContainer &c)
{
  NS_LOG_FUNCTION_NOARGS ();
  Ipv4InterfaceContainer retval;
  if (g_ipv4AssignDebug)
    {
      g_ipv4AssignDebugStats.containers++;
    }
  for (uint32_t i = 0; i < c.GetN (); ++i) {
      if (g_ipv4AssignDebug)
        {
          g_ipv4AssignDebugStats.devices++;
        }
      uint64_t t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      Ptr<NetDevice> device = c.Get (i);
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.containerGetUs, t);

      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      Ptr<Node> node = device->GetNode ();
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.getNodeUs, t);
      NS_ASSERT_MSG (node, "Ipv4AddressHelper::Assign(): NetDevice is not not associated "
                     "with any node -> fail");

      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      Ptr<Ipv4> ipv4 = node->GetObject<Ipv4> ();
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.getIpv4Us, t);
      NS_ASSERT_MSG (ipv4, "Ipv4AddressHelper::Assign(): NetDevice is associated"
                     " with a node without IPv4 stack installed -> fail "
                     "(maybe need to use InternetStackHelper?)");

      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      int32_t interface = ipv4->GetInterfaceForDevice (device);
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.getInterfaceForDeviceUs, t);
      if (interface == -1)
        {
          t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
          interface = ipv4->AddInterface (device);
          Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.addInterfaceUs, t);
          if (g_ipv4AssignDebug)
            {
              g_ipv4AssignDebugStats.createdInterfaces++;
            }
        }
      NS_ASSERT_MSG (interface >= 0, "Ipv4AddressHelper::Assign(): "
                     "Interface index not found");

      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      Ipv4Address nextAddress = NewAddress ();
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.newAddressUs, t);

      Ipv4InterfaceAddress ipv4Addr = Ipv4InterfaceAddress (nextAddress, m_mask);
      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      ipv4->AddAddress (interface, ipv4Addr);
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.addAddressUs, t);

      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      ipv4->SetMetric (interface, 1);
      ipv4->SetUp (interface);
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.setMetricUpUs, t);

      t = g_ipv4AssignDebug ? Ipv4AssignDebugNowUs () : 0;
      retval.Add (ipv4, interface);
      Ipv4AssignDebugAdd (g_ipv4AssignDebugStats.retvalAddUs, t);
    }
  return retval;
}

const uint32_t N_BITS = 32;

uint32_t
Ipv4AddressHelper::NumAddressBits (uint32_t maskbits) const
{
  NS_LOG_FUNCTION_NOARGS ();
  for (uint32_t i = 0; i < N_BITS; ++i)
    {
      if (maskbits & 1)
        {
          NS_LOG_LOGIC ("NumAddressBits -> " << i);
          return i;
        }
      maskbits >>= 1;
    }

  NS_ASSERT_MSG (false, "Ipv4AddressHelper::NumAddressBits(): Bad Mask");
  return 0;
}

} // namespace ns3
