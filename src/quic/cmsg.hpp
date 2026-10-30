#pragma once

#include <ilias/net/cmsg.hpp>
#include <ilias/net/sockopt.hpp>
#include <cstdint>

#if defined(__linux__)
#include <netinet/udp.h>
#endif

// Local additions until these native control types are available in ilias.
namespace quic::cmsg {

using ilias::cmsg::CmsgT;

#if defined(_WIN32)
/// @brief Per-message UDP segment payload size in bytes; send only.
using UdpSendMsgSize = CmsgT<IPPROTO_UDP, UDP_SEND_MSG_SIZE, DWORD>;

/// @brief Original segment payload size in a coalesced receive; receive only.
using UdpCoalescedInfo = CmsgT<IPPROTO_UDP, UDP_COALESCED_INFO, DWORD>;

/// @brief IPv4 ECN codepoint for sending or receiving a datagram.
using Ipv4Ecn = CmsgT<IPPROTO_IP, IP_ECN, int>;

/// @brief IPv6 ECN codepoint for sending or receiving a datagram.
using Ipv6Ecn = CmsgT<IPPROTO_IPV6, IPV6_ECN, int>;
#else
#ifdef UDP_SEGMENT
/// @brief Per-message UDP segment payload size in bytes; send only.
using UdpSegment = CmsgT<IPPROTO_UDP, UDP_SEGMENT, uint16_t>;
#endif

#ifdef UDP_GRO
/// @brief Original segment payload size in a GRO receive; receive only.
using UdpGro = CmsgT<IPPROTO_UDP, UDP_GRO, int>;
#endif

#if defined(__FreeBSD__)
using Ipv4TosSend = CmsgT<IPPROTO_IP, IP_TOS, uint8_t>;
#else
using ilias::cmsg::Ipv4TosSend;
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
using Ipv4TosRecv = CmsgT<IPPROTO_IP, IP_RECVTOS, uint8_t>;
#else
using ilias::cmsg::Ipv4TosRecv;
#endif

using ilias::cmsg::Ipv6TrafficClass;
#endif

} // namespace quic::cmsg

namespace quic::sockopt {

using ilias::sockopt::OptionT;

#if defined(_WIN32)
/// @brief Default UDP segment payload size in bytes; zero disables segmentation.
using UdpSendMsgSize = OptionT<IPPROTO_UDP, UDP_SEND_MSG_SIZE, DWORD>;

/// @brief Maximum coalesced receive size in bytes; zero disables coalescing.
using UdpRecvMaxCoalescedSize = OptionT<IPPROTO_UDP, UDP_RECV_MAX_COALESCED_SIZE, DWORD>;

/// @brief Enable IP_ECN metadata on received datagrams.
using Ipv4RecvEcn = OptionT<IPPROTO_IP, IP_RECVECN, DWORD>;

/// @brief Enable IPV6_ECN metadata on received datagrams.
using Ipv6RecvEcn = OptionT<IPPROTO_IPV6, IPV6_RECVECN, DWORD>;
#else
#ifdef UDP_SEGMENT
/// @brief Default UDP segment payload size; the socket option uses int, unlike its cmsg.
using UdpSegment = OptionT<IPPROTO_UDP, UDP_SEGMENT, int>;
#endif

#ifdef UDP_GRO
/// @brief Enable coalesced UDP receives and UDP_GRO metadata.
using UdpGro = OptionT<IPPROTO_UDP, UDP_GRO, int>;
#endif

/// @brief Enable IPv4 TOS metadata on received datagrams.
using Ipv4RecvTos = OptionT<IPPROTO_IP, IP_RECVTOS, int>;

/// @brief Enable IPv6 traffic-class metadata on received datagrams.
using Ipv6RecvTrafficClass = OptionT<IPPROTO_IPV6, IPV6_RECVTCLASS, int>;
#endif

} // namespace quic::sockopt
