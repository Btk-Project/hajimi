#pragma once

#ifdef HAJIMI_QUIC_TESTING
#include "udp_batch.hpp"

namespace quic {
struct ContextTestAccess {
    static auto setFaultInjector(QuicContext &context, UdpBatchSocket::FaultInjector fault) -> void;
    static auto setNextBindFault(UdpBatchSocket::FaultInjector fault) -> void;
};
} // namespace quic
#endif
