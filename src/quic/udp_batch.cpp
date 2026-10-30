#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "udp_batch.hpp"
#include <algorithm>
#include <cerrno>
#include <cstring>

#if defined(__linux__)
#include <netinet/udp.h>
#include <sys/socket.h>
#endif

using ilias::Err;
using ilias::IoResult;
using ilias::IoTask;

namespace quic {
namespace {

auto unsupported(std::error_code error) -> bool {
    return error == ilias::SystemError::InvalidArgument ||
        error == ilias::SystemError::OperationNotSupported ||
        error == ilias::SystemError::ProtocolOptionNotSupported ||
        error == ilias::SystemError::MessageTooLarge
#if defined(_WIN32)
        || error == ilias::SystemError(ERROR_INVALID_PARAMETER)
        || error == ilias::SystemError(ERROR_NOT_SUPPORTED)
#endif
        ;
}

auto setEcn(ilias::MsgHdr &msg, std::span<std::byte> control, bool ipv6, unsigned int ecn) -> void {
#if defined(_WIN32)
    msg.Control.buf = reinterpret_cast<char *>(control.data());
    msg.Control.len = WSA_CMSG_SPACE(sizeof(int));
    auto cmsg = WSA_CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = ipv6 ? IPPROTO_IPV6 : IPPROTO_IP;
    cmsg->cmsg_type = ipv6 ? IPV6_ECN : IP_ECN;
    cmsg->cmsg_len = WSA_CMSG_LEN(sizeof(int));
    int marking = static_cast<int>(ecn);
    std::memcpy(WSA_CMSG_DATA(cmsg), &marking, sizeof(marking));
#else
    msg.msg_control = control.data();
    msg.msg_controllen = CMSG_SPACE(sizeof(int));
    auto cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = ipv6 ? IPPROTO_IPV6 : IPPROTO_IP;
    cmsg->cmsg_type = ipv6 ? IPV6_TCLASS : IP_TOS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    int marking = static_cast<int>(ecn);
    std::memcpy(CMSG_DATA(cmsg), &marking, sizeof(marking));
#endif
}

// A coalesced receive must retain the original UDP datagram boundaries.
auto receiveInfo(ilias::MutableMsgHdr &msg, size_t size, size_t &segment, unsigned int &ecn) -> bool {
    segment = 0;
    ecn = 0;
    if (msg.flags() & (MSG_TRUNC | MSG_CTRUNC)) {
        return false;
    }
#if defined(_WIN32)
    for (auto cmsg = WSA_CMSG_FIRSTHDR(&msg); cmsg; cmsg = WSA_CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level == IPPROTO_UDP && cmsg->cmsg_type == UDP_COALESCED_INFO) {
            if (cmsg->cmsg_len < WSA_CMSG_LEN(sizeof(DWORD))) return false;
            DWORD value;
            std::memcpy(&value, WSA_CMSG_DATA(cmsg), sizeof(value));
            if (!value || value > 65535 || !size) return false;
            segment = value;
        }
        else if ((cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_ECN) ||
            (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_ECN)) {
            if (cmsg->cmsg_len < WSA_CMSG_LEN(sizeof(int))) return false;
            int value;
            std::memcpy(&value, WSA_CMSG_DATA(cmsg), sizeof(value));
            ecn = value & 3;
        }
    }
#else
    for (auto cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
#ifdef UDP_GRO
        if (cmsg->cmsg_level == IPPROTO_UDP && cmsg->cmsg_type == UDP_GRO) {
            if (cmsg->cmsg_len < CMSG_LEN(sizeof(int))) return false;
            int value;
            std::memcpy(&value, CMSG_DATA(cmsg), sizeof(value));
            if (value <= 0 || value > 65535 || !size) return false;
            segment = value;
        }
        else
#endif
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_TOS) {
            if (cmsg->cmsg_len < CMSG_LEN(sizeof(uint8_t))) return false;
            ecn = *reinterpret_cast<uint8_t *>(CMSG_DATA(cmsg)) & 3;
        }
        else if (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_TCLASS) {
            if (cmsg->cmsg_len < CMSG_LEN(sizeof(int))) return false;
            int value;
            std::memcpy(&value, CMSG_DATA(cmsg), sizeof(value));
            ecn = value & 3;
        }
    }
#endif
    return true;
}

} // namespace

auto UdpBatchSocket::from(ilias::Socket socket, SkBufferPool &pool, bool batching) -> IoResult<UdpBatchSocket> {
    UdpBatchSocket result;
    result.mBatching = batching;
    ILIAS_TRY(result.mHandle, ilias::IoHandle<ilias::Socket>::make(std::move(socket), ilias::IoDescriptor::Socket));
    auto &fd = result.mHandle.fd();
    ILIAS_TRY(auto local, fd.localEndpoint<ilias::IPEndpoint>());
    auto family = local.family();
    if (batching) {
#if defined(_WIN32)
        DWORD size = 65527;
        result.mStats.receiveCoalescing = bool(fd.setOption(IPPROTO_UDP, UDP_RECV_MAX_COALESCED_SIZE, &size, sizeof(size)));
        DWORD segment = 0;
        socklen_t length = sizeof(segment);
        result.mStats.sendSegmentation = bool(fd.getOption(IPPROTO_UDP, UDP_SEND_MSG_SIZE, &segment, &length));
#elif defined(UDP_SEGMENT)
        int enabled = 1;
        result.mStats.sendSegmentation = true; // Validate on the first real segmented submission.
#ifdef UDP_GRO
        result.mStats.receiveCoalescing = bool(fd.setOption(IPPROTO_UDP, UDP_GRO, &enabled, sizeof(enabled)));
#endif
#endif
    }
    int enabled = 1;
#if defined(_WIN32)
    auto _ = family == AF_INET6 ? fd.setOption(IPPROTO_IPV6, IPV6_RECVECN, &enabled, sizeof(enabled)) :
        fd.setOption(IPPROTO_IP, IP_RECVECN, &enabled, sizeof(enabled));
#else
    auto _ = family == AF_INET6 ? fd.setOption(IPPROTO_IPV6, IPV6_RECVTCLASS, &enabled, sizeof(enabled)) :
        fd.setOption(IPPROTO_IP, IP_RECVTOS, &enabled, sizeof(enabled));
#endif
    result.mReceive.resize(
#if defined(__linux__)
        batching ? receiveSlots :
#endif
        1);
    for (auto &slot : result.mReceive) {
        slot.bytes = pool.allocate(receiveCapacity);
    }
    return result;
}

auto UdpBatchSocket::send(ilias::Buffer data, const ilias::IPEndpoint &remote,
    size_t segmentSize, unsigned int ecn) -> ilias::Task<Sent> {
    if (data.empty() || !segmentSize || segmentSize > data.size() ||
        data.size() > sendCapacity || (data.size() + segmentSize - 1) / segmentSize > maxSegments) {
        co_return Sent{0, std::make_error_code(std::errc::invalid_argument)};
    }
    auto count = (data.size() + segmentSize - 1) / segmentSize;
    auto ipv6 = remote.family() == AF_INET6 && !remote.address6().isV4Mapped();
    if (count > 1 && mStats.sendSegmentation) {
        ilias::IoVec vec{data};
        ilias::MsgHdr msg;
        msg.setEndpoint(remote);
        msg.setBuffers(std::span{&vec, 1});
#if defined(_WIN32)
        alignas(WSACMSGHDR) std::array<std::byte, WSA_CMSG_SPACE(sizeof(DWORD)) + WSA_CMSG_SPACE(sizeof(int))> control{};
        msg.Control.buf = reinterpret_cast<char *>(control.data());
        msg.Control.len = static_cast<ULONG>(control.size());
        auto cmsg = WSA_CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = IPPROTO_UDP;
        cmsg->cmsg_type = UDP_SEND_MSG_SIZE;
        cmsg->cmsg_len = WSA_CMSG_LEN(sizeof(DWORD));
        DWORD size = static_cast<DWORD>(segmentSize);
        std::memcpy(WSA_CMSG_DATA(cmsg), &size, sizeof(size));
        cmsg = WSA_CMSG_NXTHDR(&msg, cmsg);
        cmsg->cmsg_level = ipv6 ? IPPROTO_IPV6 : IPPROTO_IP;
        cmsg->cmsg_type = ipv6 ? IPV6_ECN : IP_ECN;
        cmsg->cmsg_len = WSA_CMSG_LEN(sizeof(int));
        int marking = static_cast<int>(ecn);
        std::memcpy(WSA_CMSG_DATA(cmsg), &marking, sizeof(marking));
#elif defined(UDP_SEGMENT)
        alignas(cmsghdr) std::array<std::byte, CMSG_SPACE(sizeof(uint16_t)) + CMSG_SPACE(sizeof(int))> control{};
        msg.msg_control = control.data();
        msg.msg_controllen = control.size();
        auto cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = IPPROTO_UDP;
        cmsg->cmsg_type = UDP_SEGMENT;
        cmsg->cmsg_len = CMSG_LEN(sizeof(uint16_t));
        auto size = static_cast<uint16_t>(segmentSize);
        std::memcpy(CMSG_DATA(cmsg), &size, sizeof(size));
        cmsg = CMSG_NXTHDR(&msg, cmsg);
        cmsg->cmsg_level = ipv6 ? IPPROTO_IPV6 : IPPROTO_IP;
        cmsg->cmsg_type = ipv6 ? IPV6_TCLASS : IP_TOS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int));
        int marking = static_cast<int>(ecn);
        std::memcpy(CMSG_DATA(cmsg), &marking, sizeof(marking));
#endif
        ++mStats.txSubmissions;
        auto sent = co_await mHandle.sendmsg(msg, 0);
        if (sent) {
            if (*sent != data.size()) co_return Sent{0, std::make_error_code(std::errc::io_error)};
            mStats.txDatagrams += count;
            mStats.txBytes += data.size();
            mStats.txSegmentedDatagrams += count;
            mStats.maxTxBatch = std::max(mStats.maxTxBatch, count);
            co_return Sent{count};
        }
        if (!unsupported(sent.error())) co_return Sent{0, sent.error()};
        // The failed operation has completed; only now is splitting safe.
        mStats.sendSegmentation = false;
        ++mStats.segmentationFallbacks;
    }
    size_t completed = 0;
#if defined(__linux__)
    if (mBatching) {
        std::array<mmsghdr, maxSegments> messages{};
        std::array<iovec, maxSegments> vectors{};
        alignas(cmsghdr) std::array<std::array<std::byte, CMSG_SPACE(sizeof(int))>, maxSegments> controls{};
        for (size_t i = 0; i < count; ++i) {
            auto bytes = data.subspan(i * segmentSize, std::min(segmentSize, data.size() - i * segmentSize));
            vectors[i] = {const_cast<std::byte *>(bytes.data()), bytes.size()};
            messages[i].msg_hdr.msg_name = const_cast<sockaddr *>(static_cast<const sockaddr *>(remote.data()));
            messages[i].msg_hdr.msg_namelen = remote.length();
            messages[i].msg_hdr.msg_iov = &vectors[i];
            messages[i].msg_hdr.msg_iovlen = 1;
            if (ecn) {
                ilias::MsgHdr msg{messages[i].msg_hdr};
                setEcn(msg, controls[i], ipv6, ecn);
                messages[i].msg_hdr = msg;
            }
        }
        while (completed < count) {
            co_await ilias::this_coro::stopPoint();
            ++mStats.txSubmissions;
            auto n = ::sendmmsg(mHandle.fd().get(), messages.data() + completed,
                static_cast<unsigned int>(count - completed), MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n > 0) {
                for (int i = 0; i < n; ++i) mStats.txBytes += messages[completed + i].msg_len;
                completed += n;
                mStats.txDatagrams += n;
                mStats.maxTxBatch = std::max(mStats.maxTxBatch, static_cast<size_t>(n));
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                auto ready = co_await mHandle.poll(ilias::PollEvent::Out);
                if (!ready) co_return Sent{completed, ready.error()};
                continue;
            }
            co_return Sent{completed, n < 0 ? std::error_code(ilias::SystemError::fromErrno()) :
                std::make_error_code(std::errc::io_error)};
        }
        co_return Sent{completed};
    }
#endif
    while (completed < count) {
        auto bytes = data.subspan(completed * segmentSize, std::min(segmentSize, data.size() - completed * segmentSize));
        ++mStats.txSubmissions;
        IoResult<size_t> sent{0};
        if (ecn) {
            ilias::IoVec vec{bytes};
            ilias::MsgHdr msg;
            msg.setEndpoint(remote);
            msg.setBuffers(std::span{&vec, 1});
            alignas(size_t) std::array<std::byte, 64> control{};
            setEcn(msg, control, ipv6, ecn);
            sent = co_await mHandle.sendmsg(msg, 0);
        }
        else {
            sent = co_await mHandle.sendto(bytes, 0, &remote);
        }
        if (!sent) co_return Sent{completed, sent.error()};
        if (*sent != bytes.size()) co_return Sent{completed, std::make_error_code(std::errc::io_error)};
        ++completed;
        ++mStats.txDatagrams;
        mStats.txBytes += bytes.size();
        mStats.maxTxBatch = std::max<size_t>(mStats.maxTxBatch, 1);
    }
    co_return Sent{completed};
}

auto UdpBatchSocket::receive() -> IoTask<std::span<const Received>> {
    while (true) {
        co_await ilias::this_coro::stopPoint();
        size_t received = 0;
#if defined(__linux__)
        if (mBatching) {
            std::array<mmsghdr, receiveSlots> messages{};
            std::array<iovec, receiveSlots> vectors{};
            for (size_t i = 0; i < mReceive.size(); ++i) {
                auto &slot = mReceive[i];
                slot.bytes.clear();
                auto buffer = slot.bytes.prepareBack(receiveCapacity);
                vectors[i] = {buffer.data(), buffer.size()};
                auto &msg = messages[i].msg_hdr;
                msg.msg_name = slot.remote.data();
                msg.msg_namelen = slot.remote.bufsize();
                msg.msg_iov = &vectors[i];
                msg.msg_iovlen = 1;
                msg.msg_control = slot.control.data();
                msg.msg_controllen = slot.control.size();
            }
            ++mStats.rxSubmissions;
            auto n = ::recvmmsg(mHandle.fd().get(), messages.data(), static_cast<unsigned int>(mReceive.size()), MSG_DONTWAIT, nullptr);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                ILIAS_CO_TRYV(co_await mHandle.poll(ilias::PollEvent::In));
                continue;
            }
            if (n < 0) co_return Err(ilias::SystemError::fromErrno());
            for (int i = 0; i < n; ++i) {
                ilias::MutableMsgHdr msg{messages[i].msg_hdr};
                size_t segment;
                unsigned int ecn;
                if (!receiveInfo(msg, messages[i].msg_len, segment, ecn)) { ++mStats.droppedReceives; continue; }
                auto &slot = mReceive[i];
                slot.bytes.commitBack(messages[i].msg_len);
                mReceived[received++] = {ilias::makeBuffer(slot.bytes.data()), slot.remote, segment, ecn};
            }
        }
        else
#endif
        {
            auto &slot = mReceive.front();
            slot.bytes.clear();
            ilias::MutableIoVec vec{ilias::makeBuffer(slot.bytes.prepareBack(receiveCapacity))};
            ilias::MutableMsgHdr msg;
            msg.setEndpoint(slot.remote);
            msg.setBuffers(std::span{&vec, 1});
#if defined(_WIN32)
            msg.Control.buf = reinterpret_cast<char *>(slot.control.data());
            msg.Control.len = static_cast<ULONG>(slot.control.size());
#else
            msg.msg_control = slot.control.data();
            msg.msg_controllen = slot.control.size();
#endif
            ++mStats.rxSubmissions;
            auto result = co_await mHandle.recvmsg(msg, 0);
            if (!result) {
                if (result.error() == ilias::SystemError::MessageTooLarge) { ++mStats.droppedReceives; continue; }
                co_return Err(result.error());
            }
            size_t segment;
            unsigned int ecn;
            if (!receiveInfo(msg, *result, segment, ecn)) { ++mStats.droppedReceives; continue; }
            slot.bytes.commitBack(*result);
            mReceived[received++] = {ilias::makeBuffer(slot.bytes.data()), slot.remote, segment, ecn};
        }
        size_t datagrams = 0;
        for (size_t i = 0; i < received; ++i) {
            auto &msg = mReceived[i];
            mStats.rxBytes += msg.bytes.size();
            auto count = msg.segmentSize ? (msg.bytes.size() + msg.segmentSize - 1) / msg.segmentSize : 1;
            datagrams += count;
            if (count > 1) mStats.rxCoalescedDatagrams += count;
        }
        mStats.rxDatagrams += datagrams;
        mStats.maxRxBatch = std::max(mStats.maxRxBatch, datagrams);
        if (received) co_return std::span<const Received>{mReceived.data(), received};
    }
}

} // namespace quic
