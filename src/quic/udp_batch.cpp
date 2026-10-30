#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "udp_batch.hpp"
#include "cmsg.hpp"
#include <algorithm>
#include <cerrno>

#if defined(__linux__)
#include <sys/socket.h>
#endif

using ilias::Err;
using ilias::IoResult;
using ilias::IoTask;
using ilias::CmsgWriter;
using ilias::CmsgReader;
using ilias::MsgHdr;
using ilias::MutableMsgHdr;
using ilias::IoVec;
using ilias::MutableIoVec;

namespace quic {
namespace {

#if defined(_WIN32)
using SendSegment = cmsg::UdpSendMsgSize;
using ReceiveSegment = cmsg::UdpCoalescedInfo;
using SendEcn4 = cmsg::Ipv4Ecn;
using ReceiveEcn4 = cmsg::Ipv4Ecn;
using Ecn6 = cmsg::Ipv6Ecn;
using ReceiveEcn4Option = sockopt::Ipv4RecvEcn;
using ReceiveEcn6Option = sockopt::Ipv6RecvEcn;
#else
#ifdef UDP_SEGMENT
using SendSegment = cmsg::UdpSegment;
#endif
#ifdef UDP_GRO
using ReceiveSegment = cmsg::UdpGro;
#endif
using SendEcn4 = cmsg::Ipv4TosSend;
using ReceiveEcn4 = cmsg::Ipv4TosRecv;
using Ecn6 = cmsg::Ipv6TrafficClass;
using ReceiveEcn4Option = sockopt::Ipv4RecvTos;
using ReceiveEcn6Option = sockopt::Ipv6RecvTrafficClass;
#endif

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

auto appendEcn(CmsgWriter &writer, bool ipv6, unsigned int ecn) -> void {
    if (ipv6) writer.append(Ecn6{static_cast<int>(ecn)});
    else writer.append(SendEcn4(static_cast<int>(ecn)));
}

// A coalesced receive must retain the original UDP datagram boundaries.
auto receiveInfo(const MutableMsgHdr &msg, size_t size, size_t &segment, unsigned int &ecn) -> bool {
    segment = 0;
    ecn = 0;
    if (msg.flags() & (MSG_TRUNC | MSG_CTRUNC)) {
        return false;
    }
    for (auto &control : CmsgReader{msg}) {
#if defined(_WIN32) || defined(UDP_GRO)
        if (control.hold<ReceiveSegment>()) {
            auto val = control.decode<ReceiveSegment>();
            if (!val || val->value() <= 0 || val->value() > 65535 || !size) return false;
            segment = val->value();
        }
        else
#endif
        if (control.hold<ReceiveEcn4>()) {
            auto value = control.decode<ReceiveEcn4>();
            if (!value) return false;
            ecn = value->value() & 3;
        }
        else if (control.hold<Ecn6>()) {
            auto value = control.decode<Ecn6>();
            if (!value) return false;
            ecn = value->value() & 3;
        }
    }
    return true;
}

} // namespace

auto UdpBatchSocket::from(ilias::Socket socket, SkBufferPool &pool, bool batching) -> IoResult<UdpBatchSocket> {
    UdpBatchSocket result;
    result.mBatching = batching;
    ILIAS_TRY(result.mSocket, ilias::UdpSocket::from(std::move(socket)));
    ILIAS_TRY(auto local, result.mSocket.localEndpoint());
    auto family = local.family();
    if (batching) {
#if defined(_WIN32)
        result.mStats.receiveCoalescing = bool(result.mSocket.setOption(sockopt::UdpRecvMaxCoalescedSize{65527}));
        result.mStats.sendSegmentation = bool(result.mSocket.getOption<sockopt::UdpSendMsgSize>());
#elif defined(UDP_SEGMENT)
        result.mStats.sendSegmentation = true; // Validate on the first real segmented submission.
#ifdef UDP_GRO
        result.mStats.receiveCoalescing = bool(result.mSocket.setOption(sockopt::UdpGro{1}));
#endif
#endif
    }
    // Dual-stack sockets need both options to receive metadata for both families.
    auto _ = result.mSocket.setOption(ReceiveEcn4Option{1});
    if (family == AF_INET6) {
        auto _ = result.mSocket.setOption(ReceiveEcn6Option{1});
    }
    result.mSendControl.resize(128);
    result.mReceive.resize(
#if defined(__linux__)
        batching ? receiveSlots :
#endif
        1);
    for (auto &slot : result.mReceive) {
        slot.bytes = pool.allocate(receiveCapacity);
        slot.control.resize(128);
    }
    return result;
}

auto UdpBatchSocket::send(ilias::Buffer data, const IPEndpoint &remote,
    size_t segmentSize, unsigned int ecn) -> Task<Sent> {
    if (data.empty() || !segmentSize || segmentSize > data.size() ||
        data.size() > sendCapacity || (data.size() + segmentSize - 1) / segmentSize > maxSegments) {
        co_return Sent{0, std::make_error_code(std::errc::invalid_argument)};
    }
    auto count = (data.size() + segmentSize - 1) / segmentSize;
    auto ipv6 = remote.family() == AF_INET6 && !remote.address6().isV4Mapped();
#if defined(_WIN32) || defined(UDP_SEGMENT)
    if (count > 1 && mStats.sendSegmentation) {
        CmsgWriter writer{mSendControl};
        writer.append(SendSegment{static_cast<uint16_t>(segmentSize)});
        appendEcn(writer, ipv6, ecn);
        IoVec vec{data};
        MsgHdr msg;
        msg.setEndpoint(remote).setBuffers(std::span{&vec, 1}).setControl(writer.end());
        ++mStats.txSubmissions;
        auto sent = co_await mSocket.sendmsg(msg);
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
#endif
    // Rebuild after a failed segmented send; all fallback messages share this ECN.
    CmsgWriter writer{mSendControl};
    if (ecn) appendEcn(writer, ipv6, ecn);
    // Winsock requires a null control pointer when no control messages are sent.
    auto control = ecn ? writer.end() : ilias::Buffer{};
    size_t completed = 0;
#if defined(__linux__)
    if (mBatching) {
        std::array<mmsghdr, maxSegments> messages{};
        std::array<IoVec, maxSegments> vectors;
        for (size_t i = 0; i < count; ++i) {
            auto bytes = data.subspan(i * segmentSize, std::min(segmentSize, data.size() - i * segmentSize));
            vectors[i] = IoVec{bytes};
            MsgHdr msg;
            msg.setEndpoint(remote).setBuffers(std::span{&vectors[i], 1}).setControl(control);
            messages[i].msg_hdr = msg;
        }
        while (completed < count) {
            co_await ilias::this_coro::stopPoint();
            ++mStats.txSubmissions;
            auto n = ::sendmmsg(mSocket.fd().get(), messages.data() + completed,
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
                auto ready = co_await mSocket.poll(ilias::PollEvent::Out);
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
        IoVec vec{bytes};
        MsgHdr msg;
        msg.setEndpoint(remote).setBuffers(std::span{&vec, 1}).setControl(control);
        ++mStats.txSubmissions;
        auto sent = co_await mSocket.sendmsg(msg);
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
            std::array<MutableIoVec, receiveSlots> vectors;
            for (size_t i = 0; i < mReceive.size(); ++i) {
                auto &slot = mReceive[i];
                slot.bytes.clear();
                vectors[i] = MutableIoVec{ilias::makeBuffer(slot.bytes.prepareBack(receiveCapacity))};
                MutableMsgHdr msg;
                msg.setEndpoint(slot.remote).setBuffers(std::span{&vectors[i], 1}).setControl(slot.control.data());
                messages[i].msg_hdr = msg;
            }
            ++mStats.rxSubmissions;
            auto n = ::recvmmsg(mSocket.fd().get(), messages.data(), static_cast<unsigned int>(mReceive.size()), MSG_DONTWAIT, nullptr);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                ILIAS_CO_TRYV(co_await mSocket.poll(ilias::PollEvent::In));
                continue;
            }
            if (n < 0) co_return Err(ilias::SystemError::fromErrno());
            for (int i = 0; i < n; ++i) {
                MutableMsgHdr msg{messages[i].msg_hdr};
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
            MutableIoVec vec{ilias::makeBuffer(slot.bytes.prepareBack(receiveCapacity))};
            MutableMsgHdr msg;
            msg.setEndpoint(slot.remote).setBuffers(std::span{&vec, 1}).setControl(slot.control.data());
            ++mStats.rxSubmissions;
            auto result = co_await mSocket.recvmsg(msg);
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
