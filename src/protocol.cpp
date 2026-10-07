module;
#include <ilias/macros.hpp>
#include <cassert>
#include <cstdio>

module hajimi.protocol;
import std;
import ilias;
import hajimi.skbuf;

auto MessageReader::readMessage() -> ilias::IoTask<Message> {
    std::byte header[HAJIMI_HEADER] {};
    ILIAS_CO_TRYV(co_await mStream.readAll(header));

    std::uint16_t len = std::to_integer<std::uint16_t>(header[0]) << 8 | std::to_integer<std::uint16_t>(header[1]);
    std::uint8_t type = std::to_integer<std::uint8_t>(header[2]);

    if (len != 0) {
        if (mBuffer.capacity() < len) {
            mBuffer = mPool.allocate(len);
        }
        mBuffer.clear();
        auto span = mBuffer.prepareBack(len);
        ILIAS_CO_TRYV(co_await mStream.readAll(span));
        mBuffer.commitBack(len);
    }
    if (auto msg = parse(type, mBuffer)) {
        co_return std::move(*msg);
    }
    co_return ilias::Err(std::make_error_code(std::errc::bad_message));
}

auto MessageReader::parse(std::uint8_t type, SkBuffer &buffer) -> std::optional<Message> {
    switch (static_cast<MessageType>(type)) {
        case MessageType::Hello: {
            ILIAS_TRY(auto version, buffer.popIntBE<std::uint16_t>());
            ILIAS_TRY(auto name, buffer.popString());

            // std::println("[Protocol] Hello: {}, {}", version, name);
            return Message {
                Hello {
                    .version = version,
                    .name = std::move(name),
                }
            };
        }

        case MessageType::HelloAck: {
            // std::println("[Protocol] HelloAck");
            return Message { HelloAck{} };
        }

        case MessageType::Ping: {
            // std::println("[Protocol] Ping");
            return Message { Ping{} };
        }

        case MessageType::Pong: {
            // std::println("[Protocol] Pong");
            return Message { Pong{} };
        }

        case MessageType::OpenTunnel: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<std::uint64_t>());
            ILIAS_TRY(auto endpoint, buffer.popString());
            // std::println("[Protocol] OpenTunnel {} => {}", streamId, endpoint);
            return Message {
                OpenTunnel {
                    .streamId = streamId,
                    .endpoint = std::move(endpoint)
                }
            };
        }

        case MessageType::TunnelClose: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<std::uint64_t>());
            // std::println("[Protocol] TunnelClose {}", streamId);
            return Message {
                TunnelClose {
                    .streamId = streamId,
                }
            };
        }

        case MessageType::DataExchange: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<std::uint64_t>());
            // std::println("[Protocol] DataExchange {}, {} bytes", streamId, data.size());
            return Message {
                DataExchange {
                    .streamId = streamId,
                    .data = std::exchange(buffer, SkBuffer{}),
                }
            };
        }

        case MessageType::WindowUpdate: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<std::uint64_t>());
            ILIAS_TRY(auto size, buffer.popIntBE<std::uint32_t>());
            // std::println("[Protocol] WindowUpdate {}, {} bytes", streamId, size);
            return Message {
                WindowUpdate {
                    .streamId = streamId,
                    .size = size
                }
            };
        }

        case MessageType::FatalError: {
            ILIAS_TRY(auto msg, buffer.popString());
            // std::println("[Protocol] FatalError {}", msg);
            return Message {
                FatalError {
                    .msg = std::move(msg)
                }  
            };
        }
        default: {
            std::println(stderr, "[Message] readMessage, invalid type: {}", type);
            return std::nullopt;
        }
    }
}

auto MessageWriter::writeMessage(Message msg) -> ilias::IoTask<void> {
    // The number of bytes of the message
    mBuffer.clear();
    mBuffer.reserveHead(HAJIMI_HEADER_ROOM);

    switch (msg.type()) {
        case MessageType::Hello: {
            auto hello = msg.cast<Hello>().value();
            mBuffer.appendIntBE(hello.version);
            mBuffer.append(ilias::makeBuffer(hello.name));
            break;
        }
        case MessageType::HelloAck: break; // No-payload
        case MessageType::Ping: break;
        case MessageType::Pong: break;

        case MessageType::OpenTunnel: {
            auto tunnel = msg.cast<OpenTunnel>().value();
            mBuffer.appendIntBE(tunnel.streamId);
            mBuffer.append(ilias::makeBuffer(tunnel.endpoint));
            break;
        }
         case MessageType::DataExchange: {
            auto exchange = msg.cast<DataExchange>().value();
            auto buffer = std::exchange(exchange.data, SkBuffer{});
            // We just use the message buffer, avoid the copy
            // len | type | streamId | data |
            assert(buffer.headroom() >= HAJIMI_HEADER_ROOM && "This buffer is too small");
            if (buffer.size() > HAJIMI_MAX_DATA_EXCHANGE || buffer.headroom() < HAJIMI_HEADER_ROOM) [[unlikely]] {
                co_return ilias::Err(std::make_error_code(std::errc::bad_message));
            }

            buffer.prependIntBE(exchange.streamId);
            auto size = buffer.size(); // The size only contains streamId + data

            buffer.prependIntBE(static_cast<std::uint8_t>(MessageType::DataExchange));
            buffer.prependIntBE(static_cast<std::uint16_t>(size));

            // Write it
            ILIAS_CO_TRYV(co_await mStream.writeAll(buffer.data()));
            co_return co_await mStream.flush();
        }

        case MessageType::TunnelClose: {
            auto close = msg.cast<TunnelClose>().value();
            mBuffer.appendIntBE(close.streamId);
            break;
        }

        case MessageType::WindowUpdate: {
            auto update = msg.cast<WindowUpdate>().value();
            mBuffer.appendIntBE(update.streamId);
            mBuffer.appendIntBE(update.size);
            break;
        }

        case MessageType::FatalError: {
            auto err = msg.cast<FatalError>().value();
            mBuffer.append(ilias::makeBuffer(err.msg));
            break;
        }

        default: { // Unsupported now
            assert(false && "TODO: Currently not impl");
        }
    }
    // Store how many bytes we are used
    auto size = mBuffer.size();

    // | Size | Type | Data |
    mBuffer.prependIntBE(static_cast<std::uint8_t>(msg.type()));
    mBuffer.prependIntBE(static_cast<std::uint16_t>(size));

    // Write a;;
    ILIAS_CO_TRYV(co_await mStream.writeAll(mBuffer.data()));
    ILIAS_CO_TRYV(co_await mStream.flush());
    co_return {};
}
