/**
 * @file skbuf.cppm
 * @author BusyStudent (fyw90mc@gmail.com)
 * @brief Common utils for handling buffer for socket
 * @version 0.1
 * @date 2026-09-28
 * 
 * @copyright Copyright (c) 2026
 * 
 */
module;

#include <cassert> // assert

export module hajimi.skbuf;
import std;

// Forward declarations
export class SkBufferPool;
export class SkBuffer;

/**
 * @brief The node of the socket buffer | Node | Storage { HeadRoom, Data, TailRoom } | 
 * 
 */
class SkBufferNode {
public:
    // Free List
    SkBufferNode *next = nullptr;

    // Info
    SkBufferPool &pool; // The pool of the buffer
    std::uint32_t capacity; // The capacity of the buffer
    std::uint8_t  classIdx = 0xFF; // The class index of the buffer (0xFF on oversize)

    // Mutable info
    std::size_t   refcount = 0; // The reference count of the buffer

    // Storage
    // std::byte     storage[];
    inline auto storage() -> std::byte * {
        return reinterpret_cast<std::byte *>(this) + sizeof(SkBufferNode);
    }
};

// MARK: SkBufferPool
/**
 * @brief The socket buffer pool
 * 
 *   Slab
 *   +---------+---------+---------+---------+
 *   | Node+Buf| Node+Buf| Node+Buf| Node+Buf|
 *   +---------+---------+---------+---------+
 */
export class SkBufferPool {
public:
    /**
     * @brief The configuration of the buffer pool
     * 
     */
    struct ClassInfo {
        std::uint32_t capacity;
        std::uint32_t blocksPerSlab;
    };

    /**
     * @brief Construct a new Sk Buffer Pool
     * 
     * @param configs
     * @param upstream 
     */
    inline explicit SkBufferPool(std::initializer_list<ClassInfo> configs, std::pmr::memory_resource *upstream = std::pmr::get_default_resource()) : 
        mUpstream(upstream), 
        mClasses(upstream)
    {
        auto alignUp = [](std::size_t size, std::size_t align) {
            return (size + align - 1) & ~(align - 1);
        };
        // Register the classes
        for (auto &config : configs) {
            assert(config.capacity != 0 && "Bad config, capacity should > 0");
            assert(config.blocksPerSlab != 0 && "Bad config, blocksPerSlab should > 0");
            mClasses.emplace_back(Class {
                .capacity = config.capacity,
                .blocksPerSlab = config.blocksPerSlab,
                .stride = alignUp(sizeof(SkBufferNode) + config.capacity, alignof(SkBufferNode)),
                .slabs = std::pmr::vector<Slab>{upstream},
            });
        }
        std::ranges::sort(mClasses, [](const auto &a, const auto &b) {
            return a.capacity < b.capacity;
        });
        assert(mClasses.size() < 0xFF && "Too many classes");
        assert(mClasses.size() > 0 && "No class is registered");
    }

    // No copy and move
    inline SkBufferPool(const SkBufferPool &) = delete;
    inline SkBufferPool(SkBufferPool &&) = delete;

    inline ~SkBufferPool() {
        static_assert(std::is_trivially_destructible_v<SkBufferNode>, "SkBufferNode is not trivially destructible");
        assert(mBuffers == 0 && "Some buffers are not deallocated");
        for (auto &class_ : mClasses) {
            for (auto &slab : class_.slabs) { // Trivially destructible, no need to call destructor
                mUpstream->deallocate(slab.ptr, slab.size, alignof(SkBufferNode));
            }
        }
    }

    /**
     * @brief Allocate the buffer from the pool
     * 
     * @param capacity The whole capacity of the buffer
     * @return SkBuffer 
     */
    [[nodiscard]]
    inline auto allocate(std::size_t capacity) -> SkBuffer;

    // Deallocate the buffer to the pool
    inline auto deallocate(SkBufferNode *node) -> void;
private:
    // Add a big slab to the class and split into blocks
    inline auto refill(std::uint8_t classIdx) -> void;

    // The whole memory block allocate from upstream
    struct Slab {
        std::byte  *ptr;
        std::size_t size;
    };

    struct Class {
        // Info
        std::uint32_t capacity;
        std::uint32_t blocksPerSlab;

        std::size_t   stride = 0;

        // Free list
        SkBufferNode *freeList = nullptr;

        // The memory blocks we actually allocate from upstream
        std::pmr::vector<Slab> slabs;
    };

    std::pmr::memory_resource *mUpstream;
    std::pmr::vector<Class>    mClasses;
    std::size_t                mBuffers = 0; // The humber the buffer thats we allocated
};

// MARK: SkBuffer
// TODO: COW
/**
 * @brief The socket buffer { HeadRoom | Data | TailRoom }
 * 
 * @note The buffer is not MT-Safe, use it at single thread
 */
export class SkBuffer {
public:
    inline SkBuffer(SkBuffer &&) noexcept = default;
    inline SkBuffer() = default;

    // Check the buffer is unique?
    [[nodiscard]]
    inline auto unique() const -> bool {
        if (!mBuffer) return false;
        return mBuffer->refcount == 1;
    }

    // Check the buffer is empty?
    [[nodiscard]]
    inline auto empty() const -> bool {
        return size() == 0;
    }

    // Get the data size of the buffer
    [[nodiscard]]
    inline auto size() const -> std::size_t {
        if (!mBuffer) return 0;
        return mTail - mHead;
    }

    // Get the data begin of the buffer
    [[nodiscard]]
    inline auto data() const -> std::span<const std::byte> {
        if (!mBuffer) return {};
        return {mBuffer->storage() + mHead, size()};
    }

    // Get the mutable data begin of the buffer
    [[nodiscard]]
    inline auto mutableData() -> std::span<std::byte> {
        if (!mBuffer) return {};
        assert(unique() && "Buffer is not unique, Please cow before get data");
        return {mBuffer->storage() + mHead, size()};
    }

    // Get the whole capacity of the buffer
    [[nodiscard]]
    inline auto capacity() const -> std::size_t {
        if (!mBuffer) return 0;
        return mBuffer->capacity;
    }

    // Get the num of bytes that can be prepend to the buffer
    [[nodiscard]]
    inline auto headroom() const -> std::size_t {
        if (!mBuffer) return 0;
        return mHead;
    }

    // Get the num of bytes that can be append to the buffer
    [[nodiscard]]
    inline auto tailroom() const -> std::size_t {
        if (!mBuffer) return 0;
        return mBuffer->capacity - mTail;
    }

    // Get the buffer pool
    [[nodiscard]]
    inline auto pool() const -> SkBufferPool * {
        return mBuffer ? &mBuffer->pool : nullptr;
    }

    // Prepare and commit the buffer
    // Back
    [[nodiscard]]
    inline auto prepareBack(std::size_t size) -> std::span<std::byte> {
        assert(mBuffer && "Buffer is not allocated");
        assert(size <= tailroom() && "Buffer is overflow");
        assert(unique() && "Buffer is not unique, Please makeWritable() before preapreBack");
        return {mBuffer->storage() + mTail, size};
    }

    inline auto commitBack(std::size_t size) -> void {
        assert(mBuffer && "Buffer is not allocated");
        assert(size <= tailroom() && "Buffer is overflow");
        mTail += size;
    }

    inline auto consumeBack(std::size_t size) -> void {
        assert(mBuffer && "Buffer is not allocated");
        assert(size <= this->size() && "Buffer is overflow");
        mTail -= size;
    }

    // Front
    inline auto consumeFront(std::size_t size) -> void {
        assert(mBuffer && "Buffer is not allocated");
        assert(size <= this->size() && "Buffer is overflow");
        mHead += size;
    }

    // Clear the buffer to empty
    inline auto clear() -> void {
        mHead = 0;
        mTail = 0;
    }

    // Reserve the number of bytes that can be prepend to the buffer, only can be called when buffer is empty
    inline auto reserveHead(std::size_t headroom) -> void {
        assert(mBuffer && "Buffer is not allocated");
        assert(headroom <= mBuffer->capacity && "Buffer is overflow");
        assert(empty() && "reserve can only be called when buffer is empty");
        mHead = headroom;
        mTail = headroom;
    }

    // Utils
    // Append datas to the buffer back
    inline auto append(std::span<const std::byte> data) -> void {
        if (data.empty()) return;
        auto buf = prepareBack(data.size());
        std::memcpy(buf.data(), data.data(), data.size());
        commitBack(data.size());
    }

    // Append a string to the back of the buffer
    inline auto appendString(std::string_view str) -> void {
        append(std::as_bytes(std::span{str}));
    }

    // Append a int to the back of the buffer, converted as be
    template <std::integral T>
    inline auto appendIntBE(T val) -> void {
        if (std::endian::native != std::endian::big) {
            val = std::byteswap(val);
        }
        append(std::bit_cast<std::array<std::byte, sizeof(val)>>(val));
    }
    
    // Append datas to the buffer front
    inline auto prepend(std::span<const std::byte> data) -> void {
        assert(mBuffer && "Buffer is not allocated");
        assert(mHead >= data.size() && "Buffer is overflow");
        assert(unique() && "Buffer is not unique, Please makeWritable() before prepend");
        std::memcpy(mBuffer->storage() + mHead - data.size(), data.data(), data.size());
        mHead -= data.size();
    }

    template <std::integral T>
    inline auto prependIntBE(T val) -> void {
        if (std::endian::native != std::endian::big) {
            val = std::byteswap(val);
        }
        prepend(std::bit_cast<std::array<std::byte, sizeof(val)>>(val));
    }

    // Pop data from the front of the buffer
    template <std::integral T>
    [[nodiscard]]
    inline auto popIntBE() -> std::optional<T> {
        auto buf = data();
        if (buf.size() < sizeof(T)) return std::nullopt;

        // Load from the buffer (maybe unaligned, use memcpy)
        T val{};
        std::memcpy(&val, buf.data(), sizeof(val));
        consumeFront(sizeof(val));
        if (std::endian::native != std::endian::big) {
            val = std::byteswap(val);
        }
        return val;
    }

    // Pop string from the front of the buffer
    [[nodiscard]]
    inline auto popString(std::size_t size) -> std::optional<std::string> {
        if (!mBuffer || this->size() < size) return std::nullopt;
        auto buf = data();
        std::string str{reinterpret_cast<const char *>(buf.data()), size};
        consumeFront(size);
        return str;
    }

    // Pop string from the whole buffer
    [[nodiscard]]
    inline auto popString() -> std::optional<std::string> {
        return popString(size());
    }

    // Swap
    inline auto swap(SkBuffer &other) noexcept -> void {
        std::swap(mBuffer, other.mBuffer);
        std::swap(mHead, other.mHead);
        std::swap(mTail, other.mTail);
    }

    // Clone, add an refcount to it
    [[nodiscard]]
    inline auto clone() const -> SkBuffer {
        if (!mBuffer) return {};
        mBuffer->refcount += 1;
        return SkBuffer {
            mBuffer.get(),
            mHead,
            mTail
        };
    }

    // Slice the buffer
    [[nodiscard]]
    inline auto slice(std::size_t offset, std::size_t len) const -> SkBuffer {
        assert(offset <= size() && "Offset is overflow");
        assert(len <= size() - offset && "Length is overflow");

        auto buf = clone();
        buf.mHead += offset;
        buf.mTail = buf.mHead + len;
        return buf;
    }

    // Move all data to the position after the given headroom
    inline auto compact(std::size_t headroom = 0) -> void {
        assert(mBuffer && "Buffer is not allocated");
        assert(unique() && "Buffer is not unique, Please makeWritable() before compact");

        auto size = this->size();
        assert(headroom <= capacity() - size && "Buffer is overflow");

        if (size != 0 && mHead != headroom) {
            std::memmove(
                mBuffer->storage() + headroom,
                mBuffer->storage() + mHead,
                size
            );
        }

        mHead = headroom;
        mTail = headroom + size;
    }

    // Make the buffer writable, doing cow
    inline auto makeWritable() -> void {
        if (!mBuffer || unique()) return;

        // Allocate same capacity
        auto buf = pool()->allocate(capacity());
        buf.mHead = mHead;
        buf.mTail = mHead; // Currently zero size
        buf.append(data()); // Copy the data

        swap(buf);
    }
    

    // Operator
    inline auto operator =(SkBuffer &&) noexcept -> SkBuffer & = default;
    inline auto operator ==(const SkBuffer &) const -> bool = delete; // Compare the data by yourself

    // Check the buffer is valid?
    [[nodiscard]]
    inline explicit operator bool() const noexcept {
        return static_cast<bool>(mBuffer);
    }
private:
    inline explicit SkBuffer(SkBufferNode *node, std::uint32_t head = 0, std::uint32_t tail = 0) : mBuffer(node), mHead(head), mTail(tail) {}

    struct Deleter {
        auto operator()(SkBufferNode *node) -> void {
            node->refcount -= 1;
            if (node->refcount == 0) {
                node->pool.deallocate(node);
            }
        }
    };

    std::unique_ptr<SkBufferNode, Deleter> mBuffer;
    std::uint32_t mHead = 0; // The head of the buffer
    std::uint32_t mTail = 0; // The tail of the buffer
friend class SkBufferPool;
};

// MARK: SkBufferPool Impl
inline auto SkBufferPool::allocate(std::size_t capacity) -> SkBuffer {
    // Precond
    assert(std::in_range<std::uint32_t>(capacity) && "Capacity is too large");

    // Find the class
    auto it = std::lower_bound(
        mClasses.begin(),
        mClasses.end(),
        capacity,
        [](const Class &klass, std::size_t size) {
            return klass.capacity < size;
        }
    );

    SkBufferNode *node = nullptr;
    if (it == mClasses.end()) { // Oversize, just allocate from the upstream
        auto size = capacity + sizeof(SkBufferNode);
        auto ptr = static_cast<std::byte *>(mUpstream->allocate(size, alignof(SkBufferNode)));

        node = new (ptr) SkBufferNode {
            .next = nullptr,
            .pool = *this,
            .capacity = static_cast<std::uint32_t>(capacity),
            .classIdx = 0xFF,
        };
    }
    else {
        // Get the slot idx
        auto &klass = *it;
        auto idx = it - mClasses.begin();

        if (!klass.freeList) [[unlikely]] {
            // Sad Path, Allocate a new slab
            refill(idx);
        }
        
        // Take the node from the list
        node = klass.freeList;
        klass.freeList = node->next;
        node->next = nullptr;
    }

    // Set the mutable info
    node->refcount = 1; //< Currently only one reference
    mBuffers += 1;
    return SkBuffer{node};
}

inline auto SkBufferPool::deallocate(SkBufferNode *node) -> void {
    // Precond
    assert(node->refcount == 0 && "Invalid refcount, still using ?");
    assert((node->classIdx < mClasses.size() || node->classIdx == 0xFF) && "Invalid class index");
    mBuffers -= 1;
    if (node->classIdx == 0xFF) { // Oversized, give back to upstream
        mUpstream->deallocate(node, node->capacity + sizeof(SkBufferNode), alignof(SkBufferNode));
        return;
    }

    // Add it back to the free list
    auto &klass = mClasses[node->classIdx];
    node->next = klass.freeList;
    klass.freeList = node;
}

inline auto SkBufferPool::refill(std::uint8_t classIdx) -> void {
    // Allocate a new slab
    auto &klass = mClasses[classIdx];
    auto size = klass.stride * klass.blocksPerSlab;
    auto slab = static_cast<std::byte *>(mUpstream->allocate(size, alignof(SkBufferNode)));

    // Add it
    try {
        klass.slabs.push_back({slab, size});
    }
    catch (...) { // WTF: bad alloc?
        mUpstream->deallocate(slab, size, alignof(SkBufferNode));
        throw;
    }

    // Split it into blocks
    for (auto i = 0; i < klass.blocksPerSlab; ++i) {
        auto ptr = reinterpret_cast<SkBufferNode *>(slab + i * klass.stride);
        auto node = new (ptr) SkBufferNode {
            .next = klass.freeList,
            .pool = *this,
            .capacity = klass.capacity,
            .classIdx = classIdx,
        };
        klass.freeList = node;
    }
}
