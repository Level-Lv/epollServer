// common.h — FSync Server 协议常量与大端编解码
// 依据：PRD §5（协议）、§9.1（错误码）；架构文档 §8.6。
// 纯头文件：无状态、所有函数 inline，仅依赖 <cstdint> / <cstddef>。
#pragma once

#include <cstddef>
#include <cstdint>

// ---------- 协议常量（PRD §5.1） ----------
inline constexpr uint32_t    PROTOCOL_MAGIC   = 0x4653594EU;            // ASCII "FSYN"
inline constexpr uint8_t     PROTOCOL_VERSION = 0x01U;
inline constexpr std::size_t HEADER_SIZE      = 10;                     // magic(4)+ver(1)+type(1)+body_len(4)
inline constexpr uint32_t    MAX_BODY_LEN     = 4U * 1024U * 1024U;     // 4 MiB
inline constexpr std::size_t MAX_FILENAME_LEN = 512;                    // PRD §5.3
inline constexpr std::size_t OUT_HIGH_WATER   = 8U * 1024U * 1024U;     // PRD §8.3
inline constexpr std::size_t OUT_LOW_WATER    = 4U * 1024U * 1024U;     // PRD §8.3
inline constexpr std::size_t MD5_HEX_LEN      = 32;                     // 小写 hex

// ---------- 消息类型（PRD §5.2） ----------
enum class MsgType : uint8_t {
    FILE_META  = 0x01,  // C→S
    FILE_CHUNK = 0x02,  // C→S
    FILE_END   = 0x03,  // C→S
    HEARTBEAT  = 0x20,  // C→S
    ACK        = 0x80,  // S→C
    ERROR_MSG  = 0x81,  // S→C；避开 <windows.h> 的 ERROR 宏（PRD §5.2 协议名写作 ERROR）
};

// ---------- 错误码（PRD §9.1） ----------
enum class ErrCode : uint32_t {
    OK            = 0,
    BAD_MAGIC     = 1,
    BAD_VERSION   = 2,
    BAD_TYPE      = 3,
    BODY_TOO_BIG  = 4,
    NO_SESSION    = 5,
    BAD_FILENAME  = 6,
    WRITE_FAILED  = 7,
    MD5_MISMATCH  = 8,
    SIZE_MISMATCH = 9,
    OVERFLOW      = 10,
    INTERNAL_ERR  = 11,
};

// ---------- 大端读写 ----------
// 逐字节移位实现；不用 htons/ntohl，无 reinterpret_cast、无 memcpy 到整数。

inline uint16_t be_get16(const uint8_t* p) {
    return static_cast<uint16_t>(
        (static_cast<uint16_t>(p[0]) << 8) |
         static_cast<uint16_t>(p[1]));
}

inline uint32_t be_get32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8)  |
            static_cast<uint32_t>(p[3]);
}

inline uint64_t be_get64(const uint8_t* p) {
    return (static_cast<uint64_t>(p[0]) << 56) |
           (static_cast<uint64_t>(p[1]) << 48) |
           (static_cast<uint64_t>(p[2]) << 40) |
           (static_cast<uint64_t>(p[3]) << 32) |
           (static_cast<uint64_t>(p[4]) << 24) |
           (static_cast<uint64_t>(p[5]) << 16) |
           (static_cast<uint64_t>(p[6]) << 8)  |
            static_cast<uint64_t>(p[7]);
}

inline void be_put16(uint8_t* p, uint16_t v) {
    p[0] = static_cast<uint8_t>(v >> 8);
    p[1] = static_cast<uint8_t>(v);
}

inline void be_put32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v >> 24);
    p[1] = static_cast<uint8_t>(v >> 16);
    p[2] = static_cast<uint8_t>(v >> 8);
    p[3] = static_cast<uint8_t>(v);
}

inline void be_put64(uint8_t* p, uint64_t v) {
    p[0] = static_cast<uint8_t>(v >> 56);
    p[1] = static_cast<uint8_t>(v >> 48);
    p[2] = static_cast<uint8_t>(v >> 40);
    p[3] = static_cast<uint8_t>(v >> 32);
    p[4] = static_cast<uint8_t>(v >> 24);
    p[5] = static_cast<uint8_t>(v >> 16);
    p[6] = static_cast<uint8_t>(v >> 8);
    p[7] = static_cast<uint8_t>(v);
}
