// md5.cpp — RFC 1321 MD5 实现（按 §3.1–3.5；逐字节处理，无对齐、字节序假设）
#include "md5.h"

#include <algorithm>
#include <cstring>

namespace {

// RFC 1321 §3.4 的 T 表：T[i] = floor(2^32 * |sin(i+1)|)，i = 0..63
// （取值与 RFC 参考实现一致；四轮各 16 项）
constexpr uint32_t K[64] = {
    0xd76aa478u, 0xe8c7b756u, 0x242070dbu, 0xc1bdceeeu,   // F
    0xf57c0fafu, 0x4787c62au, 0xa8304613u, 0xfd469501u,
    0x698098d8u, 0x8b44f7afu, 0xffff5bb1u, 0x895cd7beu,
    0x6b901122u, 0xfd987193u, 0xa679438eu, 0x49b40821u,
    0xf61e2562u, 0xc040b340u, 0x265e5a51u, 0xe9b6c7aau,   // G
    0xd62f105du, 0x02441453u, 0xd8a1e681u, 0xe7d3fbc8u,
    0x21e1cde6u, 0xc33707d6u, 0xf4d50d87u, 0x455a14edu,
    0xa9e3e905u, 0xfcefa3f8u, 0x676f02d9u, 0x8d2a4c8au,
    0xfffa3942u, 0x8771f681u, 0x6d9d6122u, 0xfde5380cu,   // H
    0xa4beea44u, 0x4bdecfa9u, 0xf6bb4b60u, 0xbebfbc70u,
    0x289b7ec6u, 0xeaa127fau, 0xd4ef3085u, 0x04881d05u,
    0xd9d4d039u, 0xe6db99e5u, 0x1fa27cf8u, 0xc4ac5665u,
    0xf4292244u, 0x432aff97u, 0xab9423a7u, 0xfc93a039u,   // I
    0x655b59c3u, 0x8f0ccc92u, 0xffeff47du, 0x85845dd1u,
    0x6fa87e4fu, 0xfe2ce6e0u, 0xa3014314u, 0x4e0811a1u,
    0xf7537e82u, 0xbd3af235u, 0x2ad7d2bbu, 0xeb86d391u,
};

// RFC 1321 §3.4 每步左循环移位量（s 值，四轮各 16 项）
constexpr uint8_t S[64] = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
};

inline uint32_t rotl32(uint32_t v, unsigned s) {
    return (v << s) | (v >> (32u - s));
}

}  // namespace

void MD5::reset() {
    h_ = {0x67452301u, 0xefcdab89u, 0x98badcfeu, 0x10325476u};  // RFC 1321 §3.3
    buf_.fill(0);
    nbytes_ = 0;
    nbuf_ = 0;
}

void MD5::update(const void* data, std::size_t len) {
    if (len == 0) return;  // 兼容 update(nullptr, 0)

    const uint8_t* p = static_cast<const uint8_t*>(data);
    nbytes_ += len;

    // 先补满当前部分块
    if (nbuf_ != 0) {
        const std::size_t take = std::min(len, buf_.size() - nbuf_);
        std::memcpy(buf_.data() + nbuf_, p, take);
        nbuf_ += take;
        p += take;
        len -= take;
        if (nbuf_ == buf_.size()) {
            process_block(buf_.data());
            nbuf_ = 0;
        }
    }

    // 整块就地处理（process_block 逐字节装载 x[]，无对齐要求）
    while (len >= buf_.size()) {
        process_block(p);
        p += buf_.size();
        len -= buf_.size();
    }

    // 余数留在缓冲里，等待下次 update 或 final_hex
    if (len != 0) {
        std::memcpy(buf_.data(), p, len);
        nbuf_ = len;
    }
}

void MD5::process_block(const uint8_t* block) {
    // x[k] 逐字节按小端装载（RFC 1321 §3.3），与主机字节序/对齐无关
    uint32_t x[16];
    for (unsigned k = 0; k < 16; ++k) {
        x[k] =  static_cast<uint32_t>(block[4u * k + 0u])
             | (static_cast<uint32_t>(block[4u * k + 1u]) << 8)
             | (static_cast<uint32_t>(block[4u * k + 2u]) << 16)
             | (static_cast<uint32_t>(block[4u * k + 3u]) << 24);
    }

    uint32_t a = h_[0];
    uint32_t b = h_[1];
    uint32_t c = h_[2];
    uint32_t d = h_[3];

    // 64 步变换（RFC 1321 §3.4）：F/G/H/I 四轮，每轮 16 步
    for (unsigned i = 0; i < 64; ++i) {
        uint32_t f;
        unsigned g;
        if (i < 16) {         // F(b,c,d)
            f = (b & c) | (~b & d);
            g = i;
        } else if (i < 32) {  // G(b,c,d)
            f = (d & b) | (~d & c);
            g = (5u * i + 1u) & 15u;
        } else if (i < 48) {  // H(b,c,d)
            f = b ^ c ^ d;
            g = (3u * i + 5u) & 15u;
        } else {              // I(b,c,d)
            f = c ^ (b | ~d);
            g = (7u * i) & 15u;
        }

        const uint32_t old_d = d;
        d = c;
        c = b;
        b += rotl32(a + f + K[i] + x[g], S[i]);
        a = old_d;
    }

    // 本块结果累加回链接变量
    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
}

std::string MD5::final_hex() {
    // 1) 先固定原始 bit 长度（mod 2^64），后续填充喂入不再影响它
    const uint64_t bitlen = nbytes_ * 8u;

    // 2) 填充：先 0x80，再补 0 至长度 ≡ 56 (mod 64)
    static constexpr uint8_t PAD[64] = {0x80};  // 首字节 0x80，其余全 0
    const std::size_t pad_len = (nbuf_ < 56) ? (56 - nbuf_) : (120 - nbuf_);
    update(PAD, pad_len);

    // 3) 追加 8 字节小端 bit 长度（此时 nbuf_ == 56，喂入后恰好消化一整块）
    uint8_t len_le[8];
    for (unsigned i = 0; i < 8; ++i) {
        len_le[i] = static_cast<uint8_t>(bitlen >> (8u * i));
    }
    update(len_le, 8);

    // 4) A..D 各按小端输出 4 字节（RFC 1321 §3.5）
    uint8_t digest[16];
    for (unsigned i = 0; i < 4; ++i) {
        for (unsigned j = 0; j < 4; ++j) {
            digest[4u * i + j] = static_cast<uint8_t>(h_[i] >> (8u * j));
        }
    }

    // 5) 按契约销毁内部状态：清零，防止残留状态被误复用
    h_.fill(0);
    buf_.fill(0);
    nbytes_ = 0;
    nbuf_ = 0;

    // 6) 转 32 字符小写 hex
    static constexpr char HEXDIG[] = "0123456789abcdef";
    std::string out(32, '\0');
    for (unsigned i = 0; i < 16; ++i) {
        out[2u * i]      = HEXDIG[digest[i] >> 4];
        out[2u * i + 1u] = HEXDIG[digest[i] & 0x0Fu];
    }
    return out;
}
