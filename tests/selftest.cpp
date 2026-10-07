// tests/selftest.cpp — common.h + md5 自测
// 编译运行：
//   g++ -std=c++17 -Wall -Wextra -Wpedantic -O2 tests/selftest.cpp src/md5.cpp -o /tmp/selftest && /tmp/selftest
#include "../src/common.h"
#include "../src/md5.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

namespace {

int g_failures = 0;

void expect_eq(unsigned long long got, unsigned long long want, const char* what) {
    if (got != want) {
        std::printf("FAIL  %s: got=0x%llX want=0x%llX\n", what, got, want);
        ++g_failures;
    } else {
        std::printf("ok    %s = 0x%llX\n", what, got);
    }
}

void expect_str(const std::string& got, const char* want, const char* what) {
    if (got != want) {
        std::printf("FAIL  %s: got=%s want=%s\n", what, got.c_str(), want);
        ++g_failures;
    } else {
        std::printf("ok    %s = %s\n", what, got.c_str());
    }
}

// ---------- 大端读写 ----------

void test_u16() {
    uint8_t buf[2] = {0xAA, 0xBB};
    be_put16(buf, 0x1234U);
    expect_eq(buf[0], 0x12, "be_put16 -> buf[0]");
    expect_eq(buf[1], 0x34, "be_put16 -> buf[1]");
    expect_eq(be_get16(buf), 0x1234U, "be_get16 round-trip");

    const uint16_t edge[] = {0x0000U, 0x0001U, 0x00FFU, 0xFF00U, 0xFFFFU};
    for (uint16_t v : edge) {
        uint8_t b[2];
        be_put16(b, v);
        expect_eq(be_get16(b), v, "be_get16(be_put16(edge))");
    }
}

void test_u32() {
    uint8_t buf[4] = {0, 0, 0, 0};
    be_put32(buf, 0x12345678U);
    expect_eq(buf[0], 0x12, "be_put32 -> buf[0]");
    expect_eq(buf[1], 0x34, "be_put32 -> buf[1]");
    expect_eq(buf[2], 0x56, "be_put32 -> buf[2]");
    expect_eq(buf[3], 0x78, "be_put32 -> buf[3]");
    expect_eq(be_get32(buf), 0x12345678U, "be_get32 round-trip");

    const uint32_t edge[] = {0x00000000U, 0x00000001U, 0x12345678U,
                             0xFFFFFFFEU, 0xFFFFFFFFU};
    for (uint32_t v : edge) {
        uint8_t b[4];
        be_put32(b, v);
        expect_eq(be_get32(b), v, "be_get32(be_put32(edge))");
    }
}

void test_u64() {
    uint8_t buf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    be_put64(buf, 0x0102030405060708ULL);
    expect_eq(buf[0], 0x01, "be_put64 -> buf[0]");
    expect_eq(buf[1], 0x02, "be_put64 -> buf[1]");
    expect_eq(buf[2], 0x03, "be_put64 -> buf[2]");
    expect_eq(buf[3], 0x04, "be_put64 -> buf[3]");
    expect_eq(buf[4], 0x05, "be_put64 -> buf[4]");
    expect_eq(buf[5], 0x06, "be_put64 -> buf[5]");
    expect_eq(buf[6], 0x07, "be_put64 -> buf[6]");
    expect_eq(buf[7], 0x08, "be_put64 -> buf[7]");
    expect_eq(be_get64(buf), 0x0102030405060708ULL, "be_get64 round-trip");

    const uint64_t edge[] = {0ULL, 1ULL, 0x00000000FFFFFFFFULL,
                             0x8000000000000000ULL, 0xFFFFFFFFFFFFFFFFULL};
    for (uint64_t v : edge) {
        uint8_t b[8];
        be_put64(b, v);
        expect_eq(be_get64(b), v, "be_get64(be_put64(edge))");
    }
}

// ---------- MD5 ----------

// RFC 1321 §A.5 全量测试向量
void test_md5_rfc_vectors() {
    static const struct { const char* msg; const char* hex; } vecs[] = {
        {"", "d41d8cd98f00b204e9800998ecf8427e"},
        {"a", "0cc175b9c0f1b6a831c399e269772661"},
        {"abc", "900150983cd24fb0d6963f7d28e17f72"},
        {"message digest", "f96b697d7cb7938d525a2f31aaf161d0"},
        {"abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b"},
        {"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
         "d174ab98d277d9f5a5611c2c9f419d9f"},
    };
    for (const auto& v : vecs) {
        char label[128];
        std::snprintf(label, sizeof label, "md5(\"%s\")", v.msg);
        MD5 m;
        m.update(v.msg, std::strlen(v.msg));
        expect_str(m.final_hex(), v.hex, label);
    }

    {
        // "1234567890" × 8（程序拼接，避免手抄 80 位出错）
        std::string msg;
        for (int i = 0; i < 8; ++i) msg += "1234567890";
        MD5 m;
        m.update(msg.data(), msg.size());
        expect_str(m.final_hex(), "57edf4a22be3c955ac49da2e2107b67a", "md5(\"1234567890\" x 8)");
    }
}

// 构造后不 update，直接 final_hex（空串立即收尾）
void test_md5_empty_immediate() {
    MD5 m;
    expect_str(m.final_hex(), "d41d8cd98f00b204e9800998ecf8427e",
               "md5 empty, immediate final_hex");
}

// 1e6 个 'a'：分 1000 次喂入，同时压到"部分块结转"路径
void test_md5_million_a() {
    MD5 m;
    const std::string chunk(1000, 'a');
    for (int i = 0; i < 1000; ++i) {
        m.update(chunk.data(), chunk.size());
    }
    expect_str(m.final_hex(), "7707d6ae4e027c70eea2a935c2296f21", "md5(1e6 x 'a')");
}

// 多次 update vs 一次性 update：63 / 64 / 65（不满块 / 恰好一块 / 跨块）
void test_md5_split_equivalence() {
    const std::size_t lens[] = {63, 64, 65};
    for (std::size_t n : lens) {
        // 非均匀内容，避免"全同字节"掩盖索引错误
        std::string data(n, '0');
        for (std::size_t i = 0; i < n; ++i) {
            data[i] = static_cast<char>('0' + static_cast<int>(i % 10));
        }

        MD5 whole;
        whole.update(data.data(), data.size());
        const std::string want = whole.final_hex();

        int bad = 0;

        // 全部两段切分（含 cut = 0 与 cut = n 两个端点）
        for (std::size_t cut = 0; cut <= n; ++cut) {
            MD5 m;
            m.update(data.data(), cut);
            m.update(data.data() + cut, n - cut);
            if (m.final_hex() != want) ++bad;
        }

        // 7 字节一段：切分点逐段漂移，块内 / 跨块混合
        {
            MD5 m;
            for (std::size_t off = 0; off < n; off += 7) {
                const std::size_t take = (n - off < 7) ? (n - off) : 7;
                m.update(data.data() + off, take);
            }
            if (m.final_hex() != want) ++bad;
        }

        if (bad == 0) {
            std::printf("ok    md5 split equivalence n=%zu (%zu two-way cuts + 7B chunks)\n",
                        n, n + 1);
        } else {
            std::printf("FAIL  md5 split equivalence n=%zu: %d mismatch(es)\n", n, bad);
            g_failures += bad;
        }
    }
}

// final_hex 销毁状态后，唯一合法复用路径是 reset()
void test_md5_reset_reuse() {
    MD5 m;
    m.update("abc", 3);
    m.final_hex();  // 状态销毁
    m.reset();
    m.update("abc", 3);
    expect_str(m.final_hex(), "900150983cd24fb0d6963f7d28e17f72", "md5 reset() then reuse");
}

}  // namespace

int main() {
    test_u16();
    test_u32();
    test_u64();

    test_md5_rfc_vectors();
    test_md5_empty_immediate();
    test_md5_million_a();
    test_md5_split_equivalence();
    test_md5_reset_reuse();

    if (g_failures == 0) {
        std::printf("selftest: all checks passed\n");
        return 0;
    }
    std::printf("selftest: %d failure(s)\n", g_failures);
    return 1;
}
