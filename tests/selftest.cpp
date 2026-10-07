// tests/selftest.cpp — common.h 自测（当前仅覆盖大端读写）
// 编译运行：
//   g++ -std=c++17 -Wall -Wextra -Wpedantic -O2 tests/selftest.cpp -o /tmp/selftest && /tmp/selftest
#include "../src/common.h"

#include <cstdint>
#include <cstdio>

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

}  // namespace

int main() {
    test_u16();
    test_u32();
    test_u64();

    if (g_failures == 0) {
        std::printf("selftest: all big-endian checks passed\n");
        return 0;
    }
    std::printf("selftest: %d failure(s)\n", g_failures);
    return 1;
}
