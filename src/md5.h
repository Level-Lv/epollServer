// md5.h — 增量式 MD5（RFC 1321 自实现，无第三方依赖）
// 依据：PRD §6.2（文件 MD5 校验）、架构文档 §16 第 2 步。
// 本模块（.h + .cpp）仅依赖标准库：<array> <cstdint> <cstring> <string> <algorithm>
#pragma once

#include <array>
#include <cstdint>
#include <string>

class MD5 {
public:
    MD5() { reset(); }

    // 复位到初始状态（RFC 1321 §3.3 初始向量）；final_hex 之后再次使用前必须调用
    void reset();

    // 增量喂入数据，可任意次调用；结果与一次性喂入等价（跨块边界由内部缓冲处理）
    void update(const void* data, std::size_t len);

    // 输出 32 字符小写 hex（A..D 各按小端输出，RFC 1321 §3.5）。
    // 契约：调用后内部状态被销毁（清零），不得再 update()/final_hex()；
    //       唯一合法后续操作是 reset() 或析构。
    std::string final_hex();

private:
    void process_block(const uint8_t* block);  // 处理一个 64 字节块

    std::array<uint32_t, 4> h_{};    // A, B, C, D
    std::array<uint8_t, 64> buf_{};  // 未满 64B 的部分块缓冲
    uint64_t nbytes_ = 0;            // 累计喂入字节数（bit 长度 = nbytes_*8 mod 2^64）
    std::size_t nbuf_ = 0;           // buf_ 中当前有效字节数
};
