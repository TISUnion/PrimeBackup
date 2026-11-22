---
title: 'CDC 分块'
---

内容定义分块：数据块边界由文件内容决定，而非固定的字节偏移

!!! note "Beta"

    CDC 分块是一项 beta 阶段的功能。
    整体功能已可使用，但算法实现和行为在未来版本中仍可能发生变化。
    不过无须担心，使用 CDC 分块创建的备份数据在未来版本中是保证完全兼容、可正确访问的

## 什么是 CDC

CDC 是 Content-Defined Chunking 的缩写，通常译作“内容定义分块”。
与固定大小分块不同，CDC 通过扫描文件内容，依据滚动哈希指纹来确定数据块边界

由于边界由内容决定，大文件在中间插入、删除少量数据，或在末尾追加内容时，大多数未变区域仍能切出与之前相同的数据块，从而直接复用已有存储并显著提升去重率

CDC 不依赖文件的内部结构，适用于任意位置的插入、删除或原地修改

## FastCDC

Prime Backup 采用的 CDC 算法是 FastCDC，最初由 [USENIX ATC 2016 论文](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)提出，并在 [2020 年的后续论文](https://ieeexplore.ieee.org/document/9055082)中进一步改进

FastCDC 使用轻量级滚动哈希（gear hash），每步仅需一次查表与位移即可逐字节扫描数据，并按位掩码条件切分边界。同时它引入了归一化分块（normalized chunking），在低于目标平均块大小时采用更严格的掩码，超过后改用更宽松的掩码，在保持内容感知特性的前提下让块大小分布更集中于目标均值

Prime Backup 使用基于 Cython 加速的 FastCDC 2020 实现 [`pyfastcdc`](https://github.com/Fallen-Breath/pyfastcdc)，可提供接近原生代码的切块吞吐量

## 可用算法

| 算法           | 平均块大小 | 最小块大小   | 最大块大小   |
|----------------|------------|--------------|--------------|
| `fastcdc_32k`  | 32 KiB     | 8 KiB (/4)   | 256 KiB (*8) |
| `fastcdc_128k` | 128 KiB    | 32 KiB (/4)  | 1 MiB (*8)   |
| `fastcdc_1m`   | 1 MiB      | 256 KiB (/4) | 4 MiB (*4)   |

- `fastcdc_32k` 是粒度最细的 CDC 预设，适合大多数需要 CDC 的场景
- `fastcdc_128k` 采用更粗的粒度，更适合超大型文件（10 GiB 以上），可减少 `fastcdc_32k` 粒度下每条数据块记录带来的相对元数据开销
- `fastcdc_1m` 采用 1 MiB 平均块大小，进一步降低极大型文件的元数据开销，但去重粒度也更粗

这几种算法均使用 FastCDC，启用了 normalized chunking，并固定种子（`0`）以保证可重现性

## 适用场景

只要大多数备份只涉及文件的局部变化，CDC 通常都能发挥效果，例如：

- 体积较大的数据库文件，每次更新仅涉及局部行
- 需要纳入备份、且主要以尾部追加方式写入的大型日志文件
- 任何经常以局部、非全局方式进行修改的大文件

## 不适用场景

以下情况 CDC 通常效果不佳：

- 文件每次保存时都被完整重写（无法保留局部结构）
- 文件是压缩包或加密容器，任何一点内容变化都会导致大范围字节变动

另外，某个文件首次进入备份时，所有数据块仍需完整写入。
CDC 的收益主要体现在后续那些数据块可大量复用的备份上

## 依赖

CDC 分块依赖于可选的 Python 库 [`pyfastcdc`](https://github.com/Fallen-Breath/pyfastcdc)。
你可以单独安装它，也可以安装可选依赖包：

```bash
pip3 install pyfastcdc
# 或一次性安装全部可选依赖
pip3 install -r requirements.optional.txt
```
