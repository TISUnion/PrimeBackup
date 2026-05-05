---
title: 'CDC 分块'
---

内容定义分块：数据块边界由文件内容决定，而非固定的字节偏移

## 什么是 CDC

CDC 是 Content-Defined Chunking 的缩写，通常译作"内容定义分块"。
与固定大小分块不同，CDC 通过扫描文件内容，利用滚动哈希指纹来识别数据块边界

由于边界由内容决定，当大文件仅发生局部变化时——例如在中间插入或删除了少量数据，
或仅在尾部追加内容——大量未变化的内容仍会被切分成与之前相同的数据块。
这些相同的数据块可直接复用已有存储，显著提升去重效果

CDC 对文件的内部结构没有任何假设。
它对任意类型的局部修改均有效：无论是任意位置的插入、删除，还是原地覆盖写入

## FastCDC

FastCDC 是实现 CDC 的一种具体算法，也是 Prime Backup 所采用的方案。
它最初由 2016 年 USENIX ATC 会议上发表的[一篇研究论文](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)提出，
于 [2020 年的后续论文](https://ieeexplore.ieee.org/document/9055082)中进一步完善

FastCDC 的核心是 gear hash（齿轮哈希）——一种轻量级的滚动哈希，逐字节扫描数据，
每步仅需一次查表操作与一次位移运算——通过对哈希值施加位掩码条件来检测分块边界

FastCDC 区别于早期 CDC 算法的核心特性在于其 normalized chunking（归一化分块）技术。
它不采用统一的哈希掩码，而是在当前位置低于目标平均块大小时使用更严格的掩码，超过后则切换为更宽松的掩码，
使分块大小分布向目标平均值紧密收敛，同时完整保留了 CDC 的内容自适应特性

Prime Backup 使用 [`pyfastcdc`](https://github.com/Fallen-Breath/pyfastcdc)，这是一个基于 Cython 加速的 FastCDC 2020 Python 实现，
可达到接近原生代码的分块处理吞吐量

## 可用算法

| 算法             | 平均块大小   | 最小块大小  | 最大块大小   |
|----------------|---------|--------|---------|
| `fastcdc_32k`  | 32 KiB  | 8 KiB  | 256 KiB |
| `fastcdc_128k` | 128 KiB | 64 KiB | 1 MiB   |

`fastcdc_32k` 是默认选项，适合大多数使用场景。
`fastcdc_128k` 采用更粗的粒度，更适合超大型文件（10 GiB 以上），可减少 `fastcdc_32k` 粒度下每条数据块记录带来的相对元数据开销

两种算法均使用 FastCDC，启用了 normalized chunking，并固定种子（`0`）以保证可重现性

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
