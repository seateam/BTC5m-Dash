/**
 * 链上成交份额校准
 *
 * 通过解析 Polygon 交易 receipt 里的 CTF TransferSingle 事件，
 * 获取买入/卖出的真实成交份额（链上真相）。
 *
 * 用途：修正 Polymarket UserWS 推送的 size 偏差（买入时 WS 报的份额
 * 和链上真实份额有约 1% 差距）。
 *
 * 实测延迟：300-700ms（dRPC），比 REST API 轮询快约 5 秒。
 */

import { ethers } from "ethers";

// ERC-1155 TransferSingle 事件 topic
const TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";

// 公共 Polygon RPC（按实测延迟排序）
const RPCS = [
  "https://polygon.drpc.org",                    // 最快 ~200ms
  "https://polygon-bor-rpc.publicnode.com",      // 备用 1
  "https://1rpc.io/matic",                       // 备用 2
];

const QUERY_TIMEOUT_MS = 3000;
// 用完整 Network 对象 + staticNetwork 对象版本，避免 ethers v6 构造时仍触发 eth_chainId 探测
const POLYGON_NETWORK = new ethers.Network("polygon", 137);

// 复用 provider 实例，避免每次创建都触发网络探测
const providerCache = new Map<string, ethers.JsonRpcProvider>();

function getProvider(url: string): ethers.JsonRpcProvider {
  let p = providerCache.get(url);
  if (!p) {
    // 传 Network 对象给 staticNetwork，ethers 跳过启动时的 eth_chainId 探测
    p = new ethers.JsonRpcProvider(url, POLYGON_NETWORK, { staticNetwork: POLYGON_NETWORK });
    // 静默 error 事件（ethers v6 默认会抛出来，我们自己用 catch 处理）
    p.on("error", () => { /* 忽略，由调用方处理 */ });
    providerCache.set(url, p);
  }
  return p;
}

/**
 * 查询一个 RPC 的 tx receipt（带超时）
 */
async function queryReceipt(url: string, txHash: string): Promise<ethers.TransactionReceipt | null> {
  try {
    const rpc = getProvider(url);
    return await Promise.race([
      rpc.getTransactionReceipt(txHash),
      new Promise<null>((_, reject) =>
        setTimeout(() => reject(new Error(`timeout ${QUERY_TIMEOUT_MS}ms`)), QUERY_TIMEOUT_MS),
      ),
    ]);
  } catch {
    return null;
  }
}

/**
 * 解析一个 tx 里涉及 Proxy 的 CTF TransferSingle 事件
 * @returns 净成交份额（正数=买入/流入，负数=卖出/流出）；
 *          null 表示所有 RPC 都查询失败，应降级到 REST 兜底
 */
export async function getRealFillFromTx(
  txHash: string,
  proxy: string,
): Promise<number | null> {
  if (!txHash || !proxy) return null;

  const proxyPadded = ethers.zeroPadValue(proxy.toLowerCase(), 32);

  for (const url of RPCS) {
    const receipt = await queryReceipt(url, txHash);
    if (!receipt) continue;

    // tx 失败（理论上 UserWS MINED 不会失败，但防御性处理）
    if (receipt.status !== 1) return 0;

    let totalIn = 0n;
    let totalOut = 0n;

    for (const log of receipt.logs) {
      if (log.topics[0] !== TRANSFER_SINGLE) continue;
      // TransferSingle(operator, from, to, id, value)
      //   topics[0] = event sig
      //   topics[1] = operator
      //   topics[2] = from
      //   topics[3] = to
      //   data      = id (32 bytes) + value (32 bytes)
      if (log.topics.length < 4) continue;
      const value = BigInt("0x" + log.data.slice(66));
      const isIn = log.topics[3].toLowerCase() === proxyPadded.toLowerCase();
      const isOut = log.topics[2].toLowerCase() === proxyPadded.toLowerCase();
      if (isIn) totalIn += value;
      else if (isOut) totalOut += value;
    }

    // CTF 份额是 6 位小数（和 USDC 一样）
    return Number(totalIn - totalOut) / 1e6;
  }

  return null;  // 所有 RPC 都失败
}
