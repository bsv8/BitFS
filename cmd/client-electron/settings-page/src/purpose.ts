type BillingPurposeSemantic = {
  key: string;
  label: string;
  usedInFilter: boolean;
};

// 设计说明：
// - settings-page 只维护本地用途语义，不反向定义业务协议；
// - key 必须对应真实账务流水 purpose，页面展示和筛选都从这里出；
// - resolver_query_fee 已经不是当前真实流水值，这里只保留 domain_query_fee / domain_resolve_fee。
const billingPurposeSemantics: BillingPurposeSemantic[] = [
  { key: "domain_query_fee", label: "域名查询费", usedInFilter: true },
  { key: "domain_resolve_fee", label: "域名解析费", usedInFilter: true },
  { key: "node_reachability_announce_fee", label: "地址广播费", usedInFilter: true },
  { key: "node_reachability_query_fee", label: "地址查询费", usedInFilter: true },
  { key: "listen_cycle_fee", label: "监听周期费", usedInFilter: true },
  { key: "fee_pool_open", label: "费用池开启", usedInFilter: true },
  { key: "fee_pool_close", label: "费用池关闭", usedInFilter: true },
  { key: "fee_pool_suspicious_expiry_settle", label: "费用池异常结算", usedInFilter: true },
  { key: "demand_publish_fee", label: "需求发布费", usedInFilter: true },
  { key: "demand_publish_batch_fee", label: "批量需求发布费", usedInFilter: true },
  { key: "live_demand_publish_fee", label: "直播需求发布费", usedInFilter: true },
  { key: "direct_transfer_pool_open", label: "内容池开启", usedInFilter: true },
  { key: "prepare_exact_pool_amount", label: "内容池补足", usedInFilter: true },
  { key: "direct_transfer_chunk_pay", label: "内容传输费", usedInFilter: true },
  { key: "direct_transfer_pool_close", label: "内容池结算", usedInFilter: true },
  { key: "peer_call_fee", label: "节点调用费", usedInFilter: true },
  { key: "domain_register_total_payment", label: "域名注册总支付", usedInFilter: true }
];

const billingPurposeMap = new Map(billingPurposeSemantics.map((item) => [item.key, item]));

export const billingPurposeOptions = billingPurposeSemantics
  .filter((item) => item.usedInFilter)
  .map((item) => ({ value: item.key, label: item.label }));

export const defaultPurposePlaceholder = billingPurposeOptions[0]?.value || "domain_query_fee";

export function formatPurposeLabel(purpose: string): string {
  const normalized = String(purpose || "").trim();
  if (normalized === "") {
    return "-";
  }
  return billingPurposeMap.get(normalized)?.label || normalized;
}
