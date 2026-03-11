import React from "react";
import { shortHex } from "../utils";

/**
 * 详情表格组件 - 将对象数据渲染为 title:value 表格
 * 
 * 特性：
 * - 长字符串（如 hex、二进制）自动截断显示为 前四...后四 格式
 * - 嵌套对象递归格式化为 JSON
 * - 数组格式化为 JSON
 */
interface DetailTableProps {
  data: Record<string, unknown> | null | undefined;
}

export function DetailTable({ data }: DetailTableProps) {
  if (!data || typeof data !== "object") {
    return <div style={{ color: "#6a7d95", padding: "20px 0" }}>无数据</div>;
  }

  const entries = Object.entries(data as Record<string, unknown>);

  if (entries.length === 0) {
    return <div style={{ color: "#6a7d95", padding: "20px 0" }}>空对象</div>;
  }

  return (
    <table className="detail-table">
      <tbody>
        {entries.map(([key, value]) => {
          const { displayValue, title } = formatDetailValue(value);
          return (
            <tr key={key}>
              <td className="detail-key">{key}</td>
              <td className="detail-value" title={title}>{displayValue}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/**
 * 格式化详情字段值
 * 
 * 规则：
 * - 长 hex 字符串显示为 前四...后四
 * - 嵌套对象递归格式化为 JSON
 * - 数组格式化为 JSON
 */
export function formatDetailValue(value: unknown): { displayValue: string; title: string } {
  if (value === null) return { displayValue: "null", title: "null" };
  if (value === undefined) return { displayValue: "undefined", title: "undefined" };

  const type = typeof value;

  if (type === "boolean" || type === "number") {
    const str = String(value);
    return { displayValue: str, title: str };
  }

  if (type === "string") {
    const str = value as string;
    // 长字符串（可能是 hex、txid、hash 等）截断显示
    if (str.length > 20) {
      return { displayValue: shortHex(str, 4), title: str };
    }
    return { displayValue: str, title: str };
  }

  if (type === "object") {
    if (Array.isArray(value)) {
      const json = JSON.stringify(value, null, 2);
      // 数组内容太长时显示摘要
      if (json.length > 100) {
        return { displayValue: `[数组 ${value.length} 项]`, title: json };
      }
      return { displayValue: json, title: json };
    }
    // 嵌套对象
    const json = JSON.stringify(value, null, 2);
    if (json.length > 100) {
      return { displayValue: "{对象}", title: json };
    }
    return { displayValue: json, title: json };
  }

  const str = String(value);
  return { displayValue: str, title: str };
}
