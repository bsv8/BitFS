import React from "react";

/**
 * 分页组件
 * 
 * 显示分页控件：上一页/下一页按钮、页码信息、每页条数选择器
 */
interface PagerProps {
  total: number;
  page: number;
  pageSize: number;
  onPage: (p: number) => void;
  onPageSize: (s: number) => void;
}

/**
 * 计算总页数
 */
export function pageCount(total: number, pageSize: number): number {
  return Math.max(1, Math.ceil((total || 0) / pageSize));
}

export function Pager({ total, page, pageSize, onPage, onPageSize }: PagerProps) {
  const pages = pageCount(total, pageSize);
  return (
    <div className="pager">
      <button className="btn btn-light" disabled={page <= 1} onClick={() => onPage(page - 1)}>上一页</button>
      <span>第 {page} / {pages} 页</span>
      <button className="btn btn-light" disabled={page >= pages} onClick={() => onPage(page + 1)}>下一页</button>
      <select className="input small" value={pageSize} onChange={(e) => onPageSize(Number(e.target.value))}>
        {[10, 20, 50, 100].map((s) => <option key={s} value={s}>{s}/页</option>)}
      </select>
      <span>总计 {total}</span>
    </div>
  );
}
