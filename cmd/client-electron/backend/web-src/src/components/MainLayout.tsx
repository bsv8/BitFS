/**
 * MainLayout - 主应用布局组件
 * 
 * 提供统一的侧边栏导航和主内容区域布局。
 * 解锁后显示此布局，内部内容根据路由动态渲染。
 */

import React from "react";
import { setHash, type HashRoute } from "../utils";

interface MainLayoutProps {
  /** 当前路由 */
  route: HashRoute;
  /** 锁定/注销回调 */
  onLock: () => void;
  /** 子内容 */
  children: React.ReactNode;
}

/**
 * 计算当前模块名称
 */
function getModuleName(path: string): string {
  if (path.startsWith("/files")) return "files";
  if (path.startsWith("/settings")) return "settings";
  if (path.startsWith("/direct")) return "direct";
  if (path.startsWith("/live")) return "live";
  if (path.startsWith("/admin")) return "admin";
  if (path.startsWith("/finance")) return "finance";
  return "finance";
}

/**
 * 获取模块标题
 */
function getModuleTitle(moduleName: string): string {
  const titles: Record<string, string> = {
    finance: "💰 资金管理",
    direct: "🔗 Direct 交易",
    live: "📡 Live 直播",
    files: "📁 文件模块",
    admin: "⚙️ 系统管理",
    settings: "🔧 系统设置",
  };
  return titles[moduleName] || "控制台";
}

export function MainLayout({ route, onLock, children }: MainLayoutProps) {
  const moduleName = getModuleName(route.path);
  const routeQueryText = route.query.toString();
  const routeMeta = routeQueryText ? `?${route.query.size} params` : "";

  return (
    <div className="app-shell">
      {/* 侧边栏导航 */}
      <aside className="sidebar">
        <div>
          <p className="eyebrow">BitFS Ops</p>
          <h2>控制台</h2>
        </div>

        {/* 资金管理大项 */}
        <div className="menu-group">
          <h4>💰 资金管理</h4>
          <button
            className={route.path === "/finance" ? "menu active" : "menu"}
            onClick={() => setHash("/finance")}
          >
            资金总览
          </button>
          <button
            className={route.path === "/finance/ledger" ? "menu active" : "menu"}
            onClick={() => setHash("/finance/ledger", new URLSearchParams("page=1&pageSize=20"))}
          >
            链上账本
          </button>
          <button
            className={route.path === "/finance/flows" ? "menu active" : "menu"}
            onClick={() => setHash("/finance/flows", new URLSearchParams("page=1&pageSize=20"))}
          >
            费用池流水
          </button>
          <button
            className={route.path === "/finance/gateway-flows" ? "menu active" : "menu"}
            onClick={() => setHash("/finance/gateway-flows", new URLSearchParams("page=1&pageSize=20"))}
          >
            网关资金流
          </button>
          <button
            className={route.path === "/finance/transfer-pools" ? "menu active" : "menu"}
            onClick={() => setHash("/finance/transfer-pools", new URLSearchParams("page=1&pageSize=20"))}
          >
            Direct资金池
          </button>
        </div>

        {/* Direct 交易模块 */}
        <div className="menu-group">
          <h4>🔗 Direct 交易</h4>
          <button
            className={route.path === "/direct/quotes" ? "menu active" : "menu"}
            onClick={() => setHash("/direct/quotes", new URLSearchParams("page=1&pageSize=20"))}
          >
            报价列表
          </button>
          <button
            className={route.path === "/direct/deals" ? "menu active" : "menu"}
            onClick={() => setHash("/direct/deals", new URLSearchParams("page=1&pageSize=20"))}
          >
            成交记录
          </button>
          <button
            className={route.path === "/direct/sessions" ? "menu active" : "menu"}
            onClick={() => setHash("/direct/sessions", new URLSearchParams("page=1&pageSize=20"))}
          >
            会话管理
          </button>
        </div>

        {/* Live 模块 */}
        <div className="menu-group">
          <h4>📡 Live 直播</h4>
          <button
            className={route.path === "/live/streams" ? "menu active" : "menu"}
            onClick={() => setHash("/live/streams")}
          >
            直播流
          </button>
          <button
            className={route.path === "/live/follow" ? "menu active" : "menu"}
            onClick={() => setHash("/live/follow")}
          >
            我的关注
          </button>
          <button
            className={route.path === "/live/storage" ? "menu active" : "menu"}
            onClick={() => setHash("/live/storage")}
          >
            存储概览
          </button>
        </div>

        {/* 文件模块 */}
        <div className="menu-group">
          <h4>📁 文件</h4>
          <button
            className={route.path === "/files" ? "menu active" : "menu"}
            onClick={() => setHash("/files")}
          >
            模块首页
          </button>
          <button
            className={route.path === "/files/seeds" ? "menu active" : "menu"}
            onClick={() => setHash("/files/seeds", new URLSearchParams("page=1&pageSize=20"))}
          >
            种子列表
          </button>
          <button
            className={route.path === "/files/pricing" ? "menu active" : "menu"}
            onClick={() => setHash("/files/pricing", new URLSearchParams("page=1&pageSize=20"))}
          >
            价格管理
          </button>
          <button
            className={route.path === "/files/sales" ? "menu active" : "menu"}
            onClick={() => setHash("/files/sales", new URLSearchParams("page=1&pageSize=20"))}
          >
            售卖记录
          </button>
          <button
            className={route.path === "/files/index" ? "menu active" : "menu"}
            onClick={() => setHash("/files/index", new URLSearchParams("page=1&pageSize=20"))}
          >
            文件索引
          </button>
        </div>

        {/* Admin 管理模块 */}
        <div className="menu-group">
          <h4>⚙️ 管理</h4>
          <button
            className={route.path === "/admin/orchestrator" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/orchestrator", new URLSearchParams("page=1&pageSize=20"))}
          >
            调度器
          </button>
          <button
            className={route.path === "/admin/client-kernel" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/client-kernel", new URLSearchParams("page=1&pageSize=20"))}
          >
            内核命令
          </button>
          <button
            className={route.path === "/admin/feepool" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/feepool", new URLSearchParams("page=1&pageSize=20"))}
          >
            费用池审计
          </button>
          <button
            className={route.path === "/admin/workspaces" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/workspaces")}
          >
            工作区管理
          </button>
          <button
            className={route.path === "/admin/static" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/static")}
          >
            静态文件
          </button>
          <button
            className={route.path === "/admin/live" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/live")}
          >
            Live 管理
          </button>
          <button
            className={route.path === "/admin/downloads" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/downloads")}
          >
            下载管理
          </button>
          <button
            className={route.path === "/admin/utxos" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/utxos", new URLSearchParams("page=1&pageSize=20"))}
          >
            UTXO 管理
          </button>
          <button
            className={route.path === "/admin/utxo-events" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/utxo-events", new URLSearchParams("page=1&pageSize=20"))}
          >
            UTXO 事件
          </button>
          <button
            className={route.path === "/admin/scheduler-tasks" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/scheduler-tasks")}
          >
            调度器任务
          </button>
          <button
            className={route.path === "/admin/finance-business" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/finance-business", new URLSearchParams("page=1&pageSize=20"))}
          >
            财务业务
          </button>
          <button
            className={route.path === "/admin/finance-breakdown" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/finance-breakdown", new URLSearchParams("page=1&pageSize=20"))}
          >
            财务分解
          </button>
          <button
            className={route.path === "/admin/finance-utxo-links" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/finance-utxo-links", new URLSearchParams("page=1&pageSize=20"))}
          >
            UTXO 关系
          </button>
          <button
            className={route.path === "/admin/config" ? "menu active" : "menu"}
            onClick={() => setHash("/admin/config")}
          >
            系统配置
          </button>
        </div>

        {/* 设置模块 */}
        <div className="menu-group">
          <h4>🔧 设置</h4>
          <button
            className={route.path === "/settings/gateways" ? "menu active" : "menu"}
            onClick={() => setHash("/settings/gateways")}
          >
            网关管理
          </button>
          <button
            className={route.path === "/settings/arbiters" ? "menu active" : "menu"}
            onClick={() => setHash("/settings/arbiters")}
          >
            仲裁服务器
          </button>
        </div>

        <button className="btn btn-ghost" onClick={() => void onLock()}>
          🔒 锁定/注销
        </button>
      </aside>

      {/* 主内容区域 */}
      <main className="content">
        <header className="top">
          <h1>{getModuleTitle(moduleName)}</h1>
          <div
            className="path"
            title={`${route.path}${routeQueryText ? `?${routeQueryText}` : ""}`}
          >
            {route.path}
            {routeMeta}
          </div>
        </header>

        {children}
      </main>
    </div>
  );
}
