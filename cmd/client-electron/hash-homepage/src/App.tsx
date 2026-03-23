import orbitMarkURL from "./assets/orbit-mark.svg";

const quickLinks = [
  { title: "打开种子页", desc: "直接输入主页 hash 或收藏中的资源 hash。" },
  { title: "继续下载", desc: "恢复上次浏览时挂起的静态资源购买。" },
  { title: "查看缓存", desc: "把本地已命中的页面、样式、图片整理出来。" }
];

const newsCards = [
  { title: "Hash 首页也要内容寻址", desc: "主页本身就是一组 hash 文件，和普通站点的目录结构完全脱钩。" },
  { title: "普通浏览器也能预览", desc: "构建产物虽然没有后缀，但预览服务会按 manifest 回正确的 MIME。" },
  { title: "后续可接入真实 bitfs://", desc: "等 Electron 协议层接好后，这组文件就能直接进入真实购买与缓存链路。" }
];

export default function App() {
  return (
    <div className="page-shell">
      <header className="hero">
        <div className="brand-row">
          <img className="brand-mark" src={orbitMarkURL} alt="BitFS" />
          <span className="brand-pill">BitFS Hash Browser</span>
        </div>
        <h1>进入只信任 hash 的内容世界</h1>
        <p className="hero-copy">
          这是一张随客户端一起分发的本地首页。页面自己的 HTML、JS、CSS 和图片都会在构建后改写成裸 hash 文件，
          既能适配哈希浏览器，也能用普通浏览器做本地预览。
        </p>
        <form className="search-panel">
          <label className="sr-only" htmlFor="hash-input">输入资源 hash</label>
          <input id="hash-input" className="hash-input" placeholder="输入 64 位资源 hash，准备跳转到浏览器地址栏" />
          <button type="button" className="search-button">打开资源</button>
        </form>
      </header>

      <main className="content-grid">
        <section className="card-grid">
          {quickLinks.map((item) => (
            <article key={item.title} className="info-card">
              <h2>{item.title}</h2>
              <p>{item.desc}</p>
            </article>
          ))}
        </section>

        <section className="news-panel">
          <div className="section-head">
            <span>今日起始页</span>
            <strong>Hash Bulletin</strong>
          </div>
          <div className="news-list">
            {newsCards.map((item) => (
              <article key={item.title} className="news-card">
                <h3>{item.title}</h3>
                <p>{item.desc}</p>
              </article>
            ))}
          </div>
        </section>
      </main>
    </div>
  );
}
