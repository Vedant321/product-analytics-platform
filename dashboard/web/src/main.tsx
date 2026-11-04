import React from "react";
import { createRoot } from "react-dom/client";
import {
  Activity, ArrowDownRight, ArrowRight, ArrowUpRight, BarChart3, CalendarDays,
  ChevronDown, CircleHelp, Database, Download, FlaskConical, Layers3,
  LayoutDashboard, Menu, Search, ShoppingBag, TrendingUp,
} from "lucide-react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, ComposedChart,
  Line, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import "./styles.css";

type Page = "overview" | "performance" | "forecast" | "data";
type Daily = {
  date: string; total_revenue: number; total_purchases: number;
  daily_active_users: number; total_views: number; total_carts: number;
  overall_conversion_rate: number;
};
type Segment = { name: string; revenue: number; purchases: number; units?: number };
type Dashboard = {
  mode: "demo" | "live"; source: string; as_of: string; days: number;
  coverage: { start: string; end: string; days: number };
  daily: Daily[]; previous: Daily[]; brands: Segment[]; categories: Segment[];
};
type Score = { model: string; mae: number | null; status: string };
type Forecast = {
  mode: "demo" | "live"; error?: string; metric: string; history_days: number;
  horizon: number; holdout_days: number; recommended: string; selected: string;
  scores: Score[];
  history: { date: string; value: number }[];
  backtest: { date: string; actual: number; predicted: number }[];
  forecast: { date: string; value: number }[];
  note: string;
};

const fmt = (value: number, kind: "money" | "count" | "percent" = "count", compact = true) => {
  if (kind === "percent") return `${value.toFixed(2)}%`;
  const prefix = kind === "money" ? "$" : "";
  if (compact && Math.abs(value) >= 1_000_000) return `${prefix}${(value / 1_000_000).toFixed(value >= 10_000_000 ? 1 : 2)}M`;
  if (compact && Math.abs(value) >= 100_000) return `${prefix}${(value / 1_000).toFixed(0)}K`;
  return `${prefix}${Math.round(value).toLocaleString()}`;
};
const shortDate = (raw: string) => new Date(`${raw}T00:00:00`).toLocaleDateString("en-US", { month: "short", day: "numeric" });
const fullDate = (raw: string) => new Date(`${raw}T00:00:00`).toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
const sum = (rows: Daily[], key: keyof Daily) => rows.reduce((total, row) => total + Number(row[key] || 0), 0);
const change = (current: number, previous: number) => previous ? (current - previous) / previous * 100 : null;
const chartAxis = { fontSize: 11, fill: "#8a98a8", fontFamily: "DM Sans" };
const chartGrid = "#e9edf1";
const colors = ["#10aa98", "#6784e5", "#efa865", "#b48edf", "#4e9cd3", "#d9889d", "#7bba8e", "#a8b4c7"];

async function api<T>(url: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url);
  } catch {
    throw new Error("The local Python API could not be reached. Stop the current servers and run npm run dev from dashboard/web.");
  }
  const body = await response.text();
  let payload: { detail?: string } & T;
  try {
    payload = JSON.parse(body);
  } catch {
    throw new Error("The local Python API did not return data. Stop the current servers and run npm run dev from dashboard/web.");
  }
  if (!response.ok) throw new Error(payload.detail || "The data service is unavailable.");
  return payload as T;
}

function IconButton({ children, onClick, title }: { children: React.ReactNode; onClick?: () => void; title: string }) {
  return <button className="icon-button" title={title} aria-label={title} onClick={onClick}>{children}</button>;
}

function Badge({ children, tone = "neutral" }: { children: React.ReactNode; tone?: "neutral" | "good" | "warn" }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}

function Panel({ title, eyebrow, action, children, className = "" }: {
  title: string; eyebrow?: string; action?: React.ReactNode; children: React.ReactNode; className?: string;
}) {
  return <section className={`panel ${className}`}>
    <div className="panel-head"><div>{eyebrow && <div className="eyebrow">{eyebrow}</div>}<h3>{title}</h3></div>{action}</div>
    {children}
  </section>;
}

function Kpi({ label, value, previous, icon, tint, help, deltaUnit = "%" }: {
  label: string; value: string; previous?: number | null; icon: React.ReactNode; tint: string; help?: string; deltaUnit?: "%" | "pp";
}) {
  return <div className="kpi">
    <div className="kpi-top"><span>{label}</span><span className="kpi-icon" style={{ background: tint }}>{icon}</span></div>
    <div className="kpi-value">{value}</div>
    <div className="kpi-bottom">
      {previous == null ? <span className="muted">No prior period</span> :
        <span className={`delta ${previous >= 0 ? "positive" : "negative"}`}>
          {previous >= 0 ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />}
          {Math.abs(previous).toFixed(1)}{deltaUnit}
        </span>}
      <span className="muted">{help || "vs. previous period"}</span>
    </div>
  </div>;
}

function Overview({ data, onNavigate }: { data: Dashboard; onNavigate: (page: Page) => void }) {
  const current = data.daily;
  const previous = data.previous.length === current.length ? data.previous : [];
  const revenue = sum(current, "total_revenue");
  const purchases = sum(current, "total_purchases");
  const views = sum(current, "total_views");
  const carts = sum(current, "total_carts");
  const users = current.length ? current.reduce((acc, row) => acc + row.daily_active_users, 0) / current.length : 0;
  const prevRevenue = sum(previous, "total_revenue");
  const prevPurchases = sum(previous, "total_purchases");
  const prevViews = sum(previous, "total_views");
  const prevUsers = previous.length ? sum(previous, "daily_active_users") / previous.length : 0;
  const conversion = views ? purchases / views * 100 : 0;
  const prevConversion = prevViews ? prevPurchases / prevViews * 100 : 0;
  const trend = current.map(row => ({ ...row, label: shortDate(row.date) }));
  const topDay = current.reduce((best, row) => row.total_revenue > best.total_revenue ? row : best, current[0]);
  const categoryTotal = data.categories.reduce((acc, row) => acc + row.revenue, 0);
  const topCategory = data.categories[0];
  const maxBrand = data.brands[0]?.revenue || 1;
  const download = () => {
    const header = "date,total_revenue,total_purchases,daily_active_users,total_views,total_carts,overall_conversion_rate";
    const body = current.map(row => [row.date, row.total_revenue, row.total_purchases, row.daily_active_users, row.total_views, row.total_carts, row.overall_conversion_rate].join(","));
    const link = document.createElement("a");
    link.href = URL.createObjectURL(new Blob([[header, ...body].join("\n")], { type: "text/csv" }));
    link.download = `commerce-daily-${data.days}d.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
  };
  return <>
    <div className="section-heading"><div><div className="eyebrow">PERFORMANCE SNAPSHOT</div><h2>Commerce overview</h2><p>A concise view of revenue, demand, and customer activity.</p></div><button className="ghost-button" onClick={download}><Download size={15} /> Export CSV</button></div>
    <div className="kpi-grid">
      <Kpi label="Revenue" value={fmt(revenue, "money")} previous={change(revenue, prevRevenue)} icon={<TrendingUp size={19} />} tint="#def6ef" />
      <Kpi label="Purchases" value={fmt(purchases)} previous={change(purchases, prevPurchases)} icon={<ShoppingBag size={19} />} tint="#e9eeff" />
      <Kpi label="Avg. daily active users" value={fmt(users)} previous={change(users, prevUsers)} icon={<Activity size={19} />} tint="#fff0df" />
      <Kpi label="View to purchase" value={fmt(conversion, "percent")} previous={prevConversion ? conversion - prevConversion : null} icon={<BarChart3 size={19} />} tint="#f3eafa" deltaUnit="pp" help="vs. previous period" />
    </div>
    <div className="main-grid">
      <Panel title="Revenue over time" eyebrow="DAILY TREND" className="revenue-panel" action={<Badge tone="good">Last {data.days} days</Badge>}>
        <div className="chart-wrap large-chart">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={trend} margin={{ top: 18, right: 8, left: -12, bottom: 0 }}>
              <defs><linearGradient id="revenueFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#0fae9c" stopOpacity={0.19} /><stop offset="100%" stopColor="#0fae9c" stopOpacity={0} /></linearGradient></defs>
              <CartesianGrid vertical={false} stroke={chartGrid} strokeDasharray="3 4" />
              <XAxis dataKey="label" tick={chartAxis} tickLine={false} axisLine={false} minTickGap={36} />
              <YAxis tick={chartAxis} tickLine={false} axisLine={false} tickFormatter={v => fmt(v, "money")} />
              <Tooltip content={<ChartTooltip unit="money" />} />
              <Area type="monotone" dataKey="total_revenue" stroke="#0aa995" strokeWidth={3} fill="url(#revenueFill)" activeDot={{ r: 6, strokeWidth: 3, stroke: "#fff" }} name="Revenue" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="panel-foot"><span>Highest day <strong>{shortDate(topDay.date)}</strong> · {fmt(topDay.total_revenue, "money")}</span><span>Source: Gold daily metrics</span></div>
      </Panel>
      <Panel title="Journey to purchase" eyebrow="EVENT FUNNEL" className="funnel-panel" action={<CircleHelp size={16} className="muted-icon" />}>
        <div className="funnel-list">
          {[
            { label: "Product views", value: views, width: 100, color: "#5b78cc" },
            { label: "Added to cart", value: carts, width: views ? carts / views * 100 : 0, color: "#83a1e5" },
            { label: "Purchases", value: purchases, width: views ? purchases / views * 100 : 0, color: "#11aa99" },
          ].map((row, index) => <div className="funnel-item" key={row.label}>
            <div className="funnel-label"><div><span className="funnel-step">0{index + 1}</span>{row.label}</div><strong>{fmt(row.value)}</strong></div>
            <div className="funnel-track"><div style={{ width: `${Math.max(row.width, 2)}%`, background: row.color }} /></div>
            <span className="funnel-rate">{index === 0 ? "100% of views" : `${row.width.toFixed(1)}% of views`}</span>
          </div>)}
        </div>
        <div className="funnel-note"><span className="note-dot" />Counts are events, not unique people.</div>
      </Panel>
    </div>
    <div className="secondary-grid">
      <Panel title="Purchases & active users" eyebrow="DEMAND SIGNAL">
        <div className="chart-wrap medium-chart"><ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={trend} margin={{ top: 16, right: 8, left: -8, bottom: 0 }}>
            <CartesianGrid vertical={false} stroke={chartGrid} strokeDasharray="3 4" />
            <XAxis dataKey="label" tick={chartAxis} tickLine={false} axisLine={false} minTickGap={36} />
            <YAxis yAxisId="purchases" tick={chartAxis} tickLine={false} axisLine={false} tickFormatter={v => fmt(v)} />
            <YAxis yAxisId="users" orientation="right" tick={chartAxis} tickLine={false} axisLine={false} tickFormatter={v => fmt(v)} />
            <Tooltip content={<ChartTooltip />} />
            <Bar yAxisId="purchases" dataKey="total_purchases" fill="#b9e5dd" radius={[3, 3, 0, 0]} name="Purchases" maxBarSize={18} />
            <Line yAxisId="users" type="monotone" dataKey="daily_active_users" stroke="#536bc3" strokeWidth={2.6} dot={false} name="Active users" />
          </ComposedChart>
        </ResponsiveContainer></div>
        <div className="legend"><span><i style={{ background: "#b9e5dd" }} />Purchases</span><span><i style={{ background: "#536bc3" }} />Active users</span></div>
      </Panel>
      <Panel title="Revenue leaders" eyebrow="ALL-TIME BRANDS" action={<button className="text-button" onClick={() => onNavigate("performance")}>View all <ArrowRight size={14} /></button>}>
        <div className="leader-list">{data.brands.slice(0, 5).map((brand, index) =>
          <div className="leader-row" key={brand.name}><span className="leader-rank">{String(index + 1).padStart(2, "0")}</span><div className="leader-name">{brand.name}<div className="leader-bar"><i style={{ width: `${brand.revenue / maxBrand * 100}%` }} /></div></div><strong>{fmt(brand.revenue, "money")}</strong></div>)}</div>
      </Panel>
    </div>
    <div className="insight-strip"><div className="insight-icon"><BarChart3 size={20} /></div><div><span className="eyebrow">CATEGORY SHARE</span><p><strong>{topCategory?.name || "The top category"}</strong> contributes {categoryTotal ? (topCategory.revenue / categoryTotal * 100).toFixed(1) : "0"}% of all-time category revenue. Explore the full breakdown or compare 7-day forecasts.</p></div><button className="insight-link" onClick={() => onNavigate("forecast")}>Open forecast <ArrowRight size={16} /></button></div>
  </>;
}

function ChartTooltip({ active, payload, label, unit }: { active?: boolean; payload?: Array<{ name: string; value: number; color: string }>; label?: string; unit?: "money" }) {
  if (!active || !payload?.length) return null;
  return <div className="chart-tooltip"><strong>{label}</strong>{payload.filter(p => p.value != null).map((item, index) => <div key={index}><i style={{ background: item.color }} />{item.name}<b>{fmt(item.value, unit === "money" ? "money" : "count", false)}</b></div>)}</div>;
}

function Performance({ data }: { data: Dashboard }) {
  const [segment, setSegment] = React.useState<"categories" | "brands">("categories");
  const [search, setSearch] = React.useState("");
  const rows = segment === "categories" ? data.categories : data.brands;
  const filtered = rows.filter(row => row.name.toLowerCase().includes(search.toLowerCase()));
  const total = rows.reduce((acc, row) => acc + row.revenue, 0);
  const chartRows = rows.slice(0, 8).map(row => ({ ...row, label: row.name.length > 18 ? row.name.slice(0, 17) + "…" : row.name }));
  return <>
    <div className="section-heading"><div><div className="eyebrow">CONTRIBUTION ANALYSIS</div><h2>Performance drivers</h2><p>See which categories and brands account for the most revenue.</p></div><Badge>All-time Gold aggregates</Badge></div>
    <div className="segmented" role="tablist" aria-label="Performance dimension"><button className={segment === "categories" ? "active" : ""} onClick={() => { setSegment("categories"); setSearch(""); }}>Categories</button><button className={segment === "brands" ? "active" : ""} onClick={() => { setSegment("brands"); setSearch(""); }}>Brands</button></div>
    <div className="performance-top">
      <div className="mini-stat"><span>Tracked {segment}</span><strong>{rows.length}</strong><small>Ranked by total revenue</small></div>
      <div className="mini-stat"><span>Top {segment === "brands" ? "brand" : "category"}</span><strong>{rows[0]?.name || "—"}</strong><small>{fmt(rows[0]?.revenue || 0, "money")} revenue</small></div>
      <div className="mini-stat"><span>Top 3 share</span><strong>{total ? (rows.slice(0, 3).reduce((acc, row) => acc + row.revenue, 0) / total * 100).toFixed(1) : "0"}%</strong><small>Of listed {segment} revenue</small></div>
    </div>
    <div className="main-grid performance-grid">
      <Panel title="Revenue ranking" eyebrow={segment.toUpperCase()} className="revenue-panel">
        <div className="chart-wrap ranking-chart"><ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartRows} layout="vertical" margin={{ top: 4, right: 25, left: 12, bottom: 4 }}>
            <CartesianGrid horizontal={false} stroke={chartGrid} strokeDasharray="3 4" />
            <XAxis type="number" tick={chartAxis} tickLine={false} axisLine={false} tickFormatter={v => fmt(v, "money")} />
            <YAxis type="category" dataKey="label" width={105} tick={chartAxis} tickLine={false} axisLine={false} />
            <Tooltip content={<ChartTooltip unit="money" />} />
            <Bar dataKey="revenue" name="Revenue" radius={[0, 5, 5, 0]} maxBarSize={21}>{chartRows.map((row, index) => <Cell key={row.name} fill={colors[index]} />)}</Bar>
          </BarChart>
        </ResponsiveContainer></div>
      </Panel>
      <Panel title="Revenue mix" eyebrow="CONCENTRATION" className="mix-panel">
        <div className="mix-summary"><strong>{fmt(total, "money")}</strong><span>Combined revenue in listed {segment}</span></div>
        <div className="mix-bar">{rows.slice(0, 8).map((row, index) => <div key={row.name} title={`${row.name}: ${fmt(row.revenue, "money")}`} style={{ width: `${total ? row.revenue / total * 100 : 0}%`, background: colors[index] }} />)}</div>
        <div className="mix-legend">{rows.slice(0, 5).map((row, index) => <div key={row.name}><i style={{ background: colors[index] }} />{row.name}<b>{total ? (row.revenue / total * 100).toFixed(1) : "0"}%</b></div>)}</div>
        <div className="source-note">This view uses all-time Gold aggregates. The date filter applies to daily trends and forecasting.</div>
      </Panel>
    </div>
    <Panel title={segment === "categories" ? "Category detail" : "Brand detail"} eyebrow="EXPLORE THE DATA" action={<label className="search-box"><Search size={16} /><input value={search} onChange={event => setSearch(event.target.value)} placeholder={`Search ${segment}`} /></label>}>
      <div className="table-scroll"><table><thead><tr><th>Rank</th><th>{segment === "categories" ? "Category" : "Brand"}</th><th>Revenue</th><th>Purchases</th><th>Revenue share</th></tr></thead><tbody>
        {filtered.map(row => <tr key={row.name}><td className="rank-cell">{String(rows.indexOf(row) + 1).padStart(2, "0")}</td><td><strong>{row.name}</strong></td><td>{fmt(row.revenue, "money", false)}</td><td>{fmt(row.purchases, "count", false)}</td><td><div className="share-cell"><div><i style={{ width: `${total ? row.revenue / total * 100 : 0}%` }} /></div>{total ? (row.revenue / total * 100).toFixed(1) : "0"}%</div></td></tr>)}
      </tbody></table>{filtered.length === 0 && <div className="empty-search">No matching {segment}.</div>}</div>
    </Panel>
  </>;
}

function ForecastView({ data, days }: { data: Dashboard; days: number }) {
  const [metric, setMetric] = React.useState("revenue");
  const [horizon, setHorizon] = React.useState(7);
  const [model, setModel] = React.useState<string | null>(null);
  const [result, setResult] = React.useState<Forecast | null>(null);
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(true);
  React.useEffect(() => {
    let active = true;
    setLoading(true);
    api<Forecast>(`/api/forecast?metric=${metric}&days=${days}&horizon=${horizon}${model ? `&model=${encodeURIComponent(model)}` : ""}`)
      .then(value => { if (active) { setResult(value); setError(""); setLoading(false); } })
      .catch(reason => { if (active) { setError(reason.message); setLoading(false); } });
    return () => { active = false; };
  }, [metric, days, horizon, model]);
  const scores = result?.scores || [];
  const selected = result?.selected;
  const score = scores.find(row => row.model === selected)?.mae;
  const history = result?.history || [];
  const forecast = result?.forecast || [];
  const chartRows = [
    ...history.map(row => ({ date: row.date, label: shortDate(row.date), history: row.value, projected: null as number | null, backtest: null as number | null })),
    ...forecast.map((row, index) => ({ date: row.date, label: shortDate(row.date), history: index === 0 ? history.at(-1)?.value || null : null, projected: row.value, backtest: null })),
  ];
  if (chartRows.length > history.length && history.length) chartRows[history.length - 1].projected = history.at(-1)?.value || null;
  const predictedTotal = forecast.reduce((acc, row) => acc + row.value, 0);
  const displayedPrediction = metric === "users" && forecast.length ? predictedTotal / forecast.length : predictedTotal;
  const metricName = metric === "revenue" ? "Revenue" : metric === "purchases" ? "Purchases" : "Daily active users";
  return <>
    <div className="section-heading"><div><div className="eyebrow">FORECAST LAB</div><h2>Look ahead with context</h2><p>Compare time-series methods on the same holdout period before selecting a forecast.</p></div><Badge tone="good">Gold daily metrics</Badge></div>
    <div className="forecast-controls">
      <label><span>Metric</span><select value={metric} onChange={event => { setMetric(event.target.value); setModel(null); }}><option value="revenue">Revenue</option><option value="purchases">Purchases</option><option value="users">Daily active users</option></select><ChevronDown size={16} /></label>
      <label><span>Forecast horizon</span><select value={horizon} onChange={event => setHorizon(Number(event.target.value))}><option value={7}>Next 7 days</option><option value={14}>Next 14 days</option><option value={28}>Next 28 days</option></select><ChevronDown size={16} /></label>
      <div className="control-explainer"><CalendarDays size={18} /><span>Training on the latest <strong>{days} days</strong> selected in the header.</span></div>
    </div>
    {loading ? <div className="loading-panel">Comparing forecasting methods…</div> : error || result?.error ? <div className="error-panel">{error || result?.error}</div> : result && <>
      <div className="forecast-kpis">
        <div><span>{metric === "users" ? "Projected average daily active users" : `Projected ${horizon}-day ${metricName.toLowerCase()}`}</span><strong>{fmt(displayedPrediction, metric === "revenue" ? "money" : "count")}</strong><small>{metric === "users" ? "Daily average across forecast horizon" : "Sum of selected model forecast"}</small></div>
        <div><span>Selected method</span><strong>{selected}</strong><small>{selected === result.recommended ? "Lowest holdout error" : "Manual selection"}</small></div>
        <div><span>Holdout MAE</span><strong>{score == null ? "—" : fmt(score, metric === "revenue" ? "money" : "count")}</strong><small>Average absolute daily error · {result.holdout_days} days</small></div>
      </div>
      <div className="main-grid forecast-grid">
        <Panel title={`${metricName}: history and outlook`} eyebrow="MODEL OUTPUT" className="revenue-panel" action={<Badge>{horizon}-day horizon</Badge>}>
          <div className="chart-wrap forecast-chart"><ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartRows} margin={{ top: 22, right: 12, left: -8, bottom: 0 }}>
              <CartesianGrid vertical={false} stroke={chartGrid} strokeDasharray="3 4" />
              <XAxis dataKey="label" tick={chartAxis} tickLine={false} axisLine={false} minTickGap={34} />
              <YAxis tick={chartAxis} tickLine={false} axisLine={false} tickFormatter={v => fmt(v, metric === "revenue" ? "money" : "count")} />
              <Tooltip content={<ChartTooltip unit={metric === "revenue" ? "money" : undefined} />} />
              <Area type="monotone" dataKey="history" stroke="#5c78ce" fill="#e9edfa" strokeWidth={2.5} name="Observed" connectNulls />
              <Line type="monotone" dataKey="projected" stroke="#0aa995" strokeWidth={3} strokeDasharray="6 5" dot={false} name="Forecast" connectNulls />
            </ComposedChart>
          </ResponsiveContainer></div>
          <div className="legend"><span><i style={{ background: "#5c78ce" }} />Observed</span><span><i style={{ background: "#0aa995" }} />Forecast</span></div>
        </Panel>
        <Panel title="Model comparison" eyebrow="BACK-TEST RANKING" className="model-panel">
          <p className="panel-description">All methods predict the same last {result.holdout_days} observed days. Lower MAE means a closer fit on that window.</p>
          <div className="model-list">{scores.map(row => <button key={row.model} className={`model-row ${row.model === selected ? "selected" : ""}`} disabled={row.mae == null} onClick={() => setModel(row.model)}>
            <span className="model-indicator" /><span className="model-name">{row.model}<small>{row.mae == null ? row.status : row.model === result.recommended ? "Best holdout score" : "Available"}</small></span><strong>{row.mae == null ? "—" : fmt(row.mae, metric === "revenue" ? "money" : "count")}</strong>
          </button>)}</div>
          <div className="model-foot">MAE = mean absolute error. Scores may change with the selected history window.</div>
        </Panel>
      </div>
      <div className="method-banner"><FlaskConical size={20} /><div><strong>How to read this forecast</strong><p>{result.note} The green line begins after {fullDate(data.as_of)}. It is a projection from the selected history, not an observed result.</p></div></div>
    </>}
  </>;
}

function DataModel({ data }: { data: Dashboard }) {
  return <>
    <div className="section-heading"><div><div className="eyebrow">DATA FOUNDATION</div><h2>From events to decisions</h2><p>The dashboard reads curated Gold tables produced by the Databricks lakehouse.</p></div><Badge>{data.mode === "live" ? "Databricks connected" : "Preview dataset"}</Badge></div>
    <div className="data-stats"><div><span>Daily coverage</span><strong>{data.coverage.days} days</strong><small>{fullDate(data.coverage.start)} – {fullDate(data.coverage.end)}</small></div><div><span>Latest observed date</span><strong>{fullDate(data.as_of)}</strong><small>Historical data, not a live activity feed</small></div><div><span>Dashboard source</span><strong>Gold layer</strong><small>{data.source}</small></div></div>
    <Panel title="Pipeline at a glance" eyebrow="MEDALLION ARCHITECTURE" className="lineage-panel">
      <div className="lineage">
        <div><span className="lineage-icon raw"><Database size={24} /></span><small>01 · INGEST</small><strong>Bronze</strong><p>Raw e-commerce events from batch and streaming sources.</p></div>
        <ArrowRight className="lineage-arrow" size={20} />
        <div><span className="lineage-icon clean"><Layers3 size={24} /></span><small>02 · MODEL</small><strong>Silver</strong><p>Event fact table joined to user, product, category, and date dimensions.</p></div>
        <ArrowRight className="lineage-arrow" size={20} />
        <div><span className="lineage-icon ready"><BarChart3 size={24} /></span><small>03 · SERVE</small><strong>Gold</strong><p>Daily metrics and product/category aggregates used by this interface.</p></div>
      </div>
    </Panel>
    <div className="data-grid">
      {[["gold_daily_metrics", "Time series, funnel counts, KPI cards, forecasts"], ["gold_product_performance", "Brand revenue and purchase rankings"], ["gold_category_performance", "Category revenue and purchase rankings"]].map(([table, description]) =>
        <div className="data-table-card" key={table}><Database size={17} /><div><strong>{table}</strong><p>{description}</p></div><Badge tone="good">Read only</Badge></div>)}
    </div>
    <div className="source-note data-note">The category and brand Gold tables are all-time aggregates; the header history selector affects daily views and forecasting. Preview mode uses generated sample values for UI review.</div>
  </>;
}

function App() {
  const [page, setPage] = React.useState<Page>("overview");
  const [days, setDays] = React.useState(60);
  const [data, setData] = React.useState<Dashboard | null>(null);
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(true);
  const [menuOpen, setMenuOpen] = React.useState(false);
  React.useEffect(() => {
    let active = true;
    setLoading(true);
    api<Dashboard>(`/api/dashboard?days=${days}`)
      .then(payload => { if (active) { setData(payload); setError(""); setLoading(false); } })
      .catch(reason => { if (active) { setError(reason.message); setLoading(false); } });
    return () => { active = false; };
  }, [days]);
  const navigate = (next: Page) => { setPage(next); setMenuOpen(false); window.scrollTo({ top: 0, behavior: "smooth" }); };
  const navItems: { page: Page; label: string; icon: React.ReactNode }[] = [
    { page: "overview", label: "Overview", icon: <LayoutDashboard size={19} /> },
    { page: "performance", label: "Performance", icon: <BarChart3 size={19} /> },
    { page: "forecast", label: "Forecast lab", icon: <TrendingUp size={19} /> },
    { page: "data", label: "Data model", icon: <Layers3 size={19} /> },
  ];
  return <div className="app-shell">
    <aside className={`sidebar ${menuOpen ? "open" : ""}`}>
      <div className="brand"><span className="brand-symbol"><span /><span /><span /></span><div><strong>commerce<span>IQ</span></strong><small>PRODUCT ANALYTICS</small></div></div>
      <div className="sidebar-divider" />
      <div className="nav-label">WORKSPACE</div>
      <nav>{navItems.map(item => <button key={item.page} className={page === item.page ? "active" : ""} onClick={() => navigate(item.page)}>{item.icon}<span>{item.label}</span>{page === item.page && <span className="active-dot" />}</button>)}</nav>
      <div className="sidebar-bottom"><div className="source-card"><div className="source-card-head"><span className="source-status" /> DATA SOURCE</div><strong>Databricks Gold</strong><p>Curated commerce metrics</p></div><div className="sidebar-footer">Product Analytics Platform</div></div>
    </aside>
    {menuOpen && <button className="mobile-overlay" aria-label="Close menu" onClick={() => setMenuOpen(false)} />}
    <main className="content">
      <header className="topbar"><div className="topbar-left"><IconButton title="Open navigation" onClick={() => setMenuOpen(true)}><Menu size={20} /></IconButton><span className="breadcrumb">Analytics <span>/</span> {navItems.find(item => item.page === page)?.label}</span></div><div className="topbar-right">{(page === "overview" || page === "forecast") && <><span className="topbar-period">History</span><div className="period-picker" aria-label="History window">{[30, 60, 90].map(value => <button key={value} onClick={() => setDays(value)} className={days === value ? "active" : ""}>{value}d</button>)}</div><div className="topbar-separator" /></>}<div className="avatar">VS</div></div></header>
      <div className="main-content">
        {data?.mode === "demo" && <div className="demo-banner"><span className="demo-light" /><strong>Sample data</strong><span>This view uses generated records. Restart without DEMO_MODE to query the Databricks Gold tables.</span></div>}
        {data && <div className="page-intro"><div><span className="workspace-label">PRODUCT ANALYTICS / E-COMMERCE</span><h1>Commerce performance<span className="title-accent">.</span></h1><p>Last observed date: {fullDate(data.as_of)} <span className="bullet">•</span> {data.coverage.days} days of available history</p></div><div className="intro-status"><span className="status-pulse" />{data.mode === "live" ? "Connected to Gold layer" : "Preview dataset"}</div></div>}
        {loading && !data ? <div className="loading-panel">Loading commerce data…</div> : error ? <div className="error-panel"><strong>Unable to load dashboard</strong><p>{error}</p><small>For local development, run npm run dev from dashboard/web.</small></div> : data && <>
          {page === "overview" && <Overview data={data} onNavigate={navigate} />}
          {page === "performance" && <Performance data={data} />}
          {page === "forecast" && <ForecastView data={data} days={days} />}
          {page === "data" && <DataModel data={data} />}
        </>}
        <footer className="page-footer"><span>Commerce Intelligence · Product Analytics Platform</span><span>Built on Databricks Gold metrics</span></footer>
      </div>
    </main>
  </div>;
}

createRoot(document.getElementById("root")!).render(<App />);
