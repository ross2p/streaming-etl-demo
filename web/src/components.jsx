import { forwardRef } from "react";
import { rel, result } from "./engine.js";

export const Rich = ({ parts }) =>
  parts.map((p, i) => (typeof p === "string" ? p : <strong key={i}>{p.b}</strong>));

const cellClass = (m) => (m.state === "wait" ? "" : m.state === "ok" ? "done" : m.state);

export function Controls({ playing, consumerOn, counter, onSend, onPlay, onConsumer, onBatch, onReset }) {
  return (
    <div className="controls">
      <button className="primary" onClick={onSend}>Надіслати подію</button>
      <button onClick={onPlay}>{playing ? "Пауза" : "Автозапуск випадкових"}</button>
      <button onClick={onBatch}>Batch example</button>
      <label className="toggle">
        <input type="checkbox" checked={consumerOn} onChange={onConsumer} /> Consumer працює
      </label>
      <button onClick={onReset}>Спочатку</button>
      <span className="counter">{counter}</span>
    </div>
  );
}

function Producer({ draft, broken, onChange, onPreset }) {
  return (
    <div className="node">
      <h3>Producer</h3>
      <label className="sub" htmlFor="draft">Наступне повідомлення (можна редагувати)</label>
      <textarea
        id="draft"
        className={broken ? "broken" : ""}
        spellCheck={false}
        value={draft}
        onChange={(e) => onChange(e.target.value)}
        aria-describedby="draftHint"
      />
      <div className="presets">
        <button className="small" onClick={() => onPreset("random")}>Нова випадкова</button>
        <button className="small" onClick={() => onPreset("dup")}>Дублікат</button>
        <button className="small" onClick={() => onPreset("late")}>Запізніла</button>
        <button className="small" onClick={() => onPreset("badArea")}>Невідомий район</button>
        <button className="small" onClick={() => onPreset("broken")}>Зламаний JSON</button>
      </div>
      <div className="hint" id="draftHint">
        event_time це Unix-час. Вікно = event_time ÷ 30 с, округлене вниз. Ключ Kafka = район.
      </div>
    </div>
  );
}

function Kafka({ msgs, sentN }) {
  return (
    <div className="node">
      <h3>Kafka topic <span className="mono">taxi-defense-1</span></h3>
      <div className="sub">ключ = район, 3 партиції</div>
      <div className="lanes">
        {[0, 1, 2].map((p) => {
          const mine = msgs.filter((m) => m.p === p);
          const shown = mine.slice(-18);
          const hidden = mine.length - shown.length;
          const lag = mine.filter((m) => m.state === "wait").length;
          return (
            <div className="lane" key={p}>
              <span className="pl">P{p}</span>
              <div className="cells">
                {hidden > 0 && <span className="mono" style={{ color: "var(--muted)" }}>+{hidden}</span>}
                {shown.map((m) => (
                  <div key={m.n} className={"cell " + cellClass(m) + (m.n === sentN ? " new" : "")} title={"offset " + m.off}>
                    {m.n}
                  </div>
                ))}
                {mine.length === 0 && <span style={{ fontSize: 12, color: "var(--muted)" }}>порожньо</span>}
              </div>
              <span className={"lag " + (lag > 0 ? "hot" : "")}>lag {lag}</span>
            </div>
          );
        })}
      </div>
      <div className="legend">
        <span>пунктир: чекає в Kafka</span><span>зелений: збережено</span>
        <span>жовтий: дублікат</span><span>червоний: відхилено</span>
      </div>
    </div>
  );
}

function Consumer({ on, last }) {
  return (
    <div className={"node" + (on && last ? " active" : "")}>
      <h3>Consumer {!on && <span className="mono off">зупинений</span>}</h3>
      <div className="stages">
        <div className="stage"><div className="k">Extract</div><div className="v">{last ? last.E : "—"}</div></div>
        <div className={"stage" + (last && last.stop === "T" ? " stop" : "")}>
          <div className="k">Transform</div><div className="v">{last ? last.T : "—"}</div>
        </div>
        <div className={"stage" + (last && last.stop === "L" ? " stop" : "")}>
          <div className="k">Load</div><div className="v">{last && last.L ? last.L : "—"}</div>
        </div>
      </div>
    </div>
  );
}

export function Pipeline({ state, consumerOn, producerProps }) {
  return (
    <section aria-label="Конвеєр">
      <div className="pipe">
        <Producer {...producerProps} />
        <div className="arrow">→</div>
        <Kafka msgs={state.msgs} sentN={state.sentN} />
        <div className="arrow">→</div>
        <Consumer on={consumerOn} last={state.last} />
        <div className="arrow">→</div>
        <div className="node">
          <h3>PostgreSQL</h3>
          <div className="sub mono">taxi_events</div>
          <div style={{ fontSize: 28, fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>{state.db.length}</div>
          <div className="sub">рядків збережено</div>
        </div>
      </div>
    </section>
  );
}

export function Log({ msgs, last }) {
  return (
    <section className="panel">
      <h2>Повідомлення (нові зверху)</h2>
      <div className="log">
        {msgs.length === 0 && <div className="empty">Повідомлень ще немає.</div>}
        {msgs.slice().reverse().slice(0, 80).map((m) => {
          const e = m.ev || {};
          let label;
          if (m.parseErr) label = "некоректний JSON";
          else {
            const who = e.kind === "driver_available" ? "водій " + (e.driver_id || "?") : e.kind === "ride_request" ? "запит" : "тип «" + e.kind + "»";
            label = who + " · " + (typeof e.area === "string" ? e.area.trim().toLowerCase() : "?") + " · " + (Number.isInteger(e.event_time) ? rel(e.event_time) : "?");
          }
          const chip =
            m.state === "ok" ? <span className="chip ok">збережено</span>
            : m.state === "dup" ? <span className="chip dup">DUPLICATE</span>
            : m.state === "bad" ? <span className="chip bad">INVALID</span>
            : <span className="chip wait">у Kafka</span>;
          return (
            <div key={m.n} className={"row" + (last && last.n === m.n ? " current" : "")}>
              <span className="n">#{m.n}</span>
              <span className="mono">{e.kind === "ride_request" ? "REQ" : e.kind === "driver_available" ? "DRV" : "—"}</span>
              <span>{label}</span>
              <span>{chip}{m.late && <span className="chip late">запізніла</span>}</span>
            </div>
          );
        })}
      </div>
    </section>
  );
}

export function Windows({ db, changedWin }) {
  const keys = [...new Set(db.map((r) => r.area + "|" + r.wstart))]
    .sort((a, b) => b.split("|")[1] - a.split("|")[1] || a.localeCompare(b))
    .slice(0, 8);
  return (
    <section className="panel">
      <h2>Показники вікон (30 с)</h2>
      <div className="wins">
        {keys.length === 0 && <div className="empty">Вікон ще немає. Вони з’являються після першої збереженої події.</div>}
        {keys.map((k) => {
          const [area, ws] = k.split("|");
          const r = result(db, area, +ws);
          return (
            <div key={k} className={"win" + (changedWin === k ? " flash" : "")}>
              <div className="head"><b>{area}</b><span>вікно {rel(+ws)}–{rel(+ws + 30)} UTC</span></div>
              <div className="nums">
                <div><small>запити</small><strong>{r.req}</strong></div>
                <div><small>різні водії</small><strong>{r.drv}</strong></div>
                <div><small>коеф. · ціна</small><strong>{r.mult ? "×" + r.mult.toFixed(2) + " · " + r.price : "немає"}</strong></div>
              </div>
              {r.mult ? (
                <div>
                  <div className="meter"><i style={{ width: ((r.mult - 1) / 2) * 100 + "%" }} /></div>
                  <div className="meter-scale"><span>×1</span><span>×2</span><span>×3 (межа)</span></div>
                </div>
              ) : (
                <div className="empty">Немає водіїв у цьому вікні, тому ціну не пропонуємо.</div>
              )}
            </div>
          );
        })}
      </div>
      <p className="formula" style={{ marginTop: 12 }}>
        коефіцієнт = clamp(запити ÷ водії, 1, 3) · ціна = 100 грн × коефіцієнт · 0 водіїв → ціни немає
      </p>
    </section>
  );
}

function BatchRun({ run, fresh }) {
  const empty = (n) => <tr><td colSpan={n} style={{ color: "var(--muted)" }}>—</td></tr>;
  return (
    <div className={"run" + (fresh ? " fresh" : "")}>
      <div className="head">
        <b>Batch #{run.id}</b>
        <span>події {rel(run.since)}–{rel(run.until)} · прочитано {run.events} з {run.total}</span>
      </div>
      <div className="versus">
        <div>Стрімінг (consumer)<b>{run.total} INSERT</b>по одній події, одразу</div>
        <div>Batch (цей запуск)<b>1 SELECT + 1 транзакція</b>{run.events} подій → {run.stats.length + run.areas.length} рядків аналітики</div>
      </div>
      <div className="tablewrap">
        <h4>window_stats</h4>
        <table>
          <thead><tr><th>window_start</th><th>area</th><th>requests</th><th>drivers</th><th>multiplier</th><th>price</th></tr></thead>
          <tbody>
            {run.stats.length ? run.stats.map((s) => (
              <tr key={s.area + s.wstart}>
                <td>{rel(s.wstart)}</td><td>{s.area}</td><td>{s.req}</td><td>{s.drv}</td>
                <td>{s.mult ? "×" + s.mult.toFixed(2) : "—"}</td><td>{s.price ? s.price.toFixed(2) : "—"}</td>
              </tr>
            )) : empty(6)}
          </tbody>
        </table>
      </div>
      <div className="tablewrap">
        <h4>area_summary</h4>
        <table>
          <thead><tr><th>area</th><th>requests</th><th>drivers</th><th>peak_multiplier</th><th>peak_price</th></tr></thead>
          <tbody>
            {run.areas.length ? run.areas.map((a) => (
              <tr key={a.area}>
                <td>{a.area}</td><td>{a.req}</td><td>{a.drv}</td>
                <td>{a.peak ? "×" + a.peak.toFixed(2) : "—"}</td><td>{a.peakPrice ? a.peakPrice.toFixed(2) : "—"}</td>
              </tr>
            )) : empty(5)}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export const BatchPanel = forwardRef(function BatchPanel({ runs, freshRun }, ref) {
  return (
    <section className="panel" ref={ref}>
      <h2>Batch: БД taxi_analytics</h2>
      <p className="hint" style={{ marginBottom: 12 }}>
        Кнопка «Batch example» одним запитом бере з taxi_events усі події за останні 2 хвилини (рахуючи від найновішої
        події) і однією транзакцією записує аналітику в окрему базу. Це робить <span className="mono">batch_job.py</span>.
        Стрімінг-consumer при цьому нічого не знає про цю базу.
      </p>
      {runs.length
        ? runs.map((r) => <BatchRun key={r.id} run={r} fresh={r.id === freshRun} />)
        : <div className="empty">Запусків ще не було. Накопичте кілька подій і натисніть «Batch example».</div>}
    </section>
  );
});

export function EventsTable({ db, newId }) {
  return (
    <section className="panel">
      <h2>Таблиця taxi_events (нові зверху)</h2>
      <div className="tablewrap">
        <table>
          <thead><tr><th>event_id</th><th>event_time</th><th>area</th><th>kind</th><th>driver_id</th><th>window_start</th></tr></thead>
          <tbody>
            {db.length === 0 && <tr><td colSpan={6} style={{ color: "var(--muted)" }}>порожньо</td></tr>}
            {db.slice().reverse().slice(0, 60).map((r) => (
              <tr key={r.id} className={r.id === newId ? "newrow" : ""}>
                <td>{r.id}</td><td>{rel(r.time)}</td><td>{r.area}</td><td>{r.kind}</td><td>{r.driver || "—"}</td><td>{rel(r.wstart)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
