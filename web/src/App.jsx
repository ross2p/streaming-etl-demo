import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { N, T0, initialState, pick, randomEvent, reducer, rnd, toText } from "./engine.js";
import { BatchPanel, Controls, EventsTable, Log, Pipeline, Rich, Windows } from "./components.jsx";

const newGen = () => ({ clock: 1, counter: 0 });

export default function App() {
  const [state, dispatch] = useReducer(reducer, null, initialState);
  const gen = useRef(newGen());
  const stateRef = useRef(state);
  stateRef.current = state;
  const [draft, setDraft] = useState(() => toText(randomEvent(gen.current, [])));
  const [consumerOn, setConsumerOn] = useState(true);
  const [playing, setPlaying] = useState(false);
  const batchRef = useRef(null);
  const live = useRef({});
  live.current = { draft, consumerOn };

  let broken = false;
  try { JSON.parse(draft); } catch { broken = true; }
  const lag = state.msgs.filter((m) => m.state === "wait").length;

  const newDraft = () => randomEvent(gen.current, stateRef.current.msgs);

  const send = useCallback(() => {
    dispatch({ type: "send", text: live.current.draft, consumerOn: live.current.consumerOn });
    setDraft(toText(randomEvent(gen.current, stateRef.current.msgs)));
  }, []);

  useEffect(() => {
    if (!playing) return;
    send();
    const id = setInterval(send, 1300);
    return () => clearInterval(id);
  }, [playing, send]);

  useEffect(() => {
    if (!consumerOn) return;
    const id = setInterval(() => dispatch({ type: "drain" }), 700);
    return () => clearInterval(id);
  }, [consumerOn]);

  useEffect(() => {
    if (state.freshRun && batchRef.current) batchRef.current.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [state.freshRun]);

  const onConsumer = (e) => {
    setConsumerOn(e.target.checked);
    if (e.target.checked && lag) {
      dispatch({
        type: "note",
        narr: N({ b: "Consumer знову працює. " }, "У Kafka накопичилось " + lag + " повідомлень, він обробляє їх по порядку з підтвердженої позиції."),
      });
    }
  };

  const onReset = () => {
    setPlaying(false);
    setConsumerOn(true);
    gen.current = newGen();
    dispatch({ type: "reset" });
    setDraft(toText(randomEvent(gen.current, [])));
  };

  const onPreset = (kind) => {
    const { msgs, maxTime } = stateRef.current;
    const nextId = () => "ev-" + ++gen.current.counter;
    if (kind === "random") return setDraft(toText(newDraft()));
    if (kind === "dup") {
      const ok = msgs.filter((m) => m.ev && m.state === "ok");
      return setDraft(toText(ok.length ? { ...pick(ok).ev } : newDraft()));
    }
    if (kind === "late") {
      return setDraft(toText({
        ...newDraft(), event_id: nextId(), kind: "ride_request", driver_id: null, area: "center",
        event_time: Math.max(T0, maxTime - 20 - rnd(10)),
      }));
    }
    if (kind === "badArea") return setDraft(toText({ ...newDraft(), event_id: nextId(), area: "unknown" }));
    if (kind === "broken") return setDraft(toText(newDraft()).replace(/\}\s*$/, ""));
  };

  const counter = `надіслано ${state.n} · оброблено ${state.processed} · lag ${lag}`;
  const narr = state.narr || N("Відредагуйте JSON у Producer, якщо хочете, і натисніть «Надіслати подію». Або запустіть автозапуск: події генеруються випадково, зі змішаними районами, дублікатами та запізненнями.");

  return (
    <div className="wrap">
      <header>
        <h1>Taxi Streaming ETL</h1>
      </header>
      <Controls
        playing={playing} consumerOn={consumerOn} counter={counter}
        onSend={() => { setPlaying(false); send(); }}
        onPlay={() => setPlaying((p) => !p)}
        onConsumer={onConsumer}
        onBatch={() => dispatch({ type: "batch" })}
        onReset={onReset}
      />
      <div className="narration" aria-live="polite"><Rich parts={narr} /></div>
      <Pipeline
        state={state} consumerOn={consumerOn}
        producerProps={{ draft, broken, onChange: (v) => { setPlaying(false); setDraft(v); }, onPreset }}
      />
      <div className="grid">
        <Log msgs={state.msgs} last={state.last} />
        <Windows db={state.db} changedWin={state.changedWin} />
      </div>
      <BatchPanel ref={batchRef} runs={state.runs} freshRun={state.freshRun} />
      <EventsTable db={state.db} newId={state.newId} />
    </div>
  );
}
