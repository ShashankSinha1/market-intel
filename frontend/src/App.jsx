import { useState, useEffect } from "react"

function App() {
  const [signals, setSignals] = useState({})
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    async function loadSignals() {
      const response = await fetch("http://localhost:8000/api/signals")
      const data = await response.json()
      setSignals(data);
    }
    loadSignals();
  }, []);

  useEffect(() => {
    let isUnmounting = false
    let ws

    function connect() {
      ws = new WebSocket("ws://localhost:8000/ws/signals")

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        if (!isUnmounting) {
          setTimeout(connect, 3000)
        }
      }

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data)
        setSignals(prevSignals => ({...prevSignals, [data.product_id]: data}))
      }
    }

    connect()

    return () => {
      isUnmounting = true
      ws.close()
    }
  }, []);

  const rows = Object.entries(signals).sort(([a], [b]) => a.localeCompare(b))

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 px-4 py-10 sm:px-8">
      <div className="mx-auto max-w-3xl">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-2xl font-semibold tracking-tight text-white sm:text-3xl">
            Market Intel Dashboard
          </h1>
          <div className="flex items-center gap-2 rounded-full border border-slate-800 bg-slate-900/60 px-3 py-1 text-xs font-medium text-slate-300">
            <span
              className={`h-2 w-2 rounded-full ${
                connected ? "bg-emerald-400 animate-pulse" : "bg-red-500"
              }`}
            />
            {connected ? "Live" : "Reconnecting…"}
          </div>
        </div>

        <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-900/60 shadow-xl shadow-black/20">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b border-slate-800 bg-slate-900/80 text-left text-xs uppercase tracking-wider text-slate-400">
                <th className="px-5 py-3 font-medium">Product</th>
                <th className="px-5 py-3 font-medium text-right">Price</th>
                <th className="px-5 py-3 font-medium text-right">Delta</th>
                <th className="px-5 py-3 font-medium text-right">Average</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {rows.map(([productId, signal]) => {
                const delta = signal.delta
                const deltaColor =
                  delta > 0
                    ? "text-emerald-400"
                    : delta < 0
                    ? "text-red-400"
                    : "text-slate-400"

                return (
                  <tr key={productId} className="transition-colors hover:bg-slate-800/40">
                    <td className="px-5 py-3 font-medium text-slate-100">{productId}</td>
                    <td className="px-5 py-3 text-right font-mono tabular-nums text-slate-200">
                      {signal.price.toFixed(2)}
                    </td>
                    <td className={`px-5 py-3 text-right font-mono tabular-nums ${deltaColor}`}>
                      {delta > 0 ? "+" : ""}
                      {delta.toFixed(2)}
                    </td>
                    <td className="px-5 py-3 text-right font-mono tabular-nums text-slate-400">
                      {signal.average.toFixed(2)}
                    </td>
                  </tr>
                )
              })}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-5 py-8 text-center text-slate-500">
                    Waiting for signals…
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default App
