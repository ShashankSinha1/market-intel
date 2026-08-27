import { useState, useEffect } from "react"

function App() {
  const [signals, setSignals] = useState({})

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
      ws.onmessage = (event) => {
        const data = JSON.parse(event.data)
        setSignals(prevSignals => ({...prevSignals, [data.product_id]: data}))
      }

      ws.onclose = () => {
        if (!isUnmounting) {
          setTimeout(connect, 3000)
        }
      }
    }
    
    connect()

    return () => {
      isUnmounting = true
      ws.close()
    } 
  }, []);

  return (
    <div>
      <h1>Market Intel Dashboard</h1>
      <table>
        <thead>
          <tr>
          <th>Product</th>
          <th>Price</th>
          <th>Delta</th>
          <th>Average</th>
        </tr>
        </thead>
        <tbody>
          {Object.entries(signals).map(([productId, signal]) => (
            <tr key={productId}>
              <td>{signal.product_id}</td>
              <td>{signal.price.toFixed(2)}</td>
              <td>{signal.delta.toFixed(2)}</td>
              <td>{signal.average.toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default App
