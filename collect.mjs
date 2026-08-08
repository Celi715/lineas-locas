// Pega al feed GTFS-realtime de colectivos del GBA y guarda, para cada
// vehículo, un buffer circular de hasta MAX_POINTS posiciones en Postgres
// (Supabase). GitHub Actions no permite crons de menos de 5 minutos (uno de
// cada 1 minuto simplemente no se ejecuta, sin avisar), así que esta corrida
// hace varios polls espaciados por POLL_GAP_MS para cubrir esa ventana de
// 5 minutos con ~30s de granularidad.
//
// El slot dentro del buffer circular se calcula por bloque de tiempo de
// nuestro propio reloj al consultar (no del timestamp que reporta cada
// vehículo: si su GPS no actualiza tan seguido como cada 30s, dos polls
// nuestros verían el mismo timestamp reportado y se pisarían en vez de
// generar dos puntos). Un solo INSERT multi-fila por poll alcanza, sin
// necesidad de leer nada antes de escribir.

import protobuf from "protobufjs"
import pg from "pg"
import path from "node:path"
import { fileURLToPath } from "node:url"

const { Client } = pg
const __dirname = path.dirname(fileURLToPath(import.meta.url))

const GBA_BBOX = { minLat: -35.74, maxLat: -34.07, minLon: -59.58, maxLon: -57.95 }
const MAX_POINTS = 100
const POLL_INTERVAL_S = 30
// 9 polls * 30s de pausa = 4 min de ventana cubierta, con margen antes de
// que dispare la próxima corrida programada (cada 5 min, el mínimo real de
// GitHub Actions).
const POLL_COUNT = 9
const POLL_GAP_MS = 30_000
const INSERT_CHUNK_SIZE = 1000
// Un colectivo que se apaga (vuelve a cochera) deja de aparecer en el feed.
// Si no se vio en más de esto, se borra su historial entero al terminar la
// corrida, para que el próximo viaje arranque con el buffer limpio en vez de
// arrastrar puntos del recorrido anterior.
const STALE_THRESHOLD_S = 5 * 60

function requireEnv(name) {
  const value = process.env[name]
  if (!value) throw new Error(`Falta la variable de entorno ${name}`)
  return value
}

let feedMessageType = null
async function getFeedMessageType() {
  if (feedMessageType) return feedMessageType
  const root = await protobuf.load(path.join(__dirname, "gtfs-realtime.proto"))
  feedMessageType = root.lookupType("transit_realtime.FeedMessage")
  return feedMessageType
}

async function fetchVehicles(pollTime) {
  const clientId = requireEnv("BA_TRANSPORTE_CLIENT_ID")
  const clientSecret = requireEnv("BA_TRANSPORTE_CLIENT_SECRET")

  const url = `https://apitransporte.buenosaires.gob.ar/colectivos/vehiclePositions?client_id=${clientId}&client_secret=${clientSecret}`
  const res = await fetch(url, { signal: AbortSignal.timeout(15000) })
  if (!res.ok) throw new Error(`API de Transporte respondió ${res.status}`)

  const FeedMessage = await getFeedMessageType()
  const msg = FeedMessage.decode(new Uint8Array(await res.arrayBuffer()))
  const obj = FeedMessage.toObject(msg, { longs: String, defaults: true })

  const slot = Math.floor(pollTime / POLL_INTERVAL_S) % MAX_POINTS
  const vehicles = []
  for (const entity of obj.entity || []) {
    const v = entity.vehicle
    if (!v || !v.position || !v.vehicle?.id) continue
    const lat = v.position.latitude
    const lon = v.position.longitude
    if (lat < GBA_BBOX.minLat || lat > GBA_BBOX.maxLat || lon < GBA_BBOX.minLon || lon > GBA_BBOX.maxLon) continue
    const label = v.vehicle.label || ""
    const m = label.match(/^(\d+)-(.+)$/)
    const ts = Number(v.timestamp) || pollTime
    vehicles.push({
      vehicleId: v.vehicle.id,
      interno: m ? m[1] : null,
      lat,
      lon,
      ts,
      slot,
    })
  }
  return vehicles
}

async function savePositions(client, vehicles) {
  for (let i = 0; i < vehicles.length; i += INSERT_CHUNK_SIZE) {
    const chunk = vehicles.slice(i, i + INSERT_CHUNK_SIZE)
    const values = []
    const params = []
    chunk.forEach((v, idx) => {
      const base = idx * 6
      values.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`)
      params.push(v.vehicleId, v.slot, v.lat, v.lon, v.ts, v.interno)
    })

    await client.query(
      `INSERT INTO vehicle_positions (vehicle_id, slot, lat, lon, ts, interno)
       VALUES ${values.join(", ")}
       ON CONFLICT (vehicle_id, slot) DO UPDATE SET
         lat = EXCLUDED.lat, lon = EXCLUDED.lon, ts = EXCLUDED.ts, interno = EXCLUDED.interno`,
      params
    )
  }
}

async function pollOnce(client, n) {
  const pollTime = Math.floor(Date.now() / 1000)
  const vehicles = await fetchVehicles(pollTime)
  await savePositions(client, vehicles)
  console.log(`poll ${n}/${POLL_COUNT}: ${vehicles.length} vehiculos guardados`)
}

async function cleanupStaleVehicles(client) {
  const threshold = Math.floor(Date.now() / 1000) - STALE_THRESHOLD_S
  const res = await client.query(
    `DELETE FROM vehicle_positions
     WHERE vehicle_id IN (
       SELECT vehicle_id FROM vehicle_positions GROUP BY vehicle_id HAVING MAX(ts) < $1
     )`,
    [threshold]
  )
  if (res.rowCount) console.log(`cleanup: ${res.rowCount} puntos borrados de vehiculos inactivos`)
}

async function main() {
  const client = new Client({
    connectionString: requireEnv("DATABASE_URL"),
    ssl: { rejectUnauthorized: false },
  })
  await client.connect()
  try {
    for (let i = 1; i <= POLL_COUNT; i++) {
      await pollOnce(client, i)
      if (i < POLL_COUNT) await new Promise((resolve) => setTimeout(resolve, POLL_GAP_MS))
    }
    await cleanupStaleVehicles(client)
  } finally {
    await client.end()
  }
}

main().catch((err) => {
  console.error(err.message || err)
  process.exit(1)
})
