// Pega al feed GTFS-realtime de colectivos del GBA y guarda, para cada
// vehículo, un buffer circular de hasta MAX_POINTS posiciones en Postgres
// (Supabase). Se corre dos veces por invocación (con una pausa de
// POLL_GAP_MS en el medio) para lograr ~30s de granularidad aunque el cron
// que dispara esto solo pueda programarse cada 1 minuto.
//
// El slot dentro del buffer circular se calcula por bloque de tiempo
// (floor(ts / POLL_INTERVAL_S) % MAX_POINTS) en vez de con un contador
// persistido aparte: un solo INSERT multi-fila por poll alcanza, sin
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
const POLL_COUNT = 2
const POLL_GAP_MS = 30_000
const INSERT_CHUNK_SIZE = 1000

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

async function fetchVehicles() {
  const clientId = requireEnv("BA_TRANSPORTE_CLIENT_ID")
  const clientSecret = requireEnv("BA_TRANSPORTE_CLIENT_SECRET")

  const url = `https://apitransporte.buenosaires.gob.ar/colectivos/vehiclePositions?client_id=${clientId}&client_secret=${clientSecret}`
  const res = await fetch(url, { signal: AbortSignal.timeout(15000) })
  if (!res.ok) throw new Error(`API de Transporte respondió ${res.status}`)

  const FeedMessage = await getFeedMessageType()
  const msg = FeedMessage.decode(new Uint8Array(await res.arrayBuffer()))
  const obj = FeedMessage.toObject(msg, { longs: String, defaults: true })

  const vehicles = []
  for (const entity of obj.entity || []) {
    const v = entity.vehicle
    if (!v || !v.position || !v.vehicle?.id) continue
    const lat = v.position.latitude
    const lon = v.position.longitude
    if (lat < GBA_BBOX.minLat || lat > GBA_BBOX.maxLat || lon < GBA_BBOX.minLon || lon > GBA_BBOX.maxLon) continue
    const label = v.vehicle.label || ""
    const m = label.match(/^(\d+)-(.+)$/)
    const ts = Number(v.timestamp) || Math.floor(Date.now() / 1000)
    vehicles.push({
      vehicleId: v.vehicle.id,
      interno: m ? m[1] : null,
      lat,
      lon,
      ts,
      slot: Math.floor(ts / POLL_INTERVAL_S) % MAX_POINTS,
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
  const vehicles = await fetchVehicles()
  await savePositions(client, vehicles)
  console.log(`poll ${n}/${POLL_COUNT}: ${vehicles.length} vehiculos guardados`)
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
  } finally {
    await client.end()
  }
}

main().catch((err) => {
  console.error(err.message || err)
  process.exit(1)
})
