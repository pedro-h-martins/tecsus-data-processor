import postgres from "postgres";
import { DateTime } from "luxon";
import { MongoClient } from "mongodb";

const sql = postgres({
  host: process.env.PG_HOST!,
  port: 5432,
  database: process.env.PG_DB!,
  username: process.env.PG_USER!,
  password: process.env.PG_PASSWORD!,
  ssl: { rejectUnauthorized: false }, // RDS usa SSL
  max: 10,
});

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? "100");
const SLEEP_SECONDS = parseInt(process.env.SLEEP_SECONDS ?? "60");
const CACHE_RELOAD_INTERVAL =
  parseInt(process.env.CACHE_RELOAD_INTERVAL ?? "300") * 1000;

const MONGO_URI = process.env.MONGODB_URI!;
const MONGO_DB = process.env.MONGODB_DB_NAME || "api4";
const MONGO_COLLECTION = process.env.MONGODB_COLLECTION_NAME || "raw_payloads";

let mongoClient: MongoClient | null = null;

async function getCollection(): Promise<any> {
  if (!mongoClient) {
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    console.log("[MongoDB] Conectado.");
  }
  return mongoClient.db(MONGO_DB).collection(MONGO_COLLECTION);
}

let stationCache: Record<string, number> = {};
let parameterTypeCache: Record<string, number> = {};
let parameterCache: Record<string, number> = {};
let lastCacheReload = 0;

function parameterCacheKey(stationId: number, parameterTypeId: number): string {
  return `${stationId}:${parameterTypeId}`;
}

async function loadCaches(): Promise<void> {
  console.log("[Cache] Carregando caches do RDS...");

  const stations = await sql`SELECT id, id_datalogger FROM stations`;
  stationCache = Object.fromEntries(stations.map((s) => [s.id_datalogger, s.id]));

  const parameterTypes = await sql`SELECT id, json_name FROM parameter_types`;
  parameterTypeCache = Object.fromEntries(parameterTypes.map((pt) => [pt.json_name, pt.id]));

  const parameters = await sql`SELECT id, id_station, id_parameter_type FROM parameters`;
  parameterCache = Object.fromEntries(
    parameters.map((p) => [parameterCacheKey(p.id_station, p.id_parameter_type), p.id])
  );

  lastCacheReload = Date.now();
  console.log("[Cache] Caches carregados. Estações:", Object.keys(stationCache).length);
}

async function reloadCachesIfNeeded(): Promise<void> {
  if (Date.now() - lastCacheReload >= CACHE_RELOAD_INTERVAL && CACHE_RELOAD_INTERVAL > 0) {
    await loadCaches();
  }
}

async function refreshStationCache(uid: string): Promise<number | null> {
  const rows = await sql`
    SELECT id, id_datalogger FROM stations WHERE id_datalogger = ${uid} LIMIT 1
  `;
  if (rows.length > 0) {
    stationCache[rows[0].id_datalogger] = rows[0].id;
    return rows[0].id;
  }
  return null;
}

async function refreshParameterTypeCache(jsonName: string): Promise<number | null> {
  const rows = await sql`
    SELECT id, json_name FROM parameter_types WHERE json_name = ${jsonName} LIMIT 1
  `;
  if (rows.length > 0) {
    parameterTypeCache[rows[0].json_name] = rows[0].id;
    return rows[0].id;
  }
  return null;
}

async function refreshParameterCache(
  stationId: number,
  parameterTypeId: number
): Promise<number | null> {
  const rows = await sql`
    SELECT id, id_station, id_parameter_type FROM parameters
    WHERE id_station = ${stationId} AND id_parameter_type = ${parameterTypeId}
    LIMIT 1
  `;
  if (rows.length > 0) {
    const key = parameterCacheKey(rows[0].id_station, rows[0].id_parameter_type);
    parameterCache[key] = rows[0].id;
    return rows[0].id;
  }
  return null;
}

async function processBatch(): Promise<number> {
  const collection = await getCollection();
  const documents = await collection.find().limit(BATCH_SIZE).toArray();

  let totalDocs = 0;
  const idsToDelete: object[] = [];

  for (const doc of documents) {
    totalDocs++;
    const docId = doc._id;
    const payload = doc.payload;

    try {
      const uid: string | undefined = payload.uid;
      const unixtime: number | undefined = payload.uxt;

      if (!uid || !unixtime) {
        idsToDelete.push(docId);
        continue;
      }

      const stationId = stationCache[uid] ?? (await refreshStationCache(uid));
      if (!stationId) {
        idsToDelete.push(docId);
        continue;
      }

      const dateTime = DateTime.fromSeconds(unixtime, {
        zone: "America/Sao_Paulo",
      }).toISO();

      const measurements: { id_parameter: number; value: number; date_time: string }[] = [];

      for (const [key, value] of Object.entries(payload)) {
        if (["_id", "uid", "unixtime"].includes(key)) continue;

        const parameterTypeId =
          parameterTypeCache[key] ?? (await refreshParameterTypeCache(key));
        if (!parameterTypeId) continue;

        const cacheKey = parameterCacheKey(stationId, parameterTypeId);
        const parameterId =
          parameterCache[cacheKey] ?? (await refreshParameterCache(stationId, parameterTypeId));
        if (!parameterId) continue;

        measurements.push({ id_parameter: parameterId, value: Number(value), date_time: dateTime! });
      }

      if (measurements.length === 0) {
        idsToDelete.push(docId);
        continue;
      }

      // Upsert em batch com postgres.js
      await sql`
        INSERT INTO measurements ${sql(measurements, "id_parameter", "value", "date_time")}
        ON CONFLICT (id_parameter, date_time) DO UPDATE
          SET value = EXCLUDED.value
      `;

      idsToDelete.push(docId);
    } catch (err) {
      console.error("[Sync] Erro inesperado no documento:", err);
      continue;
    }
  }

  if (idsToDelete.length > 0) {
    await collection.deleteMany({ _id: { $in: idsToDelete } });
    console.log(`[Sync] ${idsToDelete.length} documentos processados e removidos do MongoDB.`);
  }

  return totalDocs;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function sleepWithCountdown(seconds: number): Promise<void> {
  for (let remaining = seconds; remaining > 0; remaining--) {
    await sleep(1000);
  }
}

export async function main(): Promise<void> {
  console.log("[Servidor B] Iniciando sync MongoDB → RDS PostgreSQL...");
  await loadCaches();

  while (true) {
    try {
      await reloadCachesIfNeeded();
      let totalProcessed = 0;

      while (true) {
        const count = await processBatch();
        totalProcessed += count;
        if (count < BATCH_SIZE) break;
        await sleep(200);
      }

      if (totalProcessed === 0) {
        await sleepWithCountdown(SLEEP_SECONDS);
      } else {
        continue;
      }
    } catch (err) {
      console.error("[Servidor B] Erro no loop principal:", err);
      await sleep(5000);
    }
  }
}

main().catch((err) => {
  console.error("[Servidor B] Falha fatal:", err);
  process.exit(1);
});