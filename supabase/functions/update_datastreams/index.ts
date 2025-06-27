import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ── CONFIG ──────────────────────────────────────────────────────────
const SUPABASE_URL  = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const BASE_API      = "https://api.sealevelsensors.org/v1.0";
const PAGE_SIZE     = 1_000;
const CHUNK_SIZE    = 1_000;
// ─────────────────────────────────────────────────────────────────────

const db = createClient(SUPABASE_URL, SERVICE_ROLE, {
  auth: { persistSession: false }
});

/* ───────── helpers ───────── */

async function allDatastreamIds(): Promise<number[]> {
  const { data, error } = await db.from("datastreams").select("datastream_id");
  if (error) throw error;
  return (data ?? []).map(r => r.datastream_id as number);
}

async function latestInDb(dsId: number) {
  const { data, error } = await db
    .from("observations")
    .select("phenomenon_time_start")
    .eq("datastream_id", dsId)
    .order("phenomenon_time_start", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.phenomenon_time_start as string | undefined;
}

/* ── NEW: fetch FeatureOfInterest once per observation ───────────── */
async function fetchFOI(obsId: number) {
  // 1) get Observation to obtain FOI link
  const o = await (await fetch(`${BASE_API}/Observations(${obsId})`)).json();
  const link: string = o["FeatureOfInterest@iot.navigationLink"];          // .../Observations(id)/FeatureOfInterest
  // 2) fetch FOI details
  const foi = await (await fetch(link)).json();
  return {
    feature_of_interest_id: foi["@iot.id"] as number,
    name:                   foi.name as string,
    description:            foi.description ?? "",
    encoding_type:          foi.encodingType as string,
    feature:                foi.feature,
    properties:             foi.properties ?? {}
  };
}

async function* streamNewObs(dsId: number, afterTime?: string) {
  let skip = 0;
  let keepGoing = true;

  while (keepGoing) {
    const qs = new URLSearchParams({
      $top: String(PAGE_SIZE),
      $skip: String(skip),
      $orderby: "phenomenonTime desc"
    });
    const url = `${BASE_API}/Datastreams(${dsId})/Observations?${qs}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API ${res.status}: ${await res.text()}`);

    const page = (await res.json()).value as any[];
    if (!page.length) break;

    for (const obs of page) {
      if (afterTime && obs.phenomenonTime <= afterTime) {
        keepGoing = false;
        break;
      }
      yield obs;
    }

    if (page.length < PAGE_SIZE) break;
    skip += PAGE_SIZE;
  }
}

/* ── NEW: bulk insert FOIs first ─────────────────────────────────── */
async function insertFOIs(rows: Awaited<ReturnType<typeof fetchFOI>>[]) {
  if (!rows.length) return;
  const { error } = await db
    .from("features_of_interest")
    .insert(rows, { onConflict: "feature_of_interest_id", ignoreDuplicates: true });
  if (error && error.code !== "23505") throw error; // ignore existing rows
}

/* ── UPDATED: insert observation chunk ───────────────────────────── */
async function insertChunk(dsId: number, chunk: any[]) {
  if (!chunk.length) return 0;

  /* 1 ▸ fetch all FOIs in parallel */
  const foiRows = await Promise.all(chunk.map((o) => fetchFOI(o["@iot.id"])));

  /* 2 ▸ insert FOIs (idempotent) */
  await insertFOIs(foiRows);

  /* 3 ▸ prepare observation rows with valid fk */
  const obsRows = chunk.map((o, idx) => ({
    observation_id:        o["@iot.id"],
    datastream_id:         dsId,
    phenomenon_time_start: o.phenomenonTime,
    result_time:           o.resultTime,
    result:                o.result,
    parameters:            o.parameters,
    feature_of_interest_id: foiRows[idx].feature_of_interest_id
  }));

  /* 4 ▸ insert observations */
  const { error, count } = await db
    .from("observations")
    .insert(obsRows, {
      onConflict: "observation_id",
      returning: "minimal",
      count: "exact"
    });
  if (error && error.code !== "23505") throw error;
  return count ?? 0;
}

/* ── Edge Function entry stays unchanged ─────────────────────────── */
serve(async () => {
  const report: Record<number, { fetched: number; inserted: number }> = {};

  for (const dsId of await allDatastreamIds()) {
    const latest = await latestInDb(dsId);

    let fetched = 0, inserted = 0, buffer: any[] = [];
    for await (const obs of streamNewObs(dsId, latest)) {
      buffer.push(obs);
      fetched++;
      if (buffer.length === CHUNK_SIZE) {
        inserted += await insertChunk(dsId, buffer);
        buffer = [];
      }
    }
    if (buffer.length) inserted += await insertChunk(dsId, buffer);

    report[dsId] = { fetched, inserted };
  }

  return new Response(JSON.stringify({ status: "ok", report }), {
    headers: { "Content-Type": "application/json" }
  });
});
