import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ── CONFIG ──────────────────────────────────────────────────────────
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const BASE_API     = "https://api.sealevelsensors.org/v1.0";
const PAGE_SIZE    = 1_000;            // external API page size
const CHUNK_SIZE   = 1_000;            // insert batch size (matches PAGE_SIZE)
// ─────────────────────────────────────────────────────────────────────

const db = createClient(SUPABASE_URL, SERVICE_ROLE, {
  auth: { persistSession: false }
});

/* ───────────────── helpers ───────────────────────────────────────── */

/** All datastream IDs already present in the `datastreams` table. */
async function allDatastreamIds(): Promise<number[]> {
  const { data, error } = await db.from("datastreams").select("datastream_id");
  if (error) throw error;
  return (data ?? []).map(r => r.datastream_id as number);
}

/** Newest timestamp we already have for a datastream. */
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

/**
 * Yield *all* API observations newer than `afterTime`.
 * Uses $orderby desc + paging with $skip, never $filter.
 */
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
        keepGoing = false;             // reached older data → stop
        break;
      }
      yield obs;                       // emit fresh row
    }

    if (page.length < PAGE_SIZE) break; // last page
    skip += PAGE_SIZE;
  }
}

/** Insert rows in chunks with conflict-ignore. */
async function insertChunk(dsId: number, chunk: any[]) {
  const rows = chunk.map(o => ({
    observation_id:        o["@iot.id"],
    datastream_id:         dsId,
    phenomenon_time_start: o.phenomenonTime,
    result_time:           o.resultTime,
    result:                o.result,
    parameters:            o.parameters
  }));

  const { error, count } = await db
    .from("observations")
    .insert(rows, {
      onConflict: "observation_id",
      returning: "minimal",
      count: "exact"
    });
  if (error) throw error;
  return count ?? 0;
}

/* ────────────────── Edge Function entry ─────────────────────────── */
serve(async () => {
  const report: Record<number, { fetched: number; inserted: number }> = {};

  for (const dsId of await allDatastreamIds()) {
    const latest = await latestInDb(dsId);

    let fetched = 0;
    let inserted = 0;
    let buffer: any[] = [];

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
