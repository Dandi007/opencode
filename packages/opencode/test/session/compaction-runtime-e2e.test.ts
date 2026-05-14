import { afterAll, describe, expect, test } from "bun:test"
import { Database as SQLite } from "bun:sqlite"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"

const TMP_ROOT = path.join(os.tmpdir(), "opencode-compact-e2e")
const RUN_ROOT = path.join(TMP_ROOT, `run-${process.pid}`)
const PROJECT_DIR = path.join(RUN_ROOT, "project")
const HOME_DIR = path.join(RUN_ROOT, "home")
const DB_DIR = path.join(RUN_ROOT, "db")
const DB_PATH = path.join(DB_DIR, "opencode.db")
const PACKAGE_DIR = path.resolve(import.meta.dir, "../..")
const BUN_BIN = path.join(os.homedir(), ".bun/bin/bun")

type OpenCodeProcess = {
  process: Bun.Subprocess<"ignore", "pipe", "pipe">
  output: string[]
}

let openCode: OpenCodeProcess | undefined
let llm: MockLLMServer | undefined
let passed = false

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function sse(lines: unknown[]) {
  return lines.map((line) => (line === "[DONE]" ? "data: [DONE]\n\n" : `data: ${JSON.stringify(line)}\n\n`)).join("")
}

function chatChunk(input: { delta?: Record<string, unknown>; finish?: string; usage?: { input: number; output: number } }) {
  return {
    id: "chatcmpl-runtime-e2e",
    object: "chat.completion.chunk",
    choices: [
      {
        delta: input.delta ?? {},
        ...(input.finish ? { finish_reason: input.finish } : {}),
      },
    ],
    ...(input.usage
      ? {
          usage: {
            prompt_tokens: input.usage.input,
            completion_tokens: input.usage.output,
            total_tokens: input.usage.input + input.usage.output,
          },
        }
      : {}),
  }
}

function chatResponse(text: string) {
  return sse([
    chatChunk({ delta: { role: "assistant" } }),
    chatChunk({ delta: { content: text } }),
    chatChunk({ finish: "stop", usage: { input: 17, output: 5 } }),
    "[DONE]",
  ])
}

function responsesResponse(text: string) {
  return sse([
    {
      type: "response.created",
      sequence_number: 1,
      response: { id: "resp_runtime_e2e", created_at: Math.floor(Date.now() / 1000), model: "test-model" },
    },
    {
      type: "response.output_item.added",
      sequence_number: 2,
      output_index: 0,
      item: { type: "message", id: "msg_runtime_e2e" },
    },
    {
      type: "response.output_text.delta",
      sequence_number: 3,
      item_id: "msg_runtime_e2e",
      delta: text,
      logprobs: null,
    },
    {
      type: "response.output_item.done",
      sequence_number: 4,
      output_index: 0,
      item: { type: "message", id: "msg_runtime_e2e" },
    },
    {
      type: "response.completed",
      sequence_number: 5,
      response: {
        incomplete_details: null,
        service_tier: null,
        usage: {
          input_tokens: 17,
          input_tokens_details: { cached_tokens: null },
          output_tokens: 5,
          output_tokens_details: { reasoning_tokens: null },
        },
      },
    },
    "[DONE]",
  ])
}

class MockLLMServer {
  readonly server: Bun.Server
  readonly requests: Array<{ url: string; body: unknown; compaction: boolean }> = []

  constructor() {
    this.server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: async (request) => {
        const url = new URL(request.url)
        if (request.method !== "POST" || (url.pathname !== "/v1/chat/completions" && url.pathname !== "/v1/responses")) {
          return new Response("not found", { status: 404 })
        }

        const body = await request.json().catch(() => ({}))
        const serialized = JSON.stringify(body)
        const title = serialized.includes("Generate a title for this conversation")
        const compaction = !title && /compact|compaction|summary|summar/i.test(serialized)
        this.requests.push({ url: url.pathname, body, compaction })

        if (compaction) await sleep(250)

        const text = compaction ? "runtime compaction summary" : title ? "Runtime E2E" : "runtime assistant response"
        const payload = url.pathname === "/v1/responses" ? responsesResponse(text) : chatResponse(text)
        return new Response(payload, { headers: { "content-type": "text/event-stream" } })
      },
    })
  }

  get url() {
    return `http://127.0.0.1:${this.server.port}/v1`
  }

  stop() {
    this.server.stop(true)
  }
}

function providerConfig(baseURL: string) {
  return {
    formatter: false,
    lsp: false,
    plugin: [],
    provider: {
      test: {
        id: "test",
        name: "Runtime Test Provider",
        npm: "@ai-sdk/openai-compatible",
        env: [],
        models: {
          "test-model": {
            id: "test-model",
            name: "Runtime Test Model",
            attachment: false,
            reasoning: false,
            temperature: false,
            tool_call: true,
            release_date: "2026-05-15",
            limit: { context: 100_000, output: 10_000 },
            cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
            options: {},
          },
        },
        options: {
          apiKey: "test-key",
          baseURL,
        },
      },
    },
  }
}

async function initProject() {
  await fs.rm(RUN_ROOT, { recursive: true, force: true })
  await fs.mkdir(PROJECT_DIR, { recursive: true })
  await fs.mkdir(HOME_DIR, { recursive: true })
  await fs.mkdir(DB_DIR, { recursive: true })
  await fs.writeFile(path.join(PROJECT_DIR, "README.md"), "# runtime compaction e2e\n")
  const proc = Bun.spawn(["git", "init"], {
    cwd: PROJECT_DIR,
    stdout: "ignore",
    stderr: "ignore",
    env: { ...process.env, GIT_MASTER: "1" },
  })
  const code = await proc.exited
  if (code !== 0) throw new Error("failed to initialize temporary git project")
}

async function readStream(stream: ReadableStream<Uint8Array>, onText: (text: string) => void) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  try {
    while (true) {
      const next = await reader.read()
      if (next.done) break
      onText(decoder.decode(next.value, { stream: true }))
    }
  } finally {
    reader.releaseLock()
  }
}

async function startOpenCode(baseURL: string) {
  const output: string[] = []
  const env = {
    ...process.env,
    HOME: HOME_DIR,
    XDG_DATA_HOME: path.join(HOME_DIR, ".local/share"),
    XDG_CONFIG_HOME: path.join(HOME_DIR, ".config"),
    OPENCODE_DB: DB_PATH,
    OPENCODE_SERVER_PASSWORD: "test",
    OPENCODE_PURE: "1",
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_DISABLE_LSP_DOWNLOAD: "1",
    OPENCODE_CONFIG_CONTENT: JSON.stringify(providerConfig(baseURL)),
    PATH: `${path.dirname(BUN_BIN)}:${process.env.PATH ?? ""}`,
  }
  const proc = Bun.spawn([BUN_BIN, "run", "--conditions=browser", "./src/index.ts", "serve", "--port", "0", "--hostname", "127.0.0.1"], {
    cwd: PACKAGE_DIR,
    env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  })
  openCode = { process: proc, output }

  let resolved = false
  let resolvePort!: (port: number) => void
  let rejectPort!: (error: Error) => void
  const ready = new Promise<number>((resolve, reject) => {
    resolvePort = resolve
    rejectPort = reject
  })

  const onText = (text: string) => {
    output.push(text)
    const match = /opencode server listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(output.join(""))
    if (!resolved && match) {
      resolved = true
      resolvePort(Number(match[1]))
    }
  }
  void readStream(proc.stdout, onText)
  void readStream(proc.stderr, onText)
  void proc.exited.then((code) => {
    if (!resolved) {
      resolved = true
      rejectPort(new Error(`opencode serve exited before ready: ${code}\n${output.join("")}`))
    }
  })

  const port = await Promise.race([
    ready,
    sleep(20_000).then(() => {
      throw new Error(`timed out waiting for opencode serve\n${output.join("")}`)
    }),
  ])
  return { port, output }
}

function authHeaders() {
  return {
    Authorization: `Basic ${Buffer.from("opencode:test").toString("base64")}`,
    "x-opencode-directory": PROJECT_DIR,
    "Content-Type": "application/json",
  }
}

async function requestJson<T>(base: string, route: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(`${base}${route}`, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers ?? {}) },
  })
  const text = await response.text()
  if (!response.ok) throw new Error(`${init.method ?? "GET"} ${route} -> ${response.status}: ${text}`)
  if (!text) return undefined as T
  return JSON.parse(text) as T
}

async function waitForIdle(base: string, sessionID: string) {
  for (let i = 0; i < 100; i++) {
    const status = await requestJson<Record<string, { type: string }>>(base, "/session/status")
    if (!status[sessionID] || status[sessionID].type === "idle") return
    await sleep(50)
  }
  throw new Error(`session did not become idle: ${sessionID}`)
}

function count(db: SQLite, sql: string, sessionID: string) {
  return (db.query(sql).get(sessionID) as { count: number }).count
}

function verifyDatabase(sessionID: string) {
  const db = new SQLite(DB_PATH, { readonly: true })
  try {
    const markerCount = count(
      db,
      `SELECT COUNT(*) AS count
       FROM message m
       JOIN part p ON p.message_id = m.id
       WHERE json_extract(p.data, '$.type') = 'compaction'
         AND json_extract(m.data, '$.role') = 'user'
         AND m.session_id = ?`,
      sessionID,
    )
    const assistantCount = count(
      db,
      `SELECT COUNT(*) AS count
       FROM message
       WHERE json_extract(data, '$.agent') = 'compaction'
         AND json_extract(data, '$.role') = 'assistant'
         AND session_id = ?`,
      sessionID,
    )
    const marker = db
      .query(
        `SELECT m.id AS marker_id
         FROM message m
         JOIN part p ON p.message_id = m.id
         WHERE json_extract(p.data, '$.type') = 'compaction'
           AND json_extract(m.data, '$.role') = 'user'
           AND m.session_id = ?`,
      )
      .get(sessionID) as { marker_id: string } | null
    const assistant = db
      .query(
        `SELECT json_extract(data, '$.parentID') AS parent_id
         FROM message
         WHERE json_extract(data, '$.agent') = 'compaction'
           AND json_extract(data, '$.role') = 'assistant'
           AND session_id = ?`,
      )
      .get(sessionID) as { parent_id: string } | null
    const nonMarkerParentCount = count(
      db,
      `SELECT COUNT(*) AS count
       FROM message a
       JOIN message parent ON parent.id = json_extract(a.data, '$.parentID')
       WHERE a.session_id = ?
         AND json_extract(a.data, '$.agent') = 'compaction'
         AND json_extract(a.data, '$.role') = 'assistant'
         AND NOT EXISTS (
           SELECT 1 FROM part pp
           WHERE pp.message_id = parent.id
             AND json_extract(pp.data, '$.type') = 'compaction'
         )`,
      sessionID,
    )
    const orphanMarkerCount = count(
      db,
      `SELECT COUNT(*) AS count
       FROM message marker
       JOIN part p ON p.message_id = marker.id
       WHERE marker.session_id = ?
         AND json_extract(marker.data, '$.role') = 'user'
         AND json_extract(p.data, '$.type') = 'compaction'
         AND NOT EXISTS (
           SELECT 1 FROM message a
           WHERE a.session_id = marker.session_id
             AND json_extract(a.data, '$.agent') = 'compaction'
             AND json_extract(a.data, '$.role') = 'assistant'
             AND json_extract(a.data, '$.parentID') = marker.id
         )`,
      sessionID,
    )

    expect(markerCount).toBe(1)
    expect(assistantCount).toBe(1)
    expect(marker?.marker_id).toBeTruthy()
    expect(assistant?.parent_id).toBe(marker?.marker_id)
    expect(nonMarkerParentCount).toBe(0)
    expect(orphanMarkerCount).toBe(0)

    return {
      markerCount,
      assistantCount,
      markerID: marker?.marker_id,
      assistantParentID: assistant?.parent_id,
      nonMarkerParentCount,
      orphanMarkerCount,
    }
  } finally {
    db.close()
  }
}

async function stopOpenCode() {
  const proc = openCode?.process
  if (!proc) return
  proc.kill("SIGTERM")
  await Promise.race([
    proc.exited,
    sleep(1_500).then(() => {
      proc.kill("SIGKILL")
    }),
  ])
}

afterAll(async () => {
  await stopOpenCode()
  llm?.stop()
  if (passed) await fs.rm(RUN_ROOT, { recursive: true, force: true })
  else console.error(`Preserving runtime E2E temp directory: ${RUN_ROOT}`)
})

describe("runtime compaction dedupe e2e", () => {
  test("dedupes concurrent compact overlap through real serve process", async () => {
    await initProject()
    llm = new MockLLMServer()
    const { port } = await startOpenCode(llm.url)
    const base = `http://127.0.0.1:${port}`

    await requestJson(base, "/global/health")

    const session = await requestJson<{ id: string }>(base, "/session", {
      method: "POST",
      body: JSON.stringify({}),
    })
    const sessionID = session.id

    await requestJson(base, `/session/${sessionID}/message`, {
      method: "POST",
      body: JSON.stringify({
        agent: "build",
        model: { providerID: "test", modelID: "test-model" },
        parts: [{ type: "text", text: "Seed the runtime compaction E2E session." }],
      }),
    })
    await waitForIdle(base, sessionID)

    const summarize = () =>
      requestJson<boolean>(base, `/session/${sessionID}/summarize`, {
        method: "POST",
        body: JSON.stringify({ providerID: "test", modelID: "test-model", auto: false }),
      })

    const results = await Promise.allSettled([summarize(), summarize()])
    for (const result of results) {
      if (result.status === "rejected") throw result.reason
    }
    await waitForIdle(base, sessionID)

    const dbResult = verifyDatabase(sessionID)
    const compactCalls = llm.requests.filter((request) => request.compaction).length
    expect(compactCalls).toBeGreaterThanOrEqual(1)

    console.log(
      JSON.stringify(
        {
          sessionID,
          dbPath: DB_PATH,
          projectDir: PROJECT_DIR,
          mockLLMRequests: llm.requests.length,
          compactCalls,
          ...dbResult,
        },
        null,
        2,
      ),
    )

    passed = true
  }, 45_000)
})
