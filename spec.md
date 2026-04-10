# Leyla

## Claude Code icin session-disconnect-safe scheduler / orchestration plugin tasarimi

> **Motto:** *"Zaman zaman beni dusunup agliyormussun Leyla"*
> Claude Code session kapaninca calismaya devam edemeyen agent workflow’leri icin, dis sureklilige dayali, tekrar baglaninca durumunu toparlayan scheduler / orchestrator.

---

# 0. Scope Correction (Rust / Claude Code CLI plugin)

Bu dokumanin ilk versiyonu bilerek daha genis ve platform-level yazildi. Ama senin asil hedefin daha spesifik:

* hedef ortam **Claude Code terminal / CLI**
* implementasyon dili **Rust**
* ana entrypoint **`/leyla` komutu**
* temel use-case: kullanici bir isi schedule ettiginde, **o anki session/context kaydedilsin**
* daha sonra isterse:

  * ayni session mantigina yakin bir restore/resume akisiyla devam etsin
  * ya da **ayri bir execution session** acip isi orada yapsin
* bu tasarim **OpenClaw icin degil**, dogrudan **Claude Code terminal deneyimi** icin dusunuluyor

Yani Leyla’nin V1 kimligi su olmali:

> **Claude Code CLI icin Rust tabanli, session-aware, resumable scheduler plugin**

Bu fark cok onemli. Cunku burada problem genel dagitik cron problemi degil; esas problem su:

* Claude Code session kapaninca komut akisi dogrudan tetiklenemiyor
* dolayisiyla `/leyla` komutu ile bir **intent + context snapshot + execution policy** kaydi olusturmak gerekiyor
* sonra uygun zamanda bu kayit ya kullanici geri geldiginde resume edilmeli ya da ayri bir detached calisma modeliyle islenmeli

Bu nedenle kalan tum bolumler su lens ile okunmali:

* durable scheduler evet
* ama daha da onemlisi **session snapshot / restore / detached execution orchestration**

# 1. Executive Summary

## Problem

Claude Code gibi session-temelli agent ortamlarinda runtime surekliligi garanti degildir. Session kapanir, terminal kapanir, process restart olur, agent context sifirlanir. Bu durumda klasik in-memory cron mantigi calismaz.

Yani problem aslinda su degildir:

* "cron nasil calisir?"

Asil problem sudur:

* **Agent yokken schedule nasil hayatta kalir?**
* **Calismasi gereken isler session yokken nasil kaybolmaz?**
* **Tekrar baglanildiginda sistem nasil state recover eder?**
* **Job execution ile agent execution nasil ayrilir?**

## Core Thesis

**Leyla, cron calistiran bir process degil; durable scheduling state tutan, execution niyetini kaydeden ve Claude Code yeniden geldiginde ya da harici worker mevcut oldugunda execution’i devam ettiren bir orchestration katmani olmalidir.**

Bu nedenle Leyla’yi su sekilde dusunmek gerekir:

* bir **durable scheduler**
* bir **execution ledger**
* bir **lease / lock aware dispatcher**
* bir **resume-capable agent task coordinator**

## Non-Goal

Leyla’nin ilk surumu sunlari hedeflememelidir:

* tam bir Kubernetes job controller olmak
* Airflow seviyesinde DAG studio sunmak
* realtime stream processor olmak
* OS-level guaranteed daemon replacement olmak

## Goal

Ilk guclu versiyonun hedefi:

1. schedule tanimlarini durable saklamak
2. due olan isleri deterministik olarak bulmak
3. duplicate execution’lari engellemek
4. retry / backoff / timeout / dead-letter desteklemek
5. Claude Code session geri geldiginde state’i toparlamak
6. agent-based ve external-worker-based execution modellerini desteklemek
7. gozlemlenebilir, acik ve gelistirilebilir bir plugin mimarisi sunmak

---

# 2. Problem Space

## 2.1 Claude Code reality

Claude Code session’i su sebeplerle kesilebilir:

* kullanici terminali kapatir
* network kopar
* CLI process restart olur
* local machine sleep olur
* context reset olur
* plugin host yeniden baslar

Bunlar scheduler icin su anlama gelir:

* in-memory timer’lar guvenilmez
* `setInterval`, `setTimeout`, node-cron gibi cozumler tek basina yetmez
* runtime liveness ile schedule truth ayrilmalidir

## 2.2 Core design insight

Leyla’da **time truth** ile **execution truth** birbirinden ayrilmalidir.

### Time truth

Bir job ne zaman calismali?

### Execution truth

O job gercekten calisti mi, basarili mi oldu, retry bekliyor mu, lease kimde, timeout oldu mu?

Klasik basarisiz tasarimlar ikisini ayni yerde toplar ve runtime dusunce state kaybolur.

Leyla’da bunlar durable storage uzerinden modellenmelidir.

---

# 3. Design Principles

## 3.1 Durable-first

Tum schedule ve execution state durable storage’da tutulur.
Memory sadece cache / optimization icindir.

## 3.2 Deterministic recovery

Sistem restart oldugunda:

* hangi job’lar overdue
* hangileri running gibi gozukuyor
* hangilerinin lease’i stale
* hangileri retry bekliyor

bunlar deterministik olarak bulunabilmelidir.

## 3.3 At-least-once by default

Tam exactly-once zor ve maliyetlidir. Ilk tasarim:

* **at-least-once dispatch**
* **idempotent handler** tavsiyesi
* duplicate guard / lease / execution token ile risk azaltma

## 3.4 Separation of concerns

Ayrik katmanlar:

* schedule calculation
* due selection
* dispatch
* execution
* result persistence
* retry policy
* observability

## 3.5 Pluggable execution

Job mutlaka ayni process’te calismak zorunda degil.
Execution stratejileri degisebilir:

* local callback
* shell command
* webhook
* queue publish
* Claude task file / agent resume request
* remote worker

## 3.6 Human-debuggable

Sistem sadece calismamali; anlasilabilir olmali.
Operasyon sorulari kolay cevaplanmali:

* neden bu job calismadi?
* neden iki kere calisti?
* neden stuck oldu?
* en son hata neydi?
* bir sonraki run ne zaman?

---

# 4. High-Level Architecture

```text
+-----------------------+
|   Claude Code Plugin  |
|       (Leyla API)     |
+-----------+-----------+
            |
            v
+-----------------------+
|   Scheduler Engine    |
| - schedule parser     |
| - due calculator      |
| - recovery loop       |
+-----------+-----------+
            |
            v
+-----------------------+
|   State Store         |
| - jobs                |
| - schedules           |
| - executions          |
| - leases              |
| - retry state         |
+-----------+-----------+
            |
   +--------+--------+
   |                 |
   v                 v
+---------+    +----------------+
|Dispatch  |    | Observability  |
| Layer    |    | logs/metrics   |
+----+-----+    +----------------+
     |
     v
+-------------------------------+
| Execution Adapters            |
| - local function              |
| - shell command               |
| - webhook                     |
| - queue publish               |
| - claude-resume task bridge   |
| - external worker             |
+-------------------------------+
```

---

# 5. Core Capability Model

## 5.1 Job definition

Bir job iki seyden olusur:

1. **schedule contract**
2. **execution contract**

### Canonical shape

```ts
export type LeylaJob = {
  id: string
  name: string
  enabled: boolean

  schedule: LeylaSchedule
  executor: LeylaExecutorSpec

  concurrency?: ConcurrencyPolicy
  retry?: RetryPolicy
  timeoutMs?: number
  misfire?: MisfirePolicy
  dedupe?: DedupePolicy

  metadata?: Record<string, unknown>
  tags?: string[]
}
```

## 5.2 Schedule types

```ts
export type LeylaSchedule =
  | { type: 'cron'; expression: string; timezone?: string }
  | { type: 'interval'; everyMs: number; anchor?: 'finish' | 'start' | 'wall-clock' }
  | { type: 'once'; at: string }
  | { type: 'manual' }
```

## 5.3 Executor types

```ts
export type LeylaExecutorSpec =
  | { type: 'local-handler'; handlerKey: string }
  | { type: 'shell'; command: string; args?: string[] }
  | { type: 'webhook'; url: string; method?: 'POST' | 'GET'; headers?: Record<string, string> }
  | { type: 'queue-publish'; topic: string; payloadTemplate?: unknown }
  | { type: 'claude-task'; taskType: string; payloadTemplate?: unknown }
  | { type: 'remote-worker'; workerType: string; payloadTemplate?: unknown }
```

## 5.4 Why executor abstraction matters

Claude Code’in kendisi surekli yasamiyorsa, execution runtime’i agent’tan ayrilabilmelidir.
Boylece:

* Claude session varsa local/orchestration action alir
* session yoksa durable intent kaydolur
* external worker varsa isi disaridan tamamlar

---

# 6. State Model

## 6.1 Main entities

Leyla icin minimum veri modeli:

* `jobs`
* `job_runs`
* `leases`
* `outbox_events`
* `heartbeats` (opsiyonel)
* `dead_letters`

## 6.2 jobs table

```ts
type JobRecord = {
  id: string
  name: string
  enabled: boolean
  definitionJson: string

  nextRunAt: string | null
  lastRunAt: string | null
  lastSuccessAt: string | null
  lastFailureAt: string | null

  createdAt: string
  updatedAt: string
  version: number
}
```

## 6.3 job_runs table

Bu tablo kritik. Tum execution history burada tutulur.

```ts
type JobRunRecord = {
  runId: string
  jobId: string

  scheduledFor: string
  dispatchAt: string | null
  startedAt: string | null
  heartbeatAt: string | null
  finishedAt: string | null

  status:
    | 'scheduled'
    | 'leased'
    | 'dispatched'
    | 'running'
    | 'succeeded'
    | 'failed'
    | 'retry_wait'
    | 'dead_letter'
    | 'cancelled'
    | 'timed_out'
    | 'orphaned'

  attempt: number
  maxAttempts: number
  leaseOwner: string | null
  leaseExpiresAt: string | null

  inputJson: string | null
  outputJson: string | null
  errorJson: string | null

  createdAt: string
  updatedAt: string
}
```

## 6.4 Why run ledger is the heart

Bu tablo olmadan recovery kotu olur. Cunku sistem restart oldugunda sadece current timer state’i degil, tum niyet ve sonuc akisini gorebilmelisin.

Job-level state yerine run-ledger tutulursa:

* retry izi kaybolmaz
* duplicate olaylari analiz edilir
* stuck execution recover edilir
* idempotency token runId uzerinden kurulabilir

---

# 7. Lifecycle Model

## 7.1 State machine

```text
scheduled
  -> leased
  -> dispatched
  -> running
     -> succeeded
     -> failed -> retry_wait -> scheduled
     -> timed_out
     -> orphaned
     -> dead_letter
```

## 7.2 Semantics

### scheduled

Job bir zaman icin olusturulmus ama daha alinmamis.

### leased

Bir scheduler instance bu run’i secmis ve belirli sure icin sahiplenmis.

### dispatched

Execution adapter’a teslim edilmis.

### running

Worker veya agent calismayi baslatmis.

### retry_wait

Fail oldu ama policy geregi tekrar denenecek.

### orphaned

Running gozukuyor ama heartbeat eskimis; sahibi kaybolmus olabilir.

### dead_letter

Retry hakki bitti, artik manuel inceleme gerekir.

---

# 8. Misfire and Session Gap Strategy

Claude session kapaninca en buyuk problem: gecmis run’lar ne olacak?

## 8.1 Misfire definition

Bir job’in `scheduledFor` zamani gelmis ama job o anda dispatch edilememis.

## 8.2 Policy options

```ts
export type MisfirePolicy =
  | { type: 'run-immediately' }
  | { type: 'skip' }
  | { type: 'coalesce' }
  | { type: 'replay-all'; maxCatchup?: number }
```

## 8.3 Recommended default

**Default: `coalesce`**

Neden?

* Session 3 saat kapaliysa ve dakikalik job varsa 180 run backlog cikar
* Bunlari koru korune replay etmek sistemi patlatabilir
* Coalesce ile sadece en guncel gerekli run dispatch edilir

## 8.4 When to use replay-all

Finansal, audit-critical, ETL gibi her slot onemliyse.

## 8.5 When to use skip

Health ping, cache warmup, low-value periodic tasks.

---

# 9. Concurrency and Lease Design

## 9.1 Why lease gerekli

Multiple scheduler instance veya restart senaryosunda ayni run iki kez dispatch edilmesin.

## 9.2 Lease contract

Bir run secilirken atomik sekilde su yapilmalidir:

1. status `scheduled` olmalı
2. `leaseOwner` set edilmeli
3. `leaseExpiresAt` belirlenmeli
4. status `leased` olmalı

## 9.3 Lease TTL

Lease sonsuz olmamali. Ornek:

* default 30s / 60s
* long-running job’lar heartbeat ile uzatir

## 9.4 Stale lease recovery

Recovery loop sunu yapar:

* `leased` veya `running` olup `leaseExpiresAt < now` olan run’lari bul
* heartbeat stale ise `orphaned` ya da tekrar `scheduled` yap
* policy’ye gore retry/cancel et

## 9.5 Concurrency policies

```ts
export type ConcurrencyPolicy =
  | { type: 'allow' }
  | { type: 'forbid-overlap' }
  | { type: 'queue-one' }
  | { type: 'replace-running' }
```

### Recommended defaults

* infra / cleanup jobs: `forbid-overlap`
* polling jobs: `queue-one`
* stateless cheap jobs: `allow`

---

# 10. Retry / Backoff / Failure Taxonomy

## 10.1 Retry is not binary

Tum hatalar ayni degil.

### Retryable

* network timeout
* 429 / 503
* temporary lock conflict
* remote worker unavailable

### Non-retryable

* validation error
* bad configuration
* missing secret
* malformed payload

## 10.2 Retry policy

```ts
export type RetryPolicy = {
  maxAttempts: number
  strategy: 'fixed' | 'linear' | 'exponential'
  baseDelayMs: number
  maxDelayMs?: number
  jitter?: boolean
  retryOn?: Array<'timeout' | 'network' | 'rate-limit' | 'unknown'>
}
```

## 10.3 Suggested default

```ts
{
  maxAttempts: 5,
  strategy: 'exponential',
  baseDelayMs: 1000,
  maxDelayMs: 60000,
  jitter: true,
  retryOn: ['timeout', 'network', 'rate-limit', 'unknown']
}
```

## 10.4 Dead letter

Retry limiti dolunca run dead-letter’a gider.
Bu sadece status degil; debug icin snapshot tutulmali:

* input
* attempts
* final error
* lease owner
* job version

---

# 11. Claude Code Specific Execution Model

Bu dokumanin en kritik bolumu bu.

## 11.1 Local cron neden yetmez

Claude Code plugin session’i kapandiginda:

* timer yok olur
* callback yok olur
* memory yok olur

Bu yuzden Leyla’nin runtime modeli su sekilde ayrilmali:

### A. Control plane

Her zaman durable state’e yazar, scheduler truth burada yasar.

### B. Execution plane

Su ortamlardan biri olabilir:

* aktif Claude session
* external worker process
* webhook receiver
* shell-executor host

## 11.2 Claude-aware task model

Claude’a direkt "background’da hep calis" diyemezsin. Bunun yerine su modeli kurarsin:

* Leyla due oldugunda **execution intent** olusturur
* intent storage’a yazilir
* Claude geri geldiginde pending intent’leri gorur
* uygun policy varsa toplu process eder

Bu pattern’in adi aslinda:
**resume-driven orchestration**

## 11.3 Two operating modes

### Mode 1: Attached mode

Claude session aktifken plugin hem scheduler hem executor olabilir.

Avantaj:

* hizli gelistirme
* local dev icin ideal

Risk:

* session dusunce execution biter

### Mode 2: Detached mode

Scheduler state durable store’da, execution external worker veya resumable task bridge ile ilerler.

Avantaj:

* production-grade
* session-disconnect-safe

Tavsiye:
**Leyla’nin asil kimligi Detached mode olmali. Attached mode sadece convenience layer olmali.**

---

# 12. Recommended Product Positioning

Leyla’yi su sekilde tanimla:

> **Durable scheduler and resumable task orchestrator for Claude Code workflows**

Bu positioning sunlardan daha gucludur:

* "cron plugin"
* "job runner"
* "simple scheduler"

Cunku asil deger:

* persistence
* recovery
* resumability
* execution abstraction

---

# 13. Minimal Viable Architecture

## 13.1 V1 scope

V1 icin yeterli ama dogru kapsami oneriyorum:

### Included

* durable job definitions
* cron + interval + one-shot support
* due job scanning
* lease-based claiming
* local shell executor
* local handler registry
* retry/backoff
* timeout handling
* dead-letter
* recovery loop
* job history
* CLI/API controls

### Not included initially

* full DAG editor
* multi-region consensus
* visual UI
* arbitrary distributed cluster sharding
* fine-grained RBAC

## 13.2 Why this scope right

Bu scope ile:

* gercek problem cozulur
* complexity patlamaz
* architecture future-proof kalir

---

# 14. Suggested Module Breakdown

```text
packages/
  leyla-core/
    scheduler/
    lifecycle/
    policies/
    types/

  leyla-store/
    sqlite/
    postgres/
    migrations/

  leyla-executors/
    local-handler/
    shell/
    webhook/
    claude-task/

  leyla-observability/
    logger/
    metrics/
    tracing/

  leyla-cli/
    commands/

  leyla-plugin-claude/
    plugin bridge / command adapters
```

## 14.1 Why monorepo

* core logic engine ayrilir
* store adapter’lari bagimsiz gelisir
* executor paketleri pluginlesir
* testing kolaylasir

---

# 15. Storage Recommendation

## 15.1 Best default for V1

**SQLite first, Postgres second**

### SQLite neden guclu baslangic

* Claude Code local plugin icin uygun
* setup friction dusuk
* transaction / atomic update kolay
* tek kullanicili veya dusuk concurrency senaryosunda yeterli

### Postgres ne zaman

* multi-instance scheduler
* remote worker pool
* shared deployment
* operational visibility artinca

## 15.2 Store interface

```ts
interface LeylaStore {
  upsertJob(job: JobRecord): Promise<void>
  getJob(jobId: string): Promise<JobRecord | null>
  listJobs(): Promise<JobRecord[]>

  claimDueRuns(now: Date, owner: string, limit: number): Promise<JobRunRecord[]>
  insertRun(run: JobRunRecord): Promise<void>
  updateRun(runId: string, patch: Partial<JobRunRecord>): Promise<void>

  getStaleRuns(now: Date): Promise<JobRunRecord[]>
  appendEvent(event: OutboxEvent): Promise<void>
}
```

Store interface’in temiz olmasi ileride backend degisimini kolaylastirir.

---

# 16. Scheduler Algorithm

## 16.1 Core loops

Leyla’da en az 3 loop olmali:

### Loop A: due generation / schedule advancement

* jobs tablosunu tarar
* next run hesaplar
* gerekirse yeni `scheduled` run olusturur

### Loop B: claim + dispatch

* due run’lari atomik claim eder
* executor’a dispatch eder

### Loop C: recovery / reaper

* stale lease
* stuck running
* timed out jobs
* retry_wait -> scheduled transition

## 16.2 Pseudocode

```ts
while (running) {
  await materializeDueRuns(now)
  const claimed = await claimDueRuns(now, instanceId, 50)

  for (const run of claimed) {
    dispatch(run)
  }

  await recoverStaleRuns(now)
  await advanceRetryQueue(now)
  await sleep(pollIntervalMs)
}
```

## 16.3 Important nuance

`nextRunAt` tek basina yeterli degil. Bazi durumlarda materialized `job_runs` kaydi lazim.
Ozellikle:

* replay-all
* audit trail
* duplicate prevention

Bu nedenle schedule hesaplama ile run creation ayri dusunulmeli.

---

# 17. Execution Adapter Design

## 17.1 Contract

```ts
interface LeylaExecutor {
  type: string
  dispatch(run: ResolvedRunContext): Promise<DispatchResult>
  heartbeat?(runId: string): Promise<void>
  cancel?(runId: string): Promise<void>
}
```

## 17.2 Shell executor

En pratik baslangic executor’lerinden biri.

Use case:

* local script calistirma
* CLI workflow tetikleme
* build / backup / sync

Result capture:

* exit code
* stdout
* stderr
* duration

## 17.3 Local handler executor

Plugin icinde registry’ye kayitli handler’lar.

```ts
leyla.register('refresh-cache', async (ctx) => {
  // business logic
})
```

## 17.4 Claude task executor

Claude-specific executor su mantikla calisabilir:

* run olusturulur
* payload durable queue’ya yazilir
* active Claude plugin baglandiginda pending task’lari cekip process eder
* sonuc yine store’a yazilir

Bu executor sync degil, **resumable orchestration executor** olarak tasarlanmalidir.

---

# 18. API Design Proposal

## 18.1 User-facing API

```ts
const leyla = createLeyla({
  store,
  executors,
  logger,
})

await leyla.schedule({
  id: 'daily-report',
  name: 'Daily report generation',
  schedule: { type: 'cron', expression: '0 9 * * *', timezone: 'Europe/Istanbul' },
  executor: { type: 'shell', command: 'node', args: ['scripts/report.js'] },
  retry: { maxAttempts: 5, strategy: 'exponential', baseDelayMs: 1000 },
  concurrency: { type: 'forbid-overlap' },
  misfire: { type: 'coalesce' },
})
```

## 18.2 Operational API

```ts
await leyla.pause('daily-report')
await leyla.resume('daily-report')
await leyla.trigger('daily-report')
await leyla.cancelRun(runId)
await leyla.retryRun(runId)
await leyla.deleteJob('daily-report')
```

## 18.3 Inspection API

```ts
await leyla.getJob('daily-report')
await leyla.listRuns({ jobId: 'daily-report', status: 'failed' })
await leyla.getStats('daily-report')
await leyla.getDeadLetters()
```

---

# 19. CLI Design Proposal

Claude Code icin CLI entegrasyonu cok degerli olur.

## Commands

```bash
leyla job add
leyla job list
leyla job inspect <jobId>
leyla job pause <jobId>
leyla job resume <jobId>
leyla run trigger <jobId>
leyla run inspect <runId>
leyla run retry <runId>
leyla run cancel <runId>
leyla doctor
leyla recover
```

## Most valuable command

### `leyla doctor`

Sunlari kontrol eder:

* stale leases
* overdue runs
* dead letters
* misconfigured jobs
* clock skew warning
* executor availability

Bu command operasyonel fark yaratir.

---

# 20. Observability Model

## 20.1 Logs

Her run icin structured logs:

* jobId
* runId
* attempt
* scheduledFor
* startedAt
* durationMs
* status
* errorCode
* leaseOwner

## 20.2 Metrics

Minimum metric set:

* jobs_total
* runs_started_total
* runs_succeeded_total
* runs_failed_total
* run_duration_ms
* overdue_runs
* stale_leases
* retry_queue_size
* dead_letter_count

## 20.3 Traces

Ozellikle remote executor veya webhook oldugunda useful.
RunId trace correlation key olmali.

## 20.4 Why this matters

Scheduler’larin en buyuk problemi “sessizce bozulma”dir. Observability olmadan scheduler var gibi gozukur ama aslinda hicbir seye guvenilmez.

---

# 21. Failure Modes and Recovery Strategy

## 21.1 Plugin crash during execution

Durum:

* run running oldu
* plugin crash oldu

Cozum:

* heartbeat stale olur
* recovery loop run’i orphaned / retry_wait durumuna alir
* policy’ye gore tekrar dispatch eder

## 21.2 Crash after dispatch before startedAt

Durum:

* dispatch edildi ama worker cevap vermedi

Cozum:

* dispatch timeout
* status leased/dispatched stale sayilir
* safe re-dispatch mekanizmasi gerekir

## 21.3 Duplicate dispatch

Durum:

* network race / timeout / restart

Cozum:

* runId bazli idempotency
* lease ownership check
* executor side dedupe token

## 21.4 Clock skew

Ozellikle distributed deployment’ta onemli.
Tavsiye:

* DB clock’a daha fazla guven
* app server now() farkini monitor et

## 21.5 Infinite retry storm

Cozum:

* capped backoff
* DLQ
* circuit breaker style pause

---

# 22. Security and Trust Boundaries

## 22.1 Shell executor riskli

Shell executor full guc verir. Bu nedenle:

* allowlist command opsiyonu
* working directory isolation
* env filtering
* output truncation
* timeout hard-kill

## 22.2 Secret handling

Job definitions icinde plaintext secret tutma.
Onun yerine:

* env reference
* secret provider key
* masked logging

## 22.3 Auditability

Administrative aksiyonlar da loglanmali:

* who paused job
* who retried run
* who changed schedule

---

# 23. Testing Strategy

## 23.1 Unit tests

* cron parsing
* next run calculation
* retry backoff
* misfire policy
* concurrency rules

## 23.2 Integration tests

* sqlite store transactional claim
* crash recovery
* stale lease reaping
* timeout behavior
* dead-letter transitions

## 23.3 Deterministic time tests

Fake clock kullan.
Scheduler test’lerinde real time kullanmak testleri kirilgan yapar.

## 23.4 Chaos tests

Ozellikle su senaryolari simule et:

* crash after claim
* crash after dispatch
* network timeout
* duplicate executor ack
* system sleep / long pause

---

# 24. Example End-to-End Flows

## 24.1 Daily shell job

1. job tanimi kaydedilir
2. `nextRunAt` hesaplanir
3. zaman gelince `job_run` olusur
4. scheduler claim eder
5. shell executor script’i baslatir
6. result store’a yazilir
7. next schedule hesaplanir

## 24.2 Claude-resume task flow

1. job due olur
2. `claude-task` executor pending intent yazar
3. session kapaliysa task bekler
4. Claude plugin tekrar baglanir
5. pending intent’leri okur
6. uygun task’i calistirir
7. result store’a commit eder

## 24.3 Failure + retry flow

1. webhook executor 503 alir
2. run failed olur
3. retry policy retry_wait hesaplar
4. belirlenen zamanda tekrar scheduled olur
5. tekrar claim edilir
6. basarili olursa succeeded
7. degilse DLQ

---

# 25. Recommended Defaults

## System defaults

```ts
{
  pollIntervalMs: 1000,
  claimBatchSize: 50,
  defaultLeaseTtlMs: 30000,
  defaultTimeoutMs: 300000,
  staleHeartbeatMs: 60000,
  maxOutputBytes: 65536,
  defaultMisfirePolicy: { type: 'coalesce' },
  defaultConcurrency: { type: 'forbid-overlap' },
  defaultRetry: {
    maxAttempts: 5,
    strategy: 'exponential',
    baseDelayMs: 1000,
    maxDelayMs: 60000,
    jitter: true,
    retryOn: ['timeout', 'network', 'rate-limit', 'unknown']
  }
}
```

## Why these defaults good

* operasyonel olarak guvenli
* runaway overlap riskini azaltir
* session gap sonrasinda flood yaratmaz

---

# 26. Anti-Patterns to Avoid

## 26.1 In-memory only scheduling

Claude Code context’i icin yanlis default.

## 26.2 nextRunAt only, no run ledger

Recovery ve audit zayif olur.

## 26.3 No lease model

Duplicate dispatch kacınılmaz hale gelir.

## 26.4 Retry everything blindly

Bug veya config hatasinda sonsuz fail loop olusur.

## 26.5 Business logic’i scheduler core’a gommek

Executor abstraction bozulur, sistem buyuyemez.

## 26.6 No misfire policy

Session gap sonrasinda sistem davranisi belirsiz olur.

---

# 27. Recommended Roadmap

## Phase 1 — Durable local scheduler

* sqlite store
* cron/interval/once
* local handler + shell executor
* run ledger
* lease claiming
* retry / timeout / DLQ
* CLI inspection

## Phase 2 — Claude-aware resumable execution

* claude-task executor
* pending intent queue
* resume polling / pickup
* backlog handling
* session-recovery UX

## Phase 3 — Shared deployment

* postgres store
* multi-instance lease safety
* remote worker executor
* metrics export

## Phase 4 — Productization

* dashboard
* advanced policies
* DAG / dependencies
* tenancy / permissions

---

# 28. Strong Opinionated Recommendation

Eger amacin gercekten fark yaratan bir sey cikarmaksa, Leyla’yi **cron parser etrafinda** degil **durable run ledger etrafinda** tasarla.

Yani product’in kalbi su olmamali:

* "cron expression alip callback cagiriyorum"

Kalbi su olmali:

* "ben schedule intent’ini durable sekilde tutuyorum, execution truth’u kaydediyorum, runtime kaybolsa bile toparliyorum"

Bu ayrim Leyla’yi oyuncak olmaktan cikarir.

---

# 29. Suggested Technical Stack

## For the corrected target stack

* **Language:** Rust
* **Runtime:** native CLI binary
* **Primary embedding target:** Claude Code terminal / CLI plugin surface
* **Local store:** SQLite
* **Shared store later:** PostgreSQL
* **Time handling:** `time` crate or `chrono`
* **Cron parsing:** stable Rust cron parser crate
* **Serialization:** `serde`
* **Validation:** typed domain model + schema validation at command boundary
* **CLI:** `clap`
* **Async runtime:** `tokio`
* **Logging / tracing:** `tracing`

## Why

Bu hedef icin Rust daha dogru cunku:

* tek binary dagitimi kolay
* CLI/plugin ergonomisi guclu
* state machine + scheduler core icin tip guvenligi yuksek
* dusuk resource ile detached worker / daemon benzeri model kurulabilir
* opensource dagitim ve cross-platform release daha temiz olur

---

# 30. Concrete V1 Deliverables

## Must-have code artifacts

* `LeylaEngine`
* `LeylaStore` interface
* `SqliteLeylaStore`
* `RunLifecycleManager`
* `DueRunMaterializer`
* `LeaseManager`
* `RecoveryManager`
* `ShellExecutor`
* `LocalHandlerExecutor`
* `leyla doctor` command
* migration files
* test harness with fake clock

## Must-have docs

* architecture doc
* failure semantics doc
* job definition schema
* operational playbook
* migration guide for Postgres

---

# 31. Example Type Skeleton

```ts
export interface LeylaEngineOptions {
  store: LeylaStore
  executors: Record<string, LeylaExecutor>
  clock?: LeylaClock
  logger?: LeylaLogger
  instanceId?: string
  pollIntervalMs?: number
}

export interface LeylaEngine {
  start(): Promise<void>
  stop(): Promise<void>

  schedule(job: LeylaJob): Promise<void>
  unschedule(jobId: string): Promise<void>
  pause(jobId: string): Promise<void>
  resume(jobId: string): Promise<void>

  trigger(jobId: string, input?: unknown): Promise<string>
  getJob(jobId: string): Promise<LeylaJobDetails | null>
  listJobs(): Promise<LeylaJobSummary[]>
  listRuns(filter?: LeylaRunFilter): Promise<LeylaRunSummary[]>
}
```

---

# 32. Final Architecture Verdict

## Best architectural identity for Leyla

**Leyla = durable scheduler + resumable orchestration layer for session-unstable agent environments**

Bu identity dogru cunku Claude Code’in asil problemi cron eksikligi degil, **runtime continuity eksikligi**.

## Final recommendation in one sentence

Leyla’yi callback calistiran bir timer sistemi olarak degil, runtime yok oldugunda bile is niyetini, execution state’ini ve recovery semantiklerini koruyan bir orchestration engine olarak tasarla.

---

# 33. Next Step Recommendation

Bu dokumandan sonra en dogru sonraki artefakt sirasi su:

1. **ADR seti**

   * why durable-first
   * why run ledger
   * why lease model
   * why coalesce default misfire

2. **Monorepo folder structure**

3. **TypeScript domain model**

4. **SQLite schema**

5. **Engine pseudocode -> production skeleton**

6. **Claude plugin integration contract**

---

# 34. Claude Code-specific V1 reframing (FINAL, corrected with resume insight)

Bu bolum artik senin gercek use-case’ine %100 aligned.

## Core reality (net kural)

Claude icin 3 farkli capability var:

1. `claude -p` → **non-interactive execution (scheduler-friendly)**
2. `claude resume` → **interactive session restore (human-friendly)**
3. active session → **live orchestration**

Ve en kritik ayrim:

> `resume` execution degil, **UX / context restore** mekanizmasi

---

## Leyla’nin FINAL execution modeli

Leyla **iki modlu** olmali:

### MODE 1 — Detached Execution (default, production)

```text
scheduler → prompt builder → claude -p → result → store
```

Ozellikler:

* deterministic
* headless
* retryable
* automation-friendly

### MODE 2 — Interactive Resume (debug / manual continuation)

```text
user → /leyla debug → claude resume <sessionId>
```

Ozellikler:

* human-in-the-loop
* context-rich
* debugging icin ideal

---

## Hybrid Flow (senin istediginin dogru hali)

### Schedule aninda:

Leyla sunlari kaydeder:

```ts
{
  jobId,
  sessionId,
  taskSnapshot,
  executionMode: 'detached' | 'interactive'
}
```

### Execution zamani:

#### Case A — default (detached)

```bash
claude -p "<rebuilt prompt from snapshot>"
```

#### Case B — user debug ister

```bash
claude resume <sessionId>
```

---

## CRITICAL DESIGN DECISION

### ❌ YANLIS

```text
scheduler → claude resume → is calisir
```

### ✅ DOGRU

```text
scheduler → claude -p → is calisir
           ↓
        (opsiyonel)
           ↓
   claude resume → debug / inspect
```

---

## Why resume is NOT execution engine

`claude resume`:

* TTY ister
* interactive acilir
* otomatik komut calistirmaz
* deterministik degildir

Bu nedenle:

> Resume = execution degil, **state visualization + continuation**

---

## Leyla Architecture (final simplified)

```text
          ┌──────────────────────┐
          │   Claude CLI User    │
          │   (/leyla commands)  │
          └─────────┬────────────┘
                    │
                    v
          ┌──────────────────────┐
          │   Leyla CLI (Rust)   │
          │  - command parser    │
          │  - snapshot builder  │
          └─────────┬────────────┘
                    │
                    v
          ┌──────────────────────┐
          │   SQLite Store       │
          └─────────┬────────────┘
                    │
                    v
          ┌──────────────────────┐
          │   Leyla Daemon       │
          │  (always running)    │
          └─────────┬────────────┘
                    │
        ┌───────────┴────────────┐
        │                        │
        v                        v
┌───────────────┐      ┌──────────────────┐
│ claude -p     │      │ claude resume    │
│ (execution)   │      │ (debug / manual) │
└───────────────┘      └──────────────────┘
```

---

## Task Snapshot (FINAL form)

Resume’a guvenmek yerine snapshot critical.

```ts
export type TaskSnapshot = {
  task: string
  cwd: string
  files?: string[]
  notes?: string
  expectedOutput?: string
  createdFromSessionId?: string
}
```

---

## Prompt Builder (execution backbone)

```text
You are executing a scheduled task.

Task:
{{task}}

Context:
- Working directory: {{cwd}}
- Relevant files: {{files}}

Instructions:
{{notes}}

Produce the expected output.
```

---

## FINAL PRODUCT DEFINITION

> **Leyla = Rust ile yazilmis, Claude Code CLI icin, `/leyla` komutu ile task snapshot alip bunu schedule eden ve zamani gelince `claude -p` ile deterministic sekilde calistiran; `claude resume` ile de debug / continuation saglayan opensource scheduler plugin.**

---

## FINAL INSIGHT (en net hali)

* `claude -p` → execution engine
* `claude resume` → human interface
* Leyla → orchestration katmani

---

## FINAL SLOGAN

> **Leyla: Claude kapaninca bile isi unutmayan, zamani gelince calistiran scheduler.**

Bu scope duzeltmesine gore Leyla V1’in en net urun tarifi su olmali:

## User flow

1. Kullanici Claude Code terminalde `/leyla` komutunu verir
2. Leyla o anki isi parse eder
3. o anki session’dan yeterli **context snapshot** alir
4. bir **schedule kaydi** ve **execution policy** olusturur
5. vakti gelince ya:

   * ayni ise yakin bir **resume flow** baslatir
   * ya da **detached ayri session / subprocess / worker execution** tetikler
6. sonuc durable store’a yazilir
7. kullanici donunce sonucu gorebilir, retry edebilir, devam ettirebilir

## V1 feature cut

* `/leyla in 2h ...`
* `/leyla tomorrow 09:00 ...`
* `/leyla every day at 09:00 ...`
* current task/context snapshot
* local SQLite persistence
* execution policy:

  * `resume-when-user-returns`
  * `run-detached`
* run history
* retry / failure logs
* inspect/list/cancel commands

## En kritik teknik soru

Leyla’nin kalbi cron degil, su iki capability:

### 1. Context snapshotting

Claude Code terminalde o anki gorevin minimum yeniden-yurutulebilir halini cikarmak:

* command intent
* working directory
* relevant files
* optional notes/prompt
* execution mode

### 2. Resume semantics

Kaydedilen is, gelecekte tekrar calistiginda ne kadar ayni context’i geri getirecek?

* full transcript restore mi?
* distilled task snapshot mi?
* file refs + instruction pack mi?

V1 icin en saglikli secim:

> **full session clone degil, distilled resumable task snapshot**

Cunku tam session replay hem kirilgan hem ortam-bagimli olur.

---

# 35. Final verdict after scope correction

Senin tarif ettigin Leyla aslinda su:

> **Rust ile yazilan, Claude Code terminal icin `/leyla` komutundan dogan, session-aware task snapshot alip bunu schedule eden ve zamani gelince resume veya detached execution yapabilen opensource scheduler plugin.**

Bu haliyle evet, benim onceki dokuman daha kapsamliydi; ama iyi yani su:

* altyapi dogru yerde kuruldu
* simdi onu daraltip tam senin istedigin urune indirebiliriz

Bir sonraki en dogru adim artik sunlardan biri:

1. **Rust crate architecture**
2. **`/leyla` command grammar / UX spec**
3. **SQLite schema**
4. **context snapshot model**
5. **detached execution strategy**
6. **MVP repo skeleton**

Bu urunun asil vurucu slogani da net:

> **Leyla: Claude Code session kapaninca bile isi unutmayan scheduler.**
