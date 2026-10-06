# ⚡ DotnetJobKit

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET Version](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-512BD4.svg)](https://dotnet.microsoft.com/)
[![C#](https://img.shields.io/badge/C%23-13.0-239120.svg)](https://learn.microsoft.com/en-us/dotnet/csharp/)
[![Database Providers](https://img.shields.io/badge/Storage-PostgreSQL%20%7C%20SQLite%20%7C%20MySQL%20%7C%20InMemory-00758F.svg)](#-storage-providers)
[![Zero Heavy Dependencies](https://img.shields.io/badge/Dependencies-Zero%20Bloat-brightgreen.svg)](#-why-dotnetjobkit)

### **The Lightweight, High-Performance Background Job Runtime for Modern .NET**

*Durable, transaction-safe background services and task processing for ASP.NET Core — without the overhead, database locks, or commercial licensing of Hangfire and Quartz.NET.*

[Quick Start](#-quick-start-in-60-seconds) • [Why DotnetJobKit?](#-why-dotnetjobkit-vs-hangfire-vs-quartznet) • [Core Features](#-key-features) • [Transactional Enqueue](#-transactional-enqueue-outbox-pattern) • [Benchmarks](#-performance--benchmarks) • [Architecture](#-how-it-works) • [Documentation](docs/)

</div>

---

## 🚀 Overview

**DotnetJobKit** is a fast, resilient, and minimal background task engine designed specifically for modern .NET (`.NET 8`, `.NET 9`, and `.NET 10`).

If you have ever built background workers in C#, you have likely faced the classic dilemma:
- **`System.Threading.Channels` or `BackgroundService`**: Super fast, but **ephemeral**. If your server or container restarts, all enqueued tasks in memory are permanently lost.
- **Hangfire**: Durable, but brings a heavy footprint, reflection-heavy expression trees, complex schema migrations, polling overhead, and expensive commercial licenses for core enterprise needs.
- **Quartz.NET**: Reliable, but carries decades of Java-era legacy baggage, verbose XML/DB triggers, and clumsy state management.

**DotnetJobKit bridges the gap:**
It gives you **fully durable ACID database persistence**, **atomic job claiming**, **transactional enqueueing (Outbox Pattern)**, and **instant wake-up signals**, all wrapped in a clean, idiomatic modern .NET API with **zero bloat**.

---

## ⚖️ Why DotnetJobKit? (vs. Hangfire vs. Quartz.NET)

| Capability | DotnetJobKit | Hangfire | Quartz.NET | Channel + BackgroundService |
| :--- | :---: | :---: | :---: | :---: |
| **Durability** | ✅ Database-backed (ACID) | ✅ Database-backed | ✅ Database-backed | ❌ In-memory only (lost on crash) |
| **Dependency Footprint** | 🪶 Ultra-lightweight (Zero bloat) | 📦 Heavy (multiple assemblies) | 📦 Heavy (legacy hierarchy) | 🪶 Zero |
| **License** | 🟢 **100% Free & Open-Source (MIT)** | 🟡 Paid commercial tiers | 🟢 Free (Apache 2.0) | 🟢 Free |
| **Concurrency Claiming** | ⚡ `SKIP LOCKED` / Atomic Fencing | ⚠️ Polling table locks | ⚠️ Clustered DB locks | ⚡ Local queue |
| **Transactional Enqueue (Outbox)** | ✅ Native EF Core Transaction support | ⚠️ Separate storage/extension | ❌ Complex setup | ❌ Not durable |
| **Worker Wake-Up** | 🔔 Instant In-Memory Signal + Timer | ⏱️ Constant DB Polling | ⏱️ Timer polling | 🔔 In-memory channel |
| **Memory at 10M+ Rows** | 📉 Bounded to `MaxConcurrency` | ⚠️ Risk of large memory spike | ⚠️ Large trigger footprint | ❌ OOM crash risk |
| **DB Lock Duration** | ⚡ Microsecond claim; handler runs outside DB tx | ⚠️ Long locks during dispatch | ⚠️ Transaction locks | ⚡ N/A |
| **Contract-Based Jobs** | ✅ Strongly typed payloads | ⚠️ Serialized Method Expressions | ⚠️ `IJob` untyped data map | ✅ C# Types |

---

## 🎯 Key Features

- **⚡ Atomic Multi-Node Claiming**: Workers atomically claim batches up to `MaxConcurrency`. On PostgreSQL, it utilizes high-speed `FOR UPDATE SKIP LOCKED` for zero-contention distribution across multiple worker instances.
- **🛡️ Native Outbox Pattern (Transactional Enqueue)**: Enqueue background jobs inside the **exact same database transaction** as your Entity Framework Core business writes. If your business transaction rolls back, the job rolls back. If it commits, the job is guaranteed to run!
- **🔔 Zero-Waste Wake Signals (`IJobWakeSignal`)**: Workers do not aggressively poll your database when idle. They calculate exact sleep deadlines to the next `EligibleAt` time and wake up instantaneously when a new job arrives.
- **⏱️ Monotonic Lease Fencing & Crash Resilience**: Handlers execute completely *outside* database transactions (so third-party HTTP calls or long calculations never hold open database locks). If a worker process crashes mid-execution, its expired lease is automatically reclaimed by surviving workers.
- **💀 Dead-Letter Queue & Retries**: Built-in retry limits with exponential delay. Once `MaxAttempts` is exhausted, jobs transition safely to `Dead` for inspection.
- **🚦 Multi-Queue Partitioning**: Organize workloads into dedicated queues (e.g., `"default"`, `"high-priority"`, `"emails"`, `"reports"`).
- **🔌 Pluggable Storage Backends**: First-class support for **PostgreSQL**, **SQLite**, **MySQL**, and **In-Memory** (for fast unit testing).
- **📊 Modern .NET First**: Full async/await with `ValueTask`, structured logging via `Microsoft.Extensions.Logging`, dependency injection, and native `IHostedService` lifecycle management.

---

## 📦 Supported Storage Providers

| Provider | Best Used For | High-Throughput Mechanism |
| :--- | :--- | :--- |
| **PostgreSQL** (`DotnetJobKit.PostgreSql`) | Cloud & Distributed High-Scale Production | `FOR UPDATE SKIP LOCKED` atomic claims + `COPY` binary bulk ingestion (>57,000 jobs/sec) |
| **SQLite** (`DotnetJobKit.Sqlite`) | Single-Node, Monoliths, Edge Devices, Local Dev | WAL mode (`Write-Ahead Logging`) + indexed dispatch queues |
| **MySQL** (`DotnetJobKit.MySql`) | MySQL / MariaDB Production Stacks | Bounded atomic leasing and indexed dispatch |
| **In-Memory** (`DotnetJobKit`) | Unit & Integration Tests | Thread-safe, non-durable in-memory state engine |

---

## ⚡ Quick Start in 60 Seconds

### 1. Install the NuGet Package

```bash
# Core runtime
dotnet add package DotnetJobKit

# Choose your database provider:
dotnet add package DotnetJobKit.PostgreSql
# or
dotnet add package DotnetJobKit.Sqlite
# or
dotnet add package DotnetJobKit.MySql
```

---

### 2. Define a Job & Handler

A job is a simple, serializable C# record. A handler implements `IJobHandler<T>`:

```csharp
// 1. The Job Payload (Strongly Typed & Version Tolerant)
public record SendWelcomeEmailJob(Guid UserId, string EmailAddress);

// 2. The Handler (Automatically resolved via Dependency Injection)
public class SendWelcomeEmailHandler : IJobHandler<SendWelcomeEmailJob>
{
    private readonly IEmailService _emailService;
    private readonly ILogger<SendWelcomeEmailHandler> _logger;

    public SendWelcomeEmailHandler(IEmailService emailService, ILogger<SendWelcomeEmailHandler> logger)
    {
        _emailService = emailService;
        _logger = logger;
    }

    public async ValueTask HandleAsync(SendWelcomeEmailJob job, JobExecutionContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Sending welcome email to {Email} (Attempt {Attempt})", job.EmailAddress, context.AttemptCount);
        
        await _emailService.SendAsync(job.EmailAddress, "Welcome aboard!", cancellationToken);
    }
}
```

---

### 3. Register in `Program.cs`

Configure the coordinator and storage in your ASP.NET Core Minimal API or Worker host:

```csharp
var builder = WebApplication.CreateBuilder(args);

// 1. Add DotnetJobKit Runtime
builder.Services.AddDotnetJobKit(options =>
{
    options.MaxConcurrency = 16;
    options.Queues = ["default", "notifications"];
    options.Retention.DeleteOnSuccess = true; // Automatically clean up completed rows
});

// 2. Select Storage Provider (e.g. PostgreSQL or SQLite)
builder.Services.AddDotnetJobKitPostgreSql(builder.Configuration.GetConnectionString("DefaultConnection")!);
// or: builder.Services.AddDotnetJobKitSqlite("Data Source=jobs.db");

// 3. Register Job Handlers
builder.Services.AddDotnetJobHandler<SendWelcomeEmailJob, SendWelcomeEmailHandler>(
    contract: "email.welcome.send",
    queue: "notifications");

var app = builder.Build();
```

---

### 4. Enqueue or Schedule Jobs

Inject `IJobClient` anywhere in your controllers, endpoints, or services:

```csharp
app.MapPost("/register", async (RegisterUserRequest request, IJobClient jobs) =>
{
    // ... create user in database ...

    // ⚡ Enqueue to run immediately in the background
    await jobs.EnqueueAsync(new SendWelcomeEmailJob(user.Id, user.Email));

    // ⏰ Schedule to run in the future (e.g., follow-up check after 24 hours)
    await jobs.ScheduleAsync(
        new SendFollowUpSurveyJob(user.Id), 
        DateTimeOffset.UtcNow.AddHours(24));

    return Results.Ok(new { message = "User registered successfully" });
});

app.Run();
```

---

## 🛡️ Transactional Enqueue (Outbox Pattern)

The biggest flaw in typical queue setups is the **Dual-Write Problem**: if you save data to your database and then enqueue a job over the network, your application can crash halfway through, leaving your system in an inconsistent state.

With `DotnetJobKit.EntityFrameworkCore`, you can enlist job submissions directly inside your **existing Entity Framework Core transaction**:

```csharp
using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

// 1. Write domain data
dbContext.Orders.Add(new Order { Id = orderId, Total = 99.99m });
await dbContext.SaveChangesAsync(cancellationToken);

// 2. Enqueue job in the SAME database transaction
await jobClient.EnqueueInTransactionAsync(
    dbContext, 
    new ProcessPaymentJob(orderId, 99.99m), 
    cancellationToken);

// 3. Commit both atomically
await transaction.CommitAsync(cancellationToken);
```

> **Guaranteed Consistency**: If the transaction fails, neither the order nor the job is saved. If the transaction commits, the job is guaranteed to be processed.

---

## 🏗️ How It Works

### The State Machine

Every job transitions through clear, deterministic states:

```text
       [ Submit / Enqueue ]
                 │
                 ▼
          ┌──────────────┐
          │    Ready     │ ◄─── (Lease expired or retry backoff)
          └──────┬───────┘
                 │ Worker claims atomically
                 ▼
          ┌──────────────┐
          │    Leased    │
          └───┬──────┬───┘
              │      │
      Success │      │ Handler fails / crashes
              │      ▼
              │   ┌────────────────────────────────┐
              │   │ Attempts < Max?                │
              │   │   YES ──► Ready (Exponential)  │
              │   │   NO  ──► Dead (Dead Letter)   │
              │   └────────────────────────────────┘
              ▼
       ┌──────────────┐
       │  Succeeded   │ ──► (Deleted if DeleteOnSuccess = true)
       └──────────────┘
```

- **`Ready`**: Eligible for execution. The `EligibleAt` timestamp indicates when it can be claimed.
- **`Leased`**: A worker has acquired an exclusive lease. `EligibleAt` becomes the lease expiry deadline.
- **`Succeeded`**: Completed cleanly. Auto-deleted or retained based on retention rules.
- **`Dead`**: Retry attempts exhausted without success. Safe from accidental re-execution.
- **`Cancelled`**: Explicitly aborted via cancellation token or API.

### Fenced Ownership & Zero DB Lock Starvation

Unlike engines that keep an open database transaction while executing code:
1. DotnetJobKit performs an **atomic sub-millisecond query** to claim rows and increment `AttemptCount`.
2. The database connection is **immediately returned to the connection pool**.
3. The C# handler runs freely on the .NET thread pool.
4. When done, it settles with an atomic query verifying that `AttemptCount` has not changed.

If a worker node drops off the network or experiences an out-of-memory crash, the lease expires naturally and another healthy worker picks it up.

---

## 📊 Performance & Benchmarks

Benchmarked using `BenchmarkDotNet` and the custom high-load CLI tool `DotnetJobKit.PerfRunner`.

### 1,000,000 Ready Jobs (Insert & Claim Throughput)

| Storage Provider | Insert 1,000,000 Jobs | Ingestion Rate | Claim 16 Batch | Memory Footprint (RAM) |
| :--- | :---: | :---: | :---: | :---: |
| **PostgreSQL** | **~17.2 s** | **~57,000+ jobs/s** | **~1.3 s** | **Bounded (~45 MB)** |
| **SQLite (WAL)** | **~72.4 s** | **~13,800 jobs/s** | **~2.3 s** | **Bounded (~32 MB)** |
| **MySQL** | **~12.1 min** | **~1,400 jobs/s** | **~105 ms** | **Bounded (~40 MB)** |

### 10,000,000 Jobs Scalability Stress Test

Under an extreme backlog test with **10 Million rows** in PostgreSQL:
- **Bulk COPY Stream**: 10M rows inserted in ~23 minutes.
- **Memory Consumption**: Remained steady under 150 MB because DotnetJobKit claims only up to `MaxConcurrency` and **never loads the backlog into RAM**.

*(Detailed benchmark methodology and raw JSON telemetry can be reviewed in [docs/PERFORMANCE.md](docs/PERFORMANCE.md) and [docs/PERFORMANCE-REPORT.md](docs/PERFORMANCE-REPORT.md).)*

---

## 🛠️ Configuration & Options

Fine-tune runtime parameters to suit your workload:

```csharp
services.AddDotnetJobKit(options =>
{
    // Maximum concurrent jobs executed per worker instance
    options.MaxConcurrency = 32;

    // Active queues this instance listens to
    options.Queues = ["default", "critical", "webhooks"];

    // Maximum sleep interval when no jobs are pending
    options.MaxIdleSleep = TimeSpan.FromMinutes(1);

    // Default duration of a claim lease
    options.DefaultLeaseDuration = TimeSpan.FromMinutes(5);

    // Retention policy
    options.Retention.DeleteOnSuccess = true;
    options.Retention.PurgeBatchSize = 1000;
});
```

---

## 🧪 Testing Your Handlers

Because DotnetJobKit separates job definitions from execution dispatchers, unit testing your business logic is effortless:

```csharp
[Fact]
public async Task SendWelcomeEmail_CallsServiceWithCorrectParameters()
{
    // Arrange
    var emailServiceMock = new Mock<IEmailService>();
    var handler = new SendWelcomeEmailHandler(emailServiceMock.Object, NullLogger<SendWelcomeEmailHandler>.Instance);
    var job = new SendWelcomeEmailJob(Guid.NewGuid(), "dev@example.com");
    var context = new JobExecutionContext(JobId.New(), attemptCount: 1);

    // Act
    await handler.HandleAsync(job, context, CancellationToken.None);

    // Assert
    emailServiceMock.Verify(x => x.SendAsync("dev@example.com", It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
}
```

For integration tests, simply use `AddDotnetJobKitInMemoryStorage()` to test your full end-to-end enqueue-and-run workflow without spinning up an external database.

---

## 📁 Repository Structure

```text
DotnetJobKit/
├── src/
│   ├── DotnetJobKit/                    # Core runtime, state machine, in-memory store
│   ├── DotnetJobKit.PostgreSql/         # PostgreSQL provider (SKIP LOCKED + COPY ingestion)
│   ├── DotnetJobKit.Sqlite/             # SQLite provider (WAL mode + partial indexes)
│   ├── DotnetJobKit.MySql/              # MySQL provider (row-level locking)
│   └── DotnetJobKit.EntityFrameworkCore # EF Core transactional enqueue enlistment
├── tests/
│   ├── DotnetJobKit.Tests/              # Unit tests for state machine & coordinator
│   └── DotnetJobKit.IntegrationTests/   # Concurrency, lease expiry, crash resilience
├── benchmarks/
│   └── DotnetJobKit.Benchmarks/         # BenchmarkDotNet harness
├── tools/
│   └── DotnetJobKit.PerfRunner/         # Multi-provider 1M & 10M CLI performance runner
├── samples/
│   ├── MinimalApi/                      # ASP.NET Core Minimal API sample
│   └── Worker/                          # Dedicated .NET Background Worker sample
└── docs/                                # Deep-dive architecture and performance reports
```

---

## 🤝 Contributing

Contributions, feedback, and performance optimizations are warmly welcomed!

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/amazing-feature`).
3. Commit your changes (`git commit -m "feat: add support for SQL Server provider"`).
4. Push to the branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request.

---

## 📄 License

DotnetJobKit is licensed under the **[MIT License](LICENSE)**. Free for commercial and personal use.

<div align="center">
Built with ❤️ for .NET developers who crave speed, reliability, and simplicity.
</div>
