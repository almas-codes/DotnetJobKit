using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.IntegrationTests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

/// <summary>
/// Mirrors ConnectBE interpreter-reminder flows (Pending → Processing → Sent/Skipped/Cancelled)
/// using DotnetJobKit contracts instead of Tbl_InterpreterReminderJob.
/// </summary>
public sealed record InterpreterReminderJobPayload(
    int AppointmentId,
    string InterpreterId,
    string AppointmentType,
    string Channel,
    int HoursBeforeAppointment);

public sealed class InterpreterReminderJobHandler : IJobHandler<InterpreterReminderJobPayload>
{
    public static readonly List<InterpreterReminderJobPayload> Sent = new();
    public static int SkippedNotConfirmed;

    public Task HandleAsync(InterpreterReminderJobPayload job, JobContext context, CancellationToken cancellationToken)
    {
        if (SimulatedAppointmentStore.IsConfirmed(job.AppointmentId))
        {
            lock (Sent)
                Sent.Add(job);
        }
        else
        {
            Interlocked.Increment(ref SkippedNotConfirmed);
        }

        return Task.CompletedTask;
    }
}

internal static class SimulatedAppointmentStore
{
    private static readonly HashSet<int> Confirmed = new();

    public static void SetConfirmed(int appointmentId, bool confirmed)
    {
        lock (Confirmed)
        {
            if (confirmed)
                Confirmed.Add(appointmentId);
            else
                Confirmed.Remove(appointmentId);
        }
    }

    public static bool IsConfirmed(int appointmentId)
    {
        lock (Confirmed)
            return Confirmed.Contains(appointmentId);
    }
}

public class ConnectBeScenarioTests
{
    private static HostApplicationBuilder CreateBuilder(FakeTimeProvider time)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<TimeProvider>(time);
        builder.Services.AddDotnetJobHandler<InterpreterReminderJobPayload, InterpreterReminderJobHandler>(
            "connectbe.interpreter-reminder",
            "notifications");
        builder.Services.AddDotnetJobKit(o =>
        {
            o.MaxConcurrency = 4;
            o.Queues = ["notifications", "default"];
            o.Retention.DeleteOnSuccess = true;
        });
        builder.Services.AddDotnetJobKitInMemoryStorage();
        return builder;
    }

    [Fact]
    public async Task Scheduled_reminder_runs_when_trigger_eligible_like_ConnectBE_Pending()
    {
        InterpreterReminderJobHandler.Sent.Clear();
        SimulatedAppointmentStore.SetConfirmed(1001, true);

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 6, 1, 10, 0, 0, TimeSpan.Zero));
        using var host = CreateBuilder(time).Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var triggerAt = time.GetUtcNow().AddHours(24);
        await submitter.ScheduleAsync(
            new InterpreterReminderJobPayload(1001, "interp-1", "VRI", "Email", 24),
            triggerAt);

        await Task.Delay(300);
        Assert.Empty(InterpreterReminderJobHandler.Sent);

        time.SetUtcNow(triggerAt.AddSeconds(1));
        host.Services.GetRequiredService<IJobWakeSignal>().Notify();

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (InterpreterReminderJobHandler.Sent.Count == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        Assert.Single(InterpreterReminderJobHandler.Sent);
        Assert.Equal("Email", InterpreterReminderJobHandler.Sent[0].Channel);
    }

    [Fact]
    public async Task Cancel_pending_reminder_before_trigger_like_ConnectBE_cancel()
    {
        InterpreterReminderJobHandler.Sent.Clear();
        SimulatedAppointmentStore.SetConfirmed(2002, true);

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero));
        using var host = CreateBuilder(time).Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var store = host.Services.GetRequiredService<IJobStore>();
        var triggerAt = time.GetUtcNow().AddHours(48);
        var jobId = await submitter.ScheduleAsync(
            new InterpreterReminderJobPayload(2002, "interp-2", "OPI", "Sms", 48),
            triggerAt);

        Assert.True(await submitter.CancelAsync(jobId));

        time.SetUtcNow(triggerAt.AddMinutes(1));
        host.Services.GetRequiredService<IJobWakeSignal>().Notify();
        await Task.Delay(500);

        var record = await store.GetAsync(jobId, CancellationToken.None);
        await host.StopAsync();

        Assert.Empty(InterpreterReminderJobHandler.Sent);
        Assert.NotNull(record);
        Assert.Equal(JobState.Cancelled, record!.State);
    }

    [Fact]
    public async Task Multiple_scheduled_reminders_dispatch_in_eligible_order()
    {
        InterpreterReminderJobHandler.Sent.Clear();
        SimulatedAppointmentStore.SetConfirmed(3003, true);

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 7, 1, 8, 0, 0, TimeSpan.Zero));
        using var host = CreateBuilder(time).Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var t0 = time.GetUtcNow();
        await submitter.ScheduleAsync(new InterpreterReminderJobPayload(3003, "i1", "VRI", "Email", 72), t0.AddHours(1));
        await submitter.ScheduleAsync(new InterpreterReminderJobPayload(3003, "i1", "VRI", "Sms", 48), t0.AddHours(2));
        await submitter.ScheduleAsync(new InterpreterReminderJobPayload(3003, "i1", "VRI", "Email", 24), t0.AddHours(3));

        time.SetUtcNow(t0.AddHours(3).AddSeconds(1));
        host.Services.GetRequiredService<IJobWakeSignal>().Notify();

        var deadline = DateTime.UtcNow.AddSeconds(8);
        while (InterpreterReminderJobHandler.Sent.Count < 3 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        // ConnectBE ProcessDueJobsAsync processes earliest ScheduledTriggerAt first (EligibleAt here).
        Assert.Equal(3, InterpreterReminderJobHandler.Sent.Count);
        Assert.Equal(72, InterpreterReminderJobHandler.Sent[0].HoursBeforeAppointment);
        Assert.Equal(48, InterpreterReminderJobHandler.Sent[1].HoursBeforeAppointment);
        Assert.Equal(24, InterpreterReminderJobHandler.Sent[2].HoursBeforeAppointment);
    }

    [Fact]
    public async Task Reminder_skipped_when_appointment_no_longer_confirmed()
    {
        InterpreterReminderJobHandler.Sent.Clear();
        InterpreterReminderJobHandler.SkippedNotConfirmed = 0;
        SimulatedAppointmentStore.SetConfirmed(4004, false);

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 8, 1, 9, 0, 0, TimeSpan.Zero));
        using var host = CreateBuilder(time).Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        await submitter.EnqueueAsync(new InterpreterReminderJobPayload(4004, "i-x", "OPI", "Email", 24));

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (InterpreterReminderJobHandler.SkippedNotConfirmed == 0
               && InterpreterReminderJobHandler.Sent.Count == 0
               && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        await host.StopAsync();
        Assert.Empty(InterpreterReminderJobHandler.Sent);
        Assert.Equal(1, InterpreterReminderJobHandler.SkippedNotConfirmed);
    }
}
