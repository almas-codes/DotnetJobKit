namespace DotnetJobKit.Execution;

public static class ExecutionKernelDiagnostics
{
    public static long DeadlineEventsScheduled;
    public static long DeadlineEventsProcessed;
    public static long StaleDeadlineEventsDiscarded;
    public static long SchedulerWakeups;
    public static long MaintenanceBatches;
    public static long MaintenanceJobs;

    public static void Reset()
    {
        DeadlineEventsScheduled = 0;
        DeadlineEventsProcessed = 0;
        StaleDeadlineEventsDiscarded = 0;
        SchedulerWakeups = 0;
        MaintenanceBatches = 0;
        MaintenanceJobs = 0;
    }
}
