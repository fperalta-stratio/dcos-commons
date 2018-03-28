package com.mesosphere.sdk.framework;

/**
 * This class provides exit codes for the scheduler process.
 */
public class ProcessExit {

    // Commented items are no longer used, but their numbers may be repurposed later
    public static final ProcessExit SUCCESS = new ProcessExit(0);
    public static final ProcessExit INITIALIZATION_FAILURE = new ProcessExit(1);
    public static final ProcessExit REGISTRATION_FAILURE = new ProcessExit(2);
    //public static final SchedulerErrorCode RE_REGISTRATION = new SchedulerErrorCode(3);
    //public static final SchedulerErrorCode OFFER_RESCINDED = new SchedulerErrorCode(4);
    public static final ProcessExit DISCONNECTED = new ProcessExit(5);
    public static final ProcessExit ERROR = new ProcessExit(6);
    //public static final SchedulerErrorCode PLAN_CREATE_FAILURE = new SchedulerErrorCode(7);
    public static final ProcessExit LOCK_UNAVAILABLE = new ProcessExit(8);
    public static final ProcessExit API_SERVER_ERROR = new ProcessExit(9);
    //public static final SchedulerErrorCode SCHEDULER_BUILD_FAILED = new SchedulerErrorCode(10);
    public static final ProcessExit SCHEDULER_ALREADY_UNINSTALLING = new ProcessExit(11);
    //public static final SchedulerErrorCode SCHEDULER_INITIALIZATION_FAILURE = new SchedulerErrorCode(12);
    public static final ProcessExit DRIVER_EXITED = new ProcessExit(13);

    private final int value;

    private ProcessExit(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Immediately exits the process with the ordinal value of the provided {@link ProcessExit}.
     */
    @SuppressWarnings("DM_EXIT")
    public static void exit(ProcessExit code) {
        String message = String.format("Process exiting immediately with code: %s[%d]", code, code.getValue());
        System.err.println(message);
        System.out.println(message);
        System.exit(code.getValue());
    }

    /**
     * Similar to {@link #exit(ProcessExit)}, except also prints the stack trace of the provided exception before
     * exiting the process. This may be used in contexts where the process is exiting in response to a thrown exception.
     */
    public static void exit(ProcessExit errorCode, Throwable e) {
        e.printStackTrace(System.err);
        e.printStackTrace(System.out);
        exit(errorCode);
    }
}
