package rb.ks.builder.bootstrap;

/**
 * Basic abstract class that inherits from Thread class and implements Bootstraper interface.
 */
public abstract class ThreadBootstraper extends Thread implements Bootstraper {

    /**
     * Interrupt thread and close bootstraper
     */
    @Override
    public void interrupt() {
        close();
    }
}
