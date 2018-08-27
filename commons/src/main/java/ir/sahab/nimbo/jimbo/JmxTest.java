package ir.sahab.nimbo.jimbo;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class JmxTest {
    public static void main(String[] args) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        SystemConfig conf = new SystemConfig();
        ObjectName name = null;
        try {
            name = new ObjectName("ir.sahab.nimbo.jimbo:type=SystemConfig");
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
        try {
            mbs.registerMBean(conf, name);
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            e.printStackTrace();
        }

        while (true) {

        }

    }
}
