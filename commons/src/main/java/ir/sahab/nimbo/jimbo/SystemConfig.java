package ir.sahab.nimbo.jimbo;

public class SystemConfig implements SystemConfigMBean {

    @Override
    public String config() {
        return "Hello JMX!";
    }
}
