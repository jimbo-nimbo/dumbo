package ir.sahab.nimbo.jimbo;


// Mostafa: What the heck is this?
public class SystemConfig implements SystemConfigMBean {

    @Override
    public String config() {
        return "Hello JMX!";
    }
}
