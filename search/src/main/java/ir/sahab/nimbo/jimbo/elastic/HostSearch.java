package ir.sahab.nimbo.jimbo.elastic;

public class HostSearch {
        private String hostName;
        private int port;

        HostSearch(String hostWithPort){
            this.hostName = hostWithPort.split(":")[0];
            this.port = Integer.valueOf(hostWithPort.split(":")[1]);
        }
        HostSearch(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
        }

        String getHostName() {
            return hostName;
        }

        int getPort() {
            return port;
        }
}
