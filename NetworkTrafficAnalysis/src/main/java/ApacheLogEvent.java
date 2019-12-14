
//83.149.9.216 - - 17/05/2015:10:05:57 +0000 GET /presentations/logstash-monitorama-2013/css/fonts/Roboto-Bold.ttf

public class ApacheLogEvent {
    private String ip;
    private  String userId;
    private Long eventTime;
    private String method;
    private String url;

    public ApacheLogEvent() {}

    public ApacheLogEvent(String ip, String userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
