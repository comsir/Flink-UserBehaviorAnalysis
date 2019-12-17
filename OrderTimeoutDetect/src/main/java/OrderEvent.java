/*case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)*/
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String eventType, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
