/*erResult(orderId: Long, eventType: String)*/

public class OrderResult {
    private Long orderId;
    private String eventType;

    public OrderResult() {
    }

    public OrderResult(Long orderId, String eventType) {
        this.orderId = orderId;
        this.eventType = eventType;
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
}
