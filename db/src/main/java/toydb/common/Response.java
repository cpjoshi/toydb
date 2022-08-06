package toydb.common;

public class Response<T> {
    private StatusCode status;
    private T result = null;

    public Response(StatusCode status) {
        this.status = status;
    }

    public Response(StatusCode status, T result) {
        this.status = status;
        this.result = result;
    }

    public StatusCode getStatus() {
        return status;
    }

    public T getResult() {
        return result;
    }
}
