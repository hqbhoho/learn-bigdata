package com.hqbhoho.bigdata.customRPC.io;

import java.io.Serializable;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class RPCResponse implements Serializable {
    private String requestId;  // 请求ID
    private int status;        // 状态码
    private String errorMessage;   // 错误信息
    private Object data;  // 数据

    public RPCResponse() {
    }

    public RPCResponse(String requestId, int status, String errorMessage, Object data) {
        this.requestId = requestId;
        this.status = status;
        this.errorMessage = errorMessage;
        this.data = data;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
