package org.apache.hadoop.fs.azurenative;

import static java.net.HttpURLConnection.*;

import com.microsoft.windowsazure.storage.*;


/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update error metrics appropriately when it gets that event.
 */
public class ErrorMetricUpdater extends StorageEvent<ResponseReceivedEvent> {
  private final AzureFileSystemInstrumentation instrumentation;
  private final OperationContext operationContext;

  private ErrorMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation) {
    this.instrumentation = instrumentation;
    this.operationContext = operationContext;
  }

  /**
   * Hooks a new listener to the given operationContext that will update the
   * error metrics for the WASB file system appropriately in response to
   * ResponseReceived events.
   *
   * @param operationContext The operationContext to hook.
   * @param instrumentation The metrics source to update.
   */
  public static void hook(
      OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation) {
    ErrorMetricUpdater listener =
        new ErrorMetricUpdater(operationContext,
            instrumentation);
    operationContext.getResponseReceivedEventHandler().addListener(listener);
  }

  @Override
  public void eventOccurred(ResponseReceivedEvent eventArg) {
    RequestResult currentResult = operationContext.getLastResult();
    int statusCode = currentResult.getStatusCode();
    // Check if it's a client-side error: a 4xx status
    // We exclude 404 because it happens frequently during the normal
    // course of operation (each call to exists() would generate that
    // if it's not found).
    if (statusCode >= 400 && statusCode < 500 &&
        statusCode != HTTP_NOT_FOUND) {
      instrumentation.clientErrorEncountered();
    } else if (statusCode >= 500) {
      // It's a server error: a 5xx status. Could be an Azure Storage
      // bug or (more likely) throttling.
      instrumentation.serverErrorEncountered();
    }
  }
}
