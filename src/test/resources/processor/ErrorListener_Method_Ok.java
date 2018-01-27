package processor;

import pcosta.kafka.api.PlatformError;
import pcosta.kafka.api.PlatformErrorListener;
import pcosta.kafka.api.annotation.ErrorListener;

/**
 * @author Pedro Costa
 */
public class ErrorListener_Method_Ok implements PlatformErrorListener {

    @ErrorListener
    public void onError(final PlatformError error) {
        // empty
    }

}
