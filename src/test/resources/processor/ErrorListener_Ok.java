package processor;

import pcosta.kafka.api.PlatformError;
import pcosta.kafka.api.PlatformErrorListener;
import pcosta.kafka.api.annotation.ErrorListener;

/**
 * @author Pedro Costa
 */
@ErrorListener
public class ErrorListener_Ok implements PlatformErrorListener {
    @Override
    public void onError(final PlatformError error) {
        // empty
    }
}
