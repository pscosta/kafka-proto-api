package processor;

import pcosta.kafka.api.PlatformError;
import pcosta.kafka.api.PlatformErrorListener;
import pcosta.kafka.api.annotation.ErrorListener;

/**
 * @author Pedro Costa
 */
@ErrorListener
public class ErrorListener_Multiple_Annotated_Listeners implements PlatformErrorListener {
    @Override
    public void onError(final PlatformError error) {
        // empty
    }

    @ErrorListener
    public class ErrorListener_Multiple_Annotated_Listeners2 implements PlatformErrorListener {
        @Override
        public void onError(final PlatformError error) {
            // empty
        }
    }

}

