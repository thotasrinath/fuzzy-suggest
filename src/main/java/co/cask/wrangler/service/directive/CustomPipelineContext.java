package co.cask.wrangler.service.directive;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.NoopMetrics;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.TransientStore;

import java.net.URL;
import java.util.Collections;
import java.util.Map;

public class CustomPipelineContext implements ExecutorContext {

    private ExecutorContext.Environment environment;

    private final DatasetContextLookupProvider lookupProvider;
    private final TransientStore store;

    public CustomPipelineContext(Environment environment, DatasetContext serviceContext, TransientStore store) {
        this.environment = environment;
        this.lookupProvider = new DatasetContextLookupProvider(serviceContext);
        this.store = store;
    }

    /**
     * @return Environment this context is prepared for.
     */
    @Override
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * @return Measurements handler.
     */
    @Override
    public StageMetrics getMetrics() {
        return NoopMetrics.INSTANCE;
    }

    /**
     * @return Context name.
     */
    @Override
    public String getContextName() {
        return "StageContext";
    }

    /**
     * @return
     */
    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    /**
     * Returns a valid service url.
     *
     * @param applicationId id of the application to which a service url.
     * @param serviceId     id of the service within application.
     * @return URL if service exists, else null.
     */
    @Override
    public URL getService(String applicationId, String serviceId) {
        return null;
    }

    @Override
    public TransientStore getTransientStore() {
        return store;
    }

    /**
     * @return Properties associated with run and pipeline.
     */
    @Override
    public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return lookupProvider.provide(s, map);
    }
}
