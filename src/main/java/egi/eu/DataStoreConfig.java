package egi.eu;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;


@ConfigMapping(prefix = "backend")
public interface DataStoreConfig {

    @WithName("class")
    String backendClass();
}
