package onedata;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;


@ConfigMapping(prefix = "datahub")
public interface OneDataConfig {

    @WithName("onezone-host")
    String zoneBaseUrl();

    @WithName("onezone-token")
    String zoneToken();

    @WithName("oneprovider-token")
    String providerToken();

    @WithName("file-token-validity-days")
    int fileTokenValidityDays();

    @WithName("file-token-validity-left-days")
    int fileTokenValidityLeftDays();
}
