package egi.eu;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.Response.Status.Family;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;
import onedata.onezone.model.TokenRequest;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.eclipse.microprofile.rest.client.RestClientDefinitionException;
import org.jboss.logging.Logger;

import onedata.OneDataConfig;
import onedata.onezone.OneZone;
import onedata.oneprovider.OneProvider;
import egi.eu.model.DataStoreItem;


/***
 * Data store specialization for EGI DataHub
 */
public class DataHub implements DataStore {

    private static final String SEPARATOR = "/";
    private Random random = new Random();
    private OneDataConfig config = null;
    private static OneZone onezone;
    private OneProvider provider;
    private static AtomicReference<Map<String, OneProvider>> providers = new AtomicReference<>();
    private static final Logger LOG = Logger.getLogger(DataHub.class);

    public String spaceId;
    public String spaceName;
    public String providerId;
    public String providerBaseUrl;


    /***
     * Prepare REST client for configured OneZone.
     * @return True on success
     */
    private Uni<Boolean> getBackendClient() {
        // Check if the OneZone URL is valid
        URL urlOneZone;
        try {
            urlOneZone = new URL(config.zoneBaseUrl());
        }
        catch (MalformedURLException e) {
            LOG.error(e.getMessage());
            return Uni.createFrom().failure(new ActionException(e, "invalidConfiguration",
                                                      Tuple2.of("onezoneBaseUrl", config.zoneBaseUrl())));
        }

        try {
            // Create the OneZone client with the configured base URL
            if (null == this.onezone) {
                this.onezone = RestClientBuilder.newBuilder()
                                .baseUrl(urlOneZone)
                                .build(OneZone.class);

                LOG.infof("Created REST client for OneZone at %s", config.zoneBaseUrl());
            } else {
                LOG.infof("Using cached REST client for OneZone at %s", config.zoneBaseUrl());
            }

            return Uni.createFrom().item(true);
        }
        catch(IllegalStateException ise) {
            LOG.error(ise.getMessage());
            return Uni.createFrom().failure(new ActionException(ise, "invalidConfiguration",
                                                      Tuple2.of("onezoneBaseUrl", config.zoneBaseUrl())));
        }
        catch(RestClientDefinitionException rcde) {
            LOG.error(rcde.getMessage());
            return Uni.createFrom().failure(new ActionException(rcde, "invalidConfiguration",
                                                      Tuple2.of("onezoneBaseUrl", config.zoneBaseUrl())));
        }
    }

    /***
     * Prepare REST client for the provider with specified base URL.
     * @return Received parameters with updated field "provider"
     */
    private Uni<Boolean> getProviderClient(String baseUrl) {

        // Check if the OneProvider URL is valid
        URL urlProvider;
        try {
            urlProvider = new URL(baseUrl);
            this.providerBaseUrl = baseUrl;
        }
        catch (MalformedURLException e) {
            LOG.error(e.getMessage());
            return Uni.createFrom().failure(new ActionException(e, "invalidConfiguration", Arrays.asList(
                                                      Tuple2.of("spaceName", this.spaceName),
                                                      Tuple2.of("providerId", this.providerId),
                                                      Tuple2.of("oneproviderBaseUrl", this.providerBaseUrl)) ));
        }

        // Check if we already have a provider client with this base URL
        var provs = this.providers.get();
        if(null == provs)
            provs = new HashMap<>();
        else if(provs.containsKey(this.providerBaseUrl)) {
            // We already have a REST client for this provider, use it
            this.provider = provs.get(this.providerBaseUrl);
            LOG.infof("Using cached REST client for OneProvider at %s", this.providerBaseUrl);
            return Uni.createFrom().item(true);
        }

        try {
            // Create and return new provider client
            this.provider = RestClientBuilder.newBuilder()
                                .baseUrl(urlProvider)
                                .build(OneProvider.class);

            provs.put(this.providerBaseUrl, this.provider);

            this.providers.set(provs);

            LOG.infof("Created REST client for OneProvider at %s", this.providerBaseUrl);
            return Uni.createFrom().item(true);
        }
        catch(IllegalStateException ise) {
            LOG.error(ise.getMessage());
            return Uni.createFrom().failure(new ActionException(ise, "invalidConfiguration", Arrays.asList(
                                                      Tuple2.of("spaceName", this.spaceName),
                                                      Tuple2.of("providerId", this.providerId),
                                                      Tuple2.of("oneproviderBaseUrl", this.providerBaseUrl)) ));
        }
        catch(RestClientDefinitionException rcde) {
            LOG.error(rcde.getMessage());
            return Uni.createFrom().failure(new ActionException(rcde, "invalidConfiguration", Arrays.asList(
                                                      Tuple2.of("spaceName", this.spaceName),
                                                      Tuple2.of("providerId", this.providerId),
                                                      Tuple2.of("oneproviderBaseUrl", this.providerBaseUrl)) ));
        }
    }

    /***
     * Get the human-readable name of the data store.
     * @return Name of the transfer service.
     */
    public String name() { return "EGI DataHub"; }

    /***
     * Retrieve root path in this data store
     * @return Root path.
     */
    public String root() { return DataHub.SEPARATOR; }

    /***
     * Retrieve separator used in this data store.
     * @return Separator character (or string).
     */
    public String separator() { return DataHub.SEPARATOR; }

    /***
     * Retrieve id of the space being published.
     * @return Space id
     */
    public String getSpaceId() { return this.spaceId; }

    /***
     * Retrieve name of the space being published.
     * @return Space name
     */
    public String getSpaceName() { return this.spaceName; }

    /***
     * Retrieve URL of the backend service.
     * @return Base URL for the service
     */
    public String getServiceUrl() { return this.providerBaseUrl; }

    /***
     * Initialize the backend data store, in this case DataHub.
     * @param config The configuration of the backend (expects OneDataConfig)
     *               Note: It is an opaque object to allow custom beans that map config properties
     *               to be injected in the resource class. If the implementation of the backend would
     *               be injected instead, its lifetime cannot be controlled efficiently.
     * @param spaceId The space to work with
     * @return Returns the details of the space
     */
    public Uni<DataStoreItem> initialize(Object config, String spaceId) {

        LOG.infof("Initializing backend for space %s", spaceId);

        if(null == config || !OneDataConfig.class.isAssignableFrom(config.getClass())) {
            LOG.error("No backend configuration");
            return Uni.createFrom().failure(new ActionException("noConfig"));
        }

        this.spaceId = spaceId;
        this.config = (OneDataConfig)config;

        Uni<DataStoreItem> result = Uni.createFrom().nullItem()

            .chain(unused -> {
                // Get OneZone REST client
                return getBackendClient();
            })
            .chain(unused -> {
                // Got OneZone REST client
                // Get the details of the space
                return this.onezone.getSpaceAsync(spaceId);
            })
            .chain(s -> {
                // Got space details
                LOG.infof("Space name is %s", s.name);

                this.spaceName = s.name;

                // Check if space is supported by at least one provider
                if(null == s.providers || s.providers.isEmpty())
                    return Uni.createFrom().failure(new ActionException("spaceHasNoProviders",
                                                              Tuple2.of("spaceName", this.spaceName)));

                // Get the details of first provider supporting the space
                this.providerId = s.providers.keySet().iterator().next();
                return this.onezone.getProviderAsync(this.providerId);
            })
            .chain(p -> {
                // Got provider details
                // Create REST client for this provider
                return getProviderClient("https://" + p.domain);
            })
            .chain(unused -> {
                // Get details of the space again, this time from the provider
                return this.provider.getSpaceAsync(spaceId);
            })
            .onItem().transformToUni(sp -> {
                // Got space details from provider, success
                return Uni.createFrom().item(new DataStoreItem(sp.fileId, sp.name, true));
            })
            .onFailure().invoke(e -> {
                LOG.errorf("Failed to initialize backend for space %s", spaceId);
            });

        return result;
    }

    /***
     * Get URL to access storage element.
     * @param id Specifies the target item
     * @return URL to access specified item
     */
    public String buildItemUrl(String id) {

        if(null == this.providerBaseUrl || this.providerBaseUrl.isEmpty())
            return null;

        return String.format("%s/api/v3/oneprovider/data/%s/content", this.providerBaseUrl, id);
    }

    /***
     * Get an access token for a storage element.
     * @param id Specifies the target item
     * @return Access token restricted to specified item and point in time after which it should be updated
     */
    public Uni<Tuple2<String, LocalDateTime>> getItemToken(String id) {

        if(null == this.onezone) {
            LOG.error("Not initialized");
            return Uni.createFrom().failure(new ActionException("notInitialized"));
        } else if(null == id || id.isEmpty()) {
            LOG.error("No item specified, cannot get access token");
            return Uni.createFrom().failure(new ActionException("noItem"));
        }

        var daysValid = this.config.fileTokenValidityDays();
        if(daysValid < 1)
            daysValid = 1;

        var daysLeft = this.config.fileTokenValidityLeftDays();
        if(daysLeft < 1)
            daysLeft = 1;

        // Get access token restricted to this file
        final var validUntil = LocalDateTime.now().plusDays(daysValid);
        final var updateAfter = validUntil.minusDays(daysLeft);
        TokenRequest tr = new TokenRequest(Arrays.asList(
                new TokenRequest.Caveat(),               // read-only
                new TokenRequest.Caveat(id),             // for this file
                new TokenRequest.Caveat(validUntil)));   // limited validity

        Uni<Tuple2<String, LocalDateTime>> result = Uni.createFrom().nullItem()

                .chain(unused -> {
                    // Get access token for this file
                    return this.onezone.getRestrictedAccessTokenAsync(tr);
                })
                .chain(token -> {
                    // Got access token
                    return Uni.createFrom().item(Tuple2.of(token.token, updateAfter));
                })
                .onFailure().invoke(e -> {
                    LOG.errorf("Failed to get access token for item %s", id);
                });

        return result;
    }

    /***
     * Fetch the metadata of a file or folder.
     * @param id Specifies the target item
     * @return Metadata of indicated item
     */
    public Uni<Map<String, String>> getItemMetadata(String id) {

        if(null == this.provider) {
            LOG.error("Not initialized");
            return Uni.createFrom().failure(new ActionException("notInitialized"));
        } else if(null == id || id.isEmpty()) {
            LOG.error("No item specified, cannot get metadata");
            return Uni.createFrom().failure(new ActionException("noItem"));
        }

        // Get item metadata
        Uni<Map<String, String>> result = this.provider.getFileMetadataAsync(id)

            .chain(metadata -> {
                // Got item metadata
                return Uni.createFrom().item(metadata);
            })
            .onFailure().invoke(e -> {
                LOG.errorf("Failed to get metadata of item %s", id);
            });

        return result;
    }

    /***
     * Update the metadata of a file or folder.
     * @param id Specifies the target item
     * @param metadata The metadata to set on the target item
     * @return True on success
     */
    public Uni<Boolean> setItemMetadata(String id, Map<String, String> metadata) {

        if(null == this.provider) {
            LOG.error("Not initialized");
            return Uni.createFrom().failure(new ActionException("notInitialized"));
        } else if(null == id || id.isEmpty()) {
            LOG.error("No item specified, cannot set metadata");
            return Uni.createFrom().failure(new ActionException("noItem"));
        }

        // Set item metadata
        Uni<Boolean> result = this.provider.setFileMetadataAsync(id, metadata)

            .chain(response -> {
                // Check if successful
                if(Family.familyOf(response.getStatus()) != Family.SUCCESSFUL) {
                    return Uni.createFrom().failure(new ActionException("setItemMetadata", Arrays.asList(
                            Tuple2.of("fileId", id),
                            Tuple2.of("providerId", this.providerId),
                            Tuple2.of("oneproviderBaseUrl", this.providerBaseUrl))).status(response.getStatusInfo().toEnum()));
                }

                // Success
                return Uni.createFrom().item(true);
            })
            .onFailure().invoke(e -> {
                LOG.errorf("Failed to get metadata of item %s", id);
            });

        return result;
    }

    /***
     * List the content of a folder.
     * @param id Specifies the target item, must be a folder
     * @return Content of indicated item
     */
    public Multi<DataStoreItem> listFolderContent(String id) {

        if(null == this.provider) {
            LOG.error("Not initialized");
            return Multi.createFrom().failure(new ActionException("notInitialized"));
        } else if(null == id || id.isEmpty()) {
            LOG.error("No item specified, cannot list content");
            return Multi.createFrom().failure(new ActionException("noItem"));
        }

        // Paginate through the items, as listFolderContentAsync() can only return 1K items
        final int fetchAtOnce = 1000;
        var aro = new AtomicReference<>(0);
        var result = Multi.createBy().repeating()

            .uni( () -> aro, state -> {
                // List folder content
                var offset = aro.get();
                Duration delay = Duration.ofMillis(this.random.nextInt(10) + 1);
                return this.provider.listFolderContentAsync(id, offset, fetchAtOnce).onItem().delayIt().by(delay);
            })
            .whilst(dir -> {
                int first = aro.get();
                int last = first + dir.children.size() - 1;
                if(!dir.isLast) {
                    // Got some folder items
                    LOG.debugf("Got folder items %s-%s", first, last);

                    // Advance our cursor for the next page
                    // NOTE: This also handles the case when we asked for a full page of items,
                    //       got less than that, but there are more items to retrieve
                    aro.set(last + 1);

                    // Continue with the next page
                    return true;
                }

                return false;
            })
            .onItem().transformToMultiAndConcatenate(dir -> {
                // Got a page of folder items
                // Turn the list of items into a stream
                return Multi.createFrom().iterable(dir.children);
            })
            .onItem().transformToUniAndConcatenate(item -> {
                // Got a folder item
                LOG.debugf("Got folder item %s (%s)", item.name, item.fileId);

                // Get details about folder item
                return this.provider.getFileAsync(item.fileId);
            })
            .onItem().transformToUniAndConcatenate(file -> {
                // Got item details
                var dsi = new DataStoreItem(file);
                LOG.debugf("Got details of %s %s", dsi.isFolder ? "folder" : "file", dsi.name);

                Duration delay = Duration.ofMillis(this.random.nextInt(10) + 1);
                return Uni.createFrom().item(dsi).onItem().delayIt().by(delay);
            })
            .onFailure().invoke(e -> {
                LOG.errorf("Failed to list content of folder %s", id);
            });

        return result;
    }

}
