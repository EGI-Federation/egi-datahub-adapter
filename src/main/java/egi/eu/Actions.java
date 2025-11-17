package egi.eu;

import com.fasterxml.jackson.core.JsonProcessingException;
import idsa.connector.model.Policy;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.security.SecuritySchemes;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.eclipse.microprofile.rest.client.RestClientDefinitionException;
import org.jboss.logging.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.security.*;
import java.security.cert.CertificateException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import idsa.connector.Connector;
import idsa.connector.ConnectorConfig;
import idsa.connector.model.*;
import onedata.OneDataConfig;
import egi.eu.model.*;
import egi.eu.model.PublishPolicy.PolicyType;


@Path("/")
@SecuritySchemes(value = {
        @SecurityScheme(securitySchemeName = "none"),
        @SecurityScheme(securitySchemeName = "bearer",
                type = SecuritySchemeType.HTTP,
                scheme = "Bearer")} )
@Produces(MediaType.APPLICATION_JSON)
public class Actions {

    // Set on root folder
    private static final String META_CATALOG_PREFIX = "idsa:catalog:";

    // Set on files
    private static final String META_UPDATEAFTER_PREFIX = "idsa:update:";
    private static final String META_ARTIFACT_PREFIX = "idsa:artifact:";
    private static final String META_POLICY_PREFIX = "idsa:policy:";

    private static SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // UTC
    private static final Logger LOG = Logger.getLogger(Actions.class);

    @Inject
    ConnectorConfig connectorConfig;

    @Inject
    OneDataConfig backendConfig;

    private static Connector connector;


    /***
     * Load a certificate store from a resource file.
     * @param filePath File path relative to the "src/main/resource" folder
     * @param password The password for the certificate store
     * @return Loaded key store, empty optional on error
     */
    private Optional<KeyStore> loadKeyStore(String filePath, String password) {

        Optional<KeyStore> oks = Optional.empty();
        try {
            var classLoader = getClass().getClassLoader();
            var ksf = classLoader.getResourceAsStream(filePath);
            var ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(ksf, password.toCharArray());
            oks = Optional.of(ks);
        }
        catch (FileNotFoundException e) {
            LOG.error(e);
        }
        catch (KeyStoreException e) {
            LOG.error(e);
        }
        catch (CertificateException e) {
            LOG.error(e);
        }
        catch (IOException e) {
            LOG.error(e);
        }
        catch (NoSuchAlgorithmException e) {
            LOG.error(e);
        }

        return oks;
    }

    /**
     * Prepare REST client for the IDSA connector.
     * @param params holds the parameters for the action being performed
     * @return Received parameters with updated field "connector"
     */
    @PostConstruct
    private Uni<ActionParameters> getConnectorClient(ActionParameters params) {

        // Check if the URLs are valid
        URL urlConnector;
        try {
            urlConnector = new URL(connectorConfig.connectorBaseUrl());
        }
        catch (MalformedURLException e) {
            LOG.error(e.getMessage());
            return Uni.createFrom().failure(new ActionException(e, "invalidConfiguration",
                                                      Tuple2.of("connectorBaseUrl", connectorConfig.connectorBaseUrl())));
        }

        try {
            if (null == this.connector) {
                // Create the connector client with the configured base URL
                var cfg = ConfigProvider.getConfig();
                var ksFile = cfg.getValue("quarkus.http.ssl.certificate.key-store-file", String.class);
                var ksPass = cfg.getValue("quarkus.http.ssl.certificate.key-store-password", String.class);
                var tsFile = cfg.getValue("quarkus.http.ssl.certificate.trust-store-file", String.class);
                var tsPass = cfg.getValue("quarkus.http.ssl.certificate.trust-store-password", String.class);

                // IDSA connectors in PRODUCTIVE_DEPLOYMENT mode require TLS, so we'll load and use a client certificate
                var rcb = RestClientBuilder.newBuilder().baseUrl(urlConnector);
                var oks = loadKeyStore(ksFile, ksPass);
                var ots = loadKeyStore(tsFile, tsPass);

                if(ots.isPresent())
                    rcb.trustStore(ots.get());

                if(oks.isPresent())
                    rcb.keyStore(oks.get(), ksPass);

                this.connector = rcb.build(Connector.class);

                LOG.infof("Created REST client for IDSA connector at %s", connectorConfig.connectorBaseUrl());
            } else {
                LOG.infof("Using cached REST client for IDSA connector at %s", connectorConfig.connectorBaseUrl());
            }

            params.connector = this.connector;

            return Uni.createFrom().item(params);
        }
        catch(IllegalStateException ise) {
            LOG.error(ise.getMessage());
            return Uni.createFrom().failure(new ActionException(ise, "invalidConfiguration",
                                                      Tuple2.of("connectorBaseUrl", connectorConfig.connectorBaseUrl())));
        }
        catch(RestClientDefinitionException rcde) {
            LOG.error(rcde.getMessage());
            return Uni.createFrom().failure(new ActionException(rcde, "invalidConfiguration",
                                                      Tuple2.of("connectorBaseUrl", connectorConfig.connectorBaseUrl())));
        }
    }

    /***
     * Compute small unique text (hash) from possibly large input text.
     * It does not need to be cryptographically secure.
     * @param text Text to compute hash from.
     * @return Computed hash.
     */
    private static String computeHash(String text) {

        String result = text;
        MessageDigest digest;

        try {
            // Get a hasher
            digest = MessageDigest.getInstance("MD5");

            // Hash our input
            digest.update(text.getBytes());

            // Convert the hash to a string
            StringBuilder sb = new StringBuilder();
            for (byte b : digest.digest()) {
                sb.append(String.format("%02X", b));
            }
            result = sb.toString();
        }
        catch (NoSuchAlgorithmException e) {
            LOG.error(e);
        }


        return result;
    }

    /***
     * Find a catalog in the IDSA connector.
     * @param ap Holds the id of the catalog in field "catalogId"
     * @return Requested catalog
     */
    private static Uni<Catalog> findCatalog(ActionParameters ap) {

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<Catalog> result = Uni.createFrom().item(ap)

            .chain(params -> {
                // Get catalog
                return params.connector.getCatalogAsync(params.catalogId);
            })
            .chain(catalog -> {
                // Catalog exists
                var params = apr.get();
                LOG.infof("Catalog for space %s exists and is %s",
                          params.backend.getSpaceName(),
                          params.catalogId);

                return Uni.createFrom().item(catalog);
            })
            .onFailure().recoverWithUni(e -> {
                // Failed to get catalog
                var params = apr.get();
                if(e instanceof WebApplicationException) {
                    var wae = (WebApplicationException)e;
                    if (Status.NOT_FOUND == Status.fromStatusCode(wae.getResponse().getStatus())) {
                        // Catalog not found
                        LOG.infof("Catalog for space %s no longer exists", params.backend.getSpaceName());
                        params.response = Response.ok().status(Status.CREATED).build();
                        return Uni.createFrom().nullItem();
                    }
                }

                // Other errors
                LOG.errorf("Failed to retrieve catalog for space %s", params.backend.getSpaceId());
                return Uni.createFrom().failure(new ActionException("findCatalog",
                                                          Tuple2.of("catalogId", params.catalogId) ));
            });

        return result;
    }

    /***
     * Find an artifact in the IDSA connector.
     * @param ap Holds the id of the artifact in field "artifactId"
     * @return Requested artifact
     */
    private static Uni<Artifact> findArtifact(ActionParameters ap) {

        final String path = String.format("%s%s%s", ap.path, ap.backend.separator(), ap.item.name);

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<Artifact> result = Uni.createFrom().item(ap)

                .chain(params -> {
                    // Get artifact
                    return params.connector.getArtifactAsync(params.artifactId);
                })
                .chain(artifact -> {
                    // Artifact exists
                    var params = apr.get();
                    LOG.infof("Artifact for file %s exists and is %s", path, params.artifactId);
                    return Uni.createFrom().item(artifact);
                })
                .onFailure().recoverWithUni(e -> {
                    // Failed to get artifact
                    var params = apr.get();
                    if(e instanceof WebApplicationException) {
                        var wae = (WebApplicationException)e;
                        if (Status.NOT_FOUND == Status.fromStatusCode(wae.getResponse().getStatus())) {
                            // Artifact not found
                            LOG.infof("Artifact for file %s no longer exists", path);
                            params.response = Response.ok().status(Status.CREATED).build();
                            return Uni.createFrom().nullItem();
                        }
                    }

                    // Other errors
                    LOG.errorf("Failed to retrieve artifact for file %s", path);
                    return Uni.createFrom().failure(new ActionException("findArtifact",
                                                              Tuple2.of("artifactId", params.catalogId) ));
                });

        return result;
    }

    /**
     * Make sure a catalog exists in the IDSA connector for our space. If the space was
     * published in the past, but the catalog no longer exists, it creates a new catalog
     * and updates the metadata of the root folder.
     * @param ap Holds the details of the space for which to validate existence of a catalog.
     * @return The received parameters, with field "response" having status code OK if the catalog
     *         already exists, CREATED if it was just created, or wraps an ActionError entity on failure.
     *         The field "catalogId" is filled in on success.
     */
    private static Uni<ActionParameters> validateCatalog(ActionParameters ap) {

        LOG.infof("Validating catalog of space %s (%s)",
                  ap.backend.getSpaceName(),
                  ap.backend.getSpaceId());

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<ActionParameters> result = Uni.createFrom().item(ap)

            .chain(params -> {
                // Get connector configuration
                return params.connector.getActiveConfigurationAsync();
            })
            .chain(cfg -> {
                // Got connector configuration
                var params = apr.get();
                params.connectorId = cfg.connectorId;

                // Get root folder metadata
                return params.backend.getItemMetadata(params.item.id);
            })
            .chain(fmd -> {
                // Got root folder metadata
                var params = apr.get();
                params.item.metadata = fmd;

                LOG.infof("Space root folder is %s", params.item.id);

                var mds = params.item.metadataAsString();
                if(!mds.isEmpty())
                    LOG.debugf("Metadata:\n%s", mds);

                // Check if root folder is already published
                if(params.item.metadata.containsKey(META_CATALOG_PREFIX + params.connectorId)) {
                    // Root folder already published to this connector
                    LOG.infof("Space %s already published to connector %s", params.backend.getSpaceName(), params.connectorId);
                    params.catalogId = params.item.metadata.get(META_CATALOG_PREFIX + params.connectorId);

                    // Get the catalog of this space (if it still exists)
                    return findCatalog(params);
                }

                // Root folder not published to this connector
                return Uni.createFrom().nullItem();
            })
            .chain(cat -> {
                // Got a catalog, if there was one already for this space
                if(null != cat)
                    // If we have a catalog, pass it to the next stage
                    return Uni.createFrom().item(cat);

                // Create new catalog for this space
                var params = apr.get();
                LOG.infof("Creating new catalog for space %s", params.backend.getSpaceName());
                return params.connector.createCatalogAsync(new CatalogDescription(params.backend.getSpaceName()));
            })
            .chain(cat -> {
                // Got the catalog for this space
                var params = apr.get();
                params.catalogId = cat.extractId();

                // Store the catalog id as metadata on the root folder
                params.item.metadata.put(META_CATALOG_PREFIX + params.connectorId, params.catalogId);
                return params.backend.setItemMetadata(params.item.id, params.item.metadata);
            })
            .chain(finish -> {
                // Root folder metadata was updated, success
                return Uni.createFrom().item(apr.get());
            })
            .onFailure().invoke(e -> {
                var params = apr.get();
                LOG.errorf("Failed to validate catalog for space %s", params.backend.getSpaceId());
            });

        return result;
    }

    /***
     * Create policy as specified, serialize it to a string.
     * @param ap Holds the details of the file, including its metadata, as well as the publishing parameters.
     * @return String representation of usage policy.
     */
    private static Uni<RuleDescription> createUsagePolicy(ActionParameters ap) {

        Uni<RuleDescription> result = Uni.createFrom().item(ap)

            .chain(params -> {
                String title = "Usage policy";
                Policy policy = null;
                switch (params.policy.type) {
                    case PROHIBIT:
                        title = "Prohibit use";
                        policy = Policy.ProhibitUse(title);
                        break;

                    case COUNTED:
                        title = String.format("Allow use %d times", params.policy.useCount);
                        policy = Policy.CountedUse(params.policy.useCount, title);
                        break;

                    case INTERVAL:
                        title = String.format("Allow use from %s to %s",
                                dateTimeFormat.format(params.policy.useFrom),
                                dateTimeFormat.format(params.policy.useUntil));
                        policy = Policy.IntervalUse(params.policy.useFrom, params.policy.useUntil, title);
                        break;

                    case INTERVAL_DELETE:
                        title = String.format("Allow use from %s to %s and delete at %s",
                                dateTimeFormat.format(params.policy.useFrom),
                                dateTimeFormat.format(params.policy.useUntil),
                                dateTimeFormat.format(params.policy.deleteAt));
                        policy = Policy.IntervalUseDeleteAt(params.policy.useFrom, params.policy.useUntil, params.policy.deleteAt, title);
                        break;

                    case DURATION:
                        title = String.format("Allow use for %s", params.policy.useFor);
                        policy = Policy.DurationUse(params.policy.useFor, title);
                        break;

                    case NOTIFY:
                        title = "Allow use with notification";
                        policy = Policy.NotifyUse(params.policy.notifyMessage, params.policy.notifyLink, title);
                        break;

                    case LOG:
                        title = "Allow use with logging";
                        policy = Policy.LogUse(title);
                        break;

                    default:
                        title = "Allow use";
                        policy = Policy.FreeUse(title);
                        break;
                }

                RuleDescription rule = null;
                try {
                    rule = new RuleDescription(title, policy);
                } catch (JsonProcessingException e) {
                    LOG.error(e);
                    return Uni.createFrom().failure(new ActionException("policyToJsonString"));
                }

                return Uni.createFrom().item(rule);
            });

        return result;
    }

    /***
     * Replace the access policy (rule) of the artifact associated with the specified file.
     * NOTE: It only replaces the first rule, in the first contract of the first offer of the first representation.
     * @param ap Holds the details of the file, including its metadata, as well as the publishing parameters.
     * @param policy The new access policy for the file, serialized to a string.
     * @return The received parameters.
     */
    private static Uni<ActionParameters> replaceAccessPolicy(ActionParameters ap, RuleDescription policy) {

        final String path = String.format("%s%s%s", ap.path, ap.backend.separator(), ap.item.name);

        LOG.infof("Replacing access policy of file %s (%s)", path, ap.item.id);

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<ActionParameters> result = Uni.createFrom().item(ap)

                // Cancel (delete) all negotiated contracts (requests) for this artifact
                // NOTE: Some contracts may not be created by us and may cover more artifacts
                //       than the one associated with this file. All negotiated contracts that
                //       cover this artifact will be deleted.

            .chain(params -> {
                // List all representations that use the artifact
                return params.connector.listArtifactRepresentationsAsync(params.artifactId);
            })
            .chain(_reps -> {
                // Got all representations that use the artifact
                if(null == _reps || !_reps._embedded.containsKey("representations"))
                    // Cannot find representations
                    return Uni.createFrom().failure(new ActionException("badArtifactRepresentations"));

                var representations = _reps._embedded.get("representations");
                if(null == representations || representations.isEmpty())
                    // No representations
                    return Uni.createFrom().failure(new ActionException("noArtifactRepresentations"));

                var representation = representations.get(0);
                var params = apr.get();
                params.representationId = representation.extractId();

                // List all offers that include the representation
                return params.connector.listRepresentationOffersAsync(params.representationId);
            })
            .chain(_offs -> {
                // Got all offers that include the representation
                if(null == _offs || !_offs._embedded.containsKey("resources"))
                    // Cannot find offers
                    return Uni.createFrom().failure(new ActionException("badRepresentationOffers"));

                var offers = _offs._embedded.get("resources");
                if(null == offers || offers.isEmpty())
                    // No offers
                    return Uni.createFrom().failure(new ActionException("noRepresentationOffers"));

                var offer = offers.get(0);
                var params = apr.get();
                params.offerId = offer.extractId();

                // List all contracts that cover the offer
                return params.connector.listOfferContractsAsync(params.offerId);
            })
            .chain(_ctrs -> {
                // Got all contracts that cover the offer
                if(null == _ctrs || !_ctrs._embedded.containsKey("contracts"))
                    // Cannot find contracts
                    return Uni.createFrom().failure(new ActionException("badResourceContracts"));

                var contracts = _ctrs._embedded.get("contracts");
                if(null == contracts || contracts.isEmpty())
                    // No contracts
                    return Uni.createFrom().failure(new ActionException("noResourceContracts"));

                var contract = contracts.get(0);
                var params = apr.get();
                params.contractId = contract.extractId();

                // List all rules in the contract
                return params.connector.listContractRulesAsync(params.contractId);
            })
            .chain(_ruls -> {
                // Got all rules in the contract
                if(null == _ruls || !_ruls._embedded.containsKey("rules"))
                    // Cannot find rules
                    return Uni.createFrom().failure(new ActionException("badContractRules"));

                var rules = _ruls._embedded.get("rules");
                if(null == rules || rules.isEmpty())
                    // No rules
                    return Uni.createFrom().failure(new ActionException("noContractRules"));

                var rule = rules.get(0);
                var params = apr.get();
                params.ruleId = rule.extractId();

                // Remove rule from the contract
                return params.connector.removeRulesFromContractAsync(params.contractId, List.of(params.ruleId));
            })
            .chain(unused -> {
                // Rule was removed from the contract
                // Delete the rule
                var params = apr.get();
                return params.connector.deleteRuleAsync(params.ruleId);
            })
            .chain(unused -> {
                // Old rule was deleted, create a new one
                var params = apr.get();
                return params.connector.createRuleAsync(policy);
            })
            .chain(rule -> {
                // Got rule
                var params = apr.get();
                params.ruleId = rule.extractId();

                // Add the rule to the contract
                return params.connector.addRulesToContractAsync(params.contractId, Arrays.asList(params.ruleId));
            })
            .chain(unused -> {
                // Access policy replaced, success
                return Uni.createFrom().item(apr.get());
            })
            .onFailure().invoke(e -> {
                var params = apr.get();
                LOG.errorf("Failed to replace access policy of file %s (%s)", path, params.item.id);
            });

        return result;
    }

    /**
     * Publish a file to an IDSA connector, by creating all the necessary entities.
     * @param ap Holds the details of the file, including its metadata, as well as the publishing parameters.
     * @return The received parameters, with field "item.metadata" updated with new metadata that needs to be
     *         saved to the file in the backend.
     */
    private static Uni<ActionParameters> publishFile(ActionParameters ap) {

        final String path = String.format("%s%s%s", ap.path, ap.backend.separator(), ap.item.name);

        LOG.infof("Publishing file %s (%s)", path, ap.item.id);

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<ActionParameters> result = Uni.createFrom().item(ap)

            .chain(params -> {
                // Get access token restricted to this file
                return params.backend.getItemToken(params.item.id);
            })
            .chain(token -> {
                // Got access token and update after date/time
                var params = apr.get();
                var updateAfterLocal = token.getItem2();
                var updateAfterLocalUtc = Date.from(updateAfterLocal.toInstant(ZoneOffset.UTC));
                params.item.metadata.put(META_UPDATEAFTER_PREFIX + params.connectorId, dateTimeFormat.format(updateAfterLocalUtc));

                // Pass access token forward
                return Uni.createFrom().item(token.getItem1());
            })
            .chain(token -> {
                // Got access token
                var params = apr.get();
                var fileContentUrl = params.backend.buildItemUrl(params.item.id);
                if(null == fileContentUrl) {
                    // Backend is not initialized
                    return Uni.createFrom().failure(new ActionException("noContentUrl"));
                }

                // Create artifact for this file
                ArtifactDescription afd = new ArtifactDescription(params.item.name, fileContentUrl, token);
                return params.connector.createArtifactAsync(afd);
            })
            .chain(artifact -> {
                // Got artifact
                var params = apr.get();
                params.artifactId = artifact.extractId();
                params.item.metadata.put(META_ARTIFACT_PREFIX + params.connectorId, params.artifactId);

                // Create representation for this artifact
                RepresentationDescription rpr = new RepresentationDescription(params.item.name, params.language);
                return params.connector.createRepresentationAsync(rpr);
            })
            .chain(representation -> {
                // Got representation
                var params = apr.get();
                params.representationId = representation.extractId();

                // Add the artifact to the representation
                return params.connector.addArtifactsToRepresentationAsync(params.representationId, Arrays.asList(params.artifactId));
            })
            .chain(artifacts -> {
                // Artifact was added to representation
                // Create offer for this representation
                var params = apr.get();
                OfferDescription res = new OfferDescription(params.item.name, params.path);
                res.publisher = params.publisher;
                res.sovereign = params.sovereign;
                res.language = params.language;
                res.license = params.license;
                if(null != params.keywords) {
                    res.keywords = new ArrayList<String>();
                    res.keywords.addAll(params.keywords);
                }

                return params.connector.createOfferAsync(res);
            })
            .chain(resource -> {
                // Got offer
                var params = apr.get();
                params.offerId = resource.extractId();

                // Add the representation to the offer
                return params.connector.addRepresentationsToResourceAsync(params.offerId, Arrays.asList(params.representationId));
            })
            .chain(representations -> {
                // Representation was added to the offer
                // Create contract for this offer
                var params = apr.get();
                ContractDescription rc = new ContractDescription(params.item.name, params.path);

                return params.connector.createContractAsync(rc);
            })
            .chain(contract -> {
                // Got contract
                var params = apr.get();
                params.contractId = contract.extractId();

                // Create usage policy for the offer
                return createUsagePolicy(params);
            })
            .chain(policy -> {
                // Got usage policy
                var params = apr.get();

                // Save the hash of the policy in item metadata
                // This allows us to check if policy changed when updating this file,
                // and create new rule if so
                params.item.metadata.put(META_POLICY_PREFIX + params.connectorId, computeHash(policy.value));

                // Create rule using the policy
                return params.connector.createRuleAsync(policy);
            })
            .chain(rule -> {
                // Got rule
                var params = apr.get();
                params.ruleId = rule.extractId();

                // Add the rule to the contract
                return params.connector.addRulesToContractAsync(params.contractId, Arrays.asList(params.ruleId));
            })
            .chain(rules -> {
                // Rule was added to the contract
                // Add the contract to the offer
                var params = apr.get();
                return params.connector.addContractsToResourceAsync(params.offerId, Arrays.asList(params.contractId));
            })
            .chain(contracts -> {
                // Contract was added to the offer
                // Add the offer to the catalog
                var params = apr.get();
                return params.connector.addResourcesToCatalogAsync(params.catalogId, Arrays.asList(params.offerId));
            })
            .chain(resources -> {
                // Offer was added to the catalog
                // This concludes publishing the file to the connector
                var params = apr.get();
                params.response = Response.status(Status.OK).build();
                LOG.infof("Published file %s (%s)", path, params.item.id);
                return Uni.createFrom().item(params);
            })
            .onFailure().invoke(e -> {
                var params = apr.get();
                LOG.errorf("Failed to publish file %s to connector %s", path, params.connectorId);
            });

        return result;
    }

    /**
     * Update a file already published to an IDSA connector, by refreshing the details in the associated artifact.
     * @param ap Holds the details of the file, including its metadata, as well as the publishing parameters.
     * @return The received parameters, with field "item.metadata" updated with new metadata that needs to be
     *         saved to the file in the backend.
     */
    private static Uni<ActionParameters> updateFile(ActionParameters ap) {

        final String path = String.format("%s%s%s", ap.path, ap.backend.separator(), ap.item.name);

        LOG.infof("Updating file %s (%s)", path, ap.item.id);

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<ActionParameters> result = Uni.createFrom().item(ap)

            .chain(params -> {
                // Check if file access token about to expire
                if(params.item.metadata.containsKey(META_UPDATEAFTER_PREFIX + params.connectorId)) {
                    var updateAfter = params.item.metadata.get(META_UPDATEAFTER_PREFIX + params.connectorId);
                    try {
                        var updateAfterUtc = dateTimeFormat.parse(updateAfter);
                        boolean updateToken = Instant.now().isAfter(updateAfterUtc.toInstant());
                        return Uni.createFrom().item(updateToken);
                    }
                    catch(ParseException pe) {
                        LOG.errorf("Cannot parse token validity of file %s (%s)", path, updateAfter);
                    }
                }

                // Cannot determine validity, refresh access token
                return Uni.createFrom().item(true);
            })
            .chain(updateToken -> {
                if(!updateToken)
                    // No need to update access token
                    return Uni.createFrom().nullItem();

                // Get new access token restricted to this file
                var params = apr.get();
                return params.backend.getItemToken(params.item.id);
            })
            .chain(token -> {
                if(null == token)
                    // No need to update access token
                    return Uni.createFrom().nullItem();

                // Got new access token and validity end date/time
                var params = apr.get();
                var updateAfterLocal = token.getItem2();
                var updateAfterLocalUtc = Date.from(updateAfterLocal.toInstant(ZoneOffset.UTC));
                params.item.metadata.put(META_UPDATEAFTER_PREFIX + params.connectorId, dateTimeFormat.format(updateAfterLocalUtc));

                // Pass access token forward
                return Uni.createFrom().item(token.getItem1());
            })
            .chain(token -> {
                if(null == token)
                    // No need to update access token
                    return Uni.createFrom().nullItem();

                // Got access token
                var params = apr.get();
                var fileContentUrl = params.backend.buildItemUrl(params.item.id);
                if(null == fileContentUrl) {
                    // Backend is not initialized
                    return Uni.createFrom().failure(new ActionException("noContentUrl"));
                }

                // Update artifact of this file
                ArtifactDescription afd = new ArtifactDescription(params.item.name, fileContentUrl, token);
                return params.connector.updateArtifactAsync(params.artifactId, afd);
            })
            .chain(updatedArtifact -> {
                // Artifact now has a valid access token
                // NOTE: Connector endpoint PUT /artifacts/{id} usually returns no content (artifact will be null)
                return Uni.createFrom().item(apr.get());
            })
            .chain(params -> {
                // Check if policy needs updating
                if(params.item.metadata.containsKey(META_POLICY_PREFIX + params.connectorId)) {
                    // Got the hash of the current policy
                    // Create new usage policy for resource
                    return createUsagePolicy(params);
                }

                // Cannot check existing policy, will not replace
                return Uni.createFrom().nullItem();
            })
            .chain(policy -> {
                if(null != policy) {
                    // Check if the policy has changed
                    var params = apr.get();
                    var currentPolicyHash = params.item.metadata.get(META_POLICY_PREFIX + params.connectorId);
                    var newPolicyHash = computeHash(policy.value);
                    if (!currentPolicyHash.equals(newPolicyHash)) {
                        // Publishing again with a different policy
                        params.item.metadata.put(META_POLICY_PREFIX + params.connectorId, newPolicyHash);
                        return replaceAccessPolicy(params, policy);
                    }
                }

                // No need to update access policy
                return Uni.createFrom().nullItem();
            })
            .chain(finish -> {
                // This concludes updating the file in the connector
                var params = apr.get();
                params.response = Response.status(Status.RESET_CONTENT).build();
                LOG.infof("Updated file %s (%s)", path, params.item.id);
                return Uni.createFrom().item(params);
            })
            .onFailure().invoke(e -> {
                var params = apr.get();
                LOG.errorf("Failed to update file %s to connector %s", path, params.connectorId);
            });

        return result;
    }

    /**
     * Publish a file to an IDSA connector.
     * @param ap Holds the id of the file to publish, the backend from where to get info
     *           about the file, the connector where to publish it, and the catalog in which
     *           to create an artifact for this file.
     * @return ActionParameters with the field "response" having status code OK if the file was published
     *         for the first time, RESET_CONTENT if the file was updated, or wraps an ActionError entity on failure.
     *         The field "item.metadata" is filled in on success.
     */
    private static Uni<ActionParameters> handleFile(ActionParameters ap) {

        final String path = String.format("%s%s%s", ap.path, ap.backend.separator(), ap.item.name);

        if(ap.item.size <= 0) {
            // We will ignore empty files
            LOG.infof("Ignoring empty file %s (%s)", path, ap.item.id);
            ap.item.isIgnored = true;
            return Uni.createFrom().item(ap);
        }

        LOG.debugf("Checking if already published file %s (%s)", path, ap.item.id);

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<ActionParameters> result = Uni.createFrom().item(ap)

            .chain(params -> {
                // Get file metadata
                return params.backend.getItemMetadata(params.item.id);
            })
            .chain(fmd -> {
                // Got file metadata
                var params = apr.get();
                params.item.metadata = new HashMap<>();
                if(null != fmd && !fmd.isEmpty())
                    params.item.metadata.putAll(fmd);

                var mds = params.item.metadataAsString();
                if(!mds.isEmpty())
                    LOG.debugf("Metadata:\n%s", mds);

                // Check if file is already published
                if(params.item.metadata.containsKey(META_ARTIFACT_PREFIX + params.connectorId)) {
                    // File marked as already published to this connector
                    LOG.infof("File %s already published", path);
                    params.artifactId = params.item.metadata.get(META_ARTIFACT_PREFIX + params.connectorId);

                    // Get the artifact of this file (if it still exists)
                    return findArtifact(params);
                }

                // No artifact as file not yet published to this connector
                return Uni.createFrom().nullItem();
            })
            .chain(artifact -> {
                // Got an artifact, if there was one already for this file
                var params = apr.get();
                if(null != artifact)
                    // If we have an artifact, refresh it with a new access token for the file
                    return updateFile(params);

                // Create new artifact for this file, which means create all entities involved
                return publishFile(params);
            })
            .chain(params -> {
                // Update file metadata
                return params.backend.setItemMetadata(params.item.id, params.item.metadata);
            })
            .chain(success -> {
                // File handled, success
                var params = apr.get();
                return Uni.createFrom().item(params);
            })
            .onFailure().recoverWithUni(e -> {
                var params = apr.get();
                LOG.errorf("Failed to handle file %s (%s)", path, params.item.id);

                return Uni.createFrom().failure(new ActionException(e, Arrays.asList(
                                                          Tuple2.of("fileName", params.item.name),
                                                          Tuple2.of("fileId", params.item.id),
                                                          Tuple2.of("path", params.path)) ));
            });

        return result;
    }

    /***
     * Publish files from a folder (and all subfolders) to an IDSA connector.
     * @param ap Holds the details of the folder to publish, the backend from where to get info
     *           about the files, the connector and catalog where to publish them.
     * @return ActionParameters with the field "response" wrapping an ActionSuccess entity
     *         that holds the stats of the operation, or an ActionError entity on failure.
     */
    private static Uni<ActionParameters> handleFolder(ActionParameters ap) {

        final String path = String.format("%s%s%s", ap.path.length() > ap.backend.root().length() ? ap.path : "",
                                                    ap.backend.separator(),
                                                    ap.item.name);

        LOG.infof("Publishing content of folder %s (%s)", path, ap.item.id);

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        AtomicReference<ActionSuccess> asr = new AtomicReference<>(new ActionSuccess(ap.backend.getSpaceId(), ap.backend.getSpaceName()));

        var result = Uni.createFrom().item(ap)

            .onItem().transformToMulti(params -> {
                // List content of the folder
                return params.backend.listFolderContent(params.item.id);
            })
            .onItem().transformToUniAndMerge(item -> {
                // Got item details
                ActionParameters fileParams = new ActionParameters(apr.get());
                fileParams.path = path;
                fileParams.item = new DataStoreItem(item);

                // Check the type of the item
                if(item.isFolder)
                    // This is a subfolder, recurse into it
                    return handleFolder(fileParams);

                // This is a file, handle it
                return handleFile(fileParams);
            })
            .onItem().transformToUniAndMerge(handledParams -> {
                // Folder item was handled
                var stats = asr.get();
                if(handledParams.item.isFolder) {
                    // Subfolder was handled
                    stats._subFolders.incrementAndGet();

                    // Count files handled in this subfolder
                    var entity = handledParams.response.getEntity();
                    if(null != entity && ActionSuccess.class.isAssignableFrom(entity.getClass())) {
                        ActionSuccess as = (ActionSuccess)entity;
                        stats._newFiles.addAndGet(as._newFiles.get());
                        stats._updatedFiles.addAndGet(as._updatedFiles.get());
                    }
                }
                else if(!handledParams.item.isIgnored) {
                    // File was handled, count it
                    var status = Status.fromStatusCode(handledParams.response.getStatus());
                    if(status.equals(Status.OK))
                        stats._newFiles.incrementAndGet();
                    else if(status.equals(Status.RESET_CONTENT))
                        stats._updatedFiles.incrementAndGet();
                    else {
                        var handledPath = String.format("%s%s%s", path, ap.backend.separator(), handledParams.item.name);
                        LOG.errorf("Unexpected outcome when handling item %s (%s)", handledPath, handledParams.item.id);
                    }
                }

                // Success for this item
                return Uni.createFrom().item(apr.get());
            })
            .onCompletion().invoke(() -> {
                // Success for all items
                var params = apr.get();
                LOG.infof("Finished handling folder %s (%s)", path, params.item.id);
            })
            .onFailure().recoverWithMulti(e -> {
                var params = apr.get();
                LOG.errorf("Failed to handle folder %s (%s)", path, params.item.id);

                var ae = new ActionException(e);
                var details = ae.getDetails();
                if(null == details)
                    details = new HashMap<>();

                if (!details.containsKey("fileName") && !details.containsKey("folderName"))
                    ae.detail(Tuple2.of("folderName", params.item.name));
                if (!details.containsKey("fileId") && !details.containsKey("folderId"))
                    ae.detail(Tuple2.of("folderId", params.item.id));
                if (!details.containsKey("path"))
                    ae.detail(Tuple2.of("path", params.path));

                return Multi.createFrom().failure(ae);
            })
            .collect()
            .in(ActionParameters::new, (acc, params) -> {
                var stats = asr.get();
                acc.response = stats.publishSummary().toResponse();
            });

        return result;
    }

    /**
     * Publish all non-empty files from the specified backend space to the configured IDSA connector.
     * @param spaceId is the space to publish
     * @return API Response, wraps an ActionSuccess or an ActionError entity
     */
    @POST
    @Path("publish/{spaceId}")
    @SecurityRequirement(name = "none")
    @Operation(summary = "Publish EGI DataHub space to an IDSA connector",
               description = "Publishes each file from an EGI DataHub space to the configured IDSA connector.")
    @APIResponses(value = {
        @APIResponse(responseCode = "200", description = "Success",
                content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ActionSuccess.class))),
        @APIResponse(responseCode = "400", description="Invalid parameters",
                content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ActionError.class))),
        @APIResponse(responseCode = "404", description="Space not found",
                content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ActionError.class)))
    })
    public Uni<Response> publish(@Parameter(description = "The id of the space to publish", required = true)
                                 @PathParam("spaceId")
                                 String spaceId,
                                 @Parameter(description = "Optional publish parameters")
                                 PublishParams optionalParams) {

        LOG.infof("Publishing space %s", spaceId);

        ActionParameters ap = new ActionParameters();
        if(null != optionalParams) {
            ap.publisher = optionalParams.publisher;
            ap.sovereign = optionalParams.sovereign;
            ap.language = optionalParams.language;
            ap.license = optionalParams.license;
            ap.keywords = (null != optionalParams.keywords) ? new ArrayList<String>() : null;
            if(null != optionalParams.keywords)
                ap.keywords.addAll(optionalParams.keywords);

            ap.policy = (null != optionalParams.policy) ? optionalParams.policy : new PublishPolicy(PolicyType.FREE);
        } else {
            ap.policy = new PublishPolicy(PolicyType.FREE);
        }

        AtomicReference<ActionParameters> apr = new AtomicReference<>(ap);
        Uni<Response> result = Uni.createFrom().nullItem()

            .chain(unused -> {
                // Validate that we have all required policy details
                switch(ap.policy.type) {
                    case COUNTED:
                        if(ap.policy.useCount <= 0) {
                            return Uni.createFrom().failure(new ActionException("invalidParameters",
                                                                      Tuple2.of("policy.useCount", "Field is required and must be greater than 0")));
                        } break;
                    case INTERVAL:
                        if(null == ap.policy.useFrom || null == ap.policy.useUntil) {
                            var errorDetails = new ArrayList<Tuple2<String, String>>();
                            if(null == ap.policy.useFrom)
                                errorDetails.add(Tuple2.of("policy.useFrom", "Field is required"));
                            if(null == ap.policy.useUntil)
                                errorDetails.add(Tuple2.of("policy.useUntil", "Field is required"));

                            return Uni.createFrom().failure(new ActionException("invalidParameters", errorDetails));
                        } break;
                    case INTERVAL_DELETE:
                        if(null == ap.policy.useFrom || null == ap.policy.useUntil) {
                            var errorDetails = new ArrayList<Tuple2<String, String>>();
                            if(null == ap.policy.useFrom)
                                errorDetails.add(Tuple2.of("policy.useFrom", "Field is required"));
                            if(null == ap.policy.useUntil)
                                errorDetails.add(Tuple2.of("policy.useUntil", "Field is required"));
                            if(null == ap.policy.deleteAt)
                                errorDetails.add(Tuple2.of("policy.deleteAt", "Field is required"));

                            return Uni.createFrom().failure(new ActionException("invalidParameters", errorDetails));
                        }
                    case DURATION:
                        if(null != ap.policy.useFor) {
                            return Uni.createFrom().failure(new ActionException("invalidParameters",
                                                                      Tuple2.of("policy.useFor", "Field is required")));
                        } break;
                    case NOTIFY:
                        if(null == ap.policy.notifyLink || null == ap.policy.notifyMessage ||
                           ap.policy.notifyMessage.isBlank() || ap.policy.notifyLink.isBlank()) {
                            var errorDetails = new ArrayList<Tuple2<String, String>>();
                            if(null == ap.policy.notifyLink || ap.policy.notifyLink.isBlank())
                                errorDetails.add(Tuple2.of("policy.notifyLink", "Field is required"));
                            if(null == ap.policy.notifyMessage || ap.policy.notifyMessage.isBlank())
                                errorDetails.add(Tuple2.of("policy.notifyMessage", "Field is required"));

                            return Uni.createFrom().failure(new ActionException("invalidParameters", errorDetails));
                        } break;
                }

                return Uni.createFrom().item(ap);
            })
            .chain(params -> {
                // Create REST client for the IDSA connector
                return getConnectorClient(params);
            })
            .chain(params -> {
                // Got REST client for the IDSA connector
                params.backend = new DataHub();

                // Initialize the backend storage
                return params.backend.initialize(this.backendConfig, spaceId);
            })
            .chain(rootItem -> {
                // Got space details
                var params = apr.get();
                params.item = new DataStoreItem(rootItem.id, rootItem.name, true);
                params.path = params.backend.root();

                // Ensure we have a catalog for this space
                return Actions.validateCatalog(params);
            })
            .chain(params -> {
                // Catalog for the space is ready
                // Publish the content of the space into it
                return Actions.handleFolder(params);
            })
            .chain(params -> {
                // Root folder published, success
                return Uni.createFrom().item(params.response);
            })
            .onFailure().recoverWithItem(e -> {
                LOG.errorf("Failed to publish space %s", spaceId);
                LOG.error(e);

                var params = apr.get();
                List<Tuple2<String, String>> details = new ArrayList<>();
                details.add(Tuple2.of("spaceId", spaceId));
                if(null != params.backend) {
                    var spaceName = params.backend.getSpaceName();
                    if(null != spaceName && !spaceName.isEmpty())
                        details.add(Tuple2.of("spaceName", spaceName));

                    var backendUrl = params.backend.getServiceUrl();
                    if(null != backendUrl && !backendUrl.isEmpty())
                        details.add(Tuple2.of("backendUrl", backendUrl));
                }

                // Trace involved IDSA entities
                if(null != params.connectorId && !params.connectorId.isEmpty())
                    details.add(Tuple2.of("connectorId", params.connectorId));
                if(null != params.catalogId && !params.catalogId.isEmpty())
                    details.add(Tuple2.of("catalogId", params.catalogId));
                if(null != params.offerId && !params.offerId.isEmpty())
                    details.add(Tuple2.of("resourceId", params.offerId));
                if(null != params.representationId && !params.representationId.isEmpty())
                    details.add(Tuple2.of("representationId", params.representationId));
                if(null != params.artifactId && !params.artifactId.isEmpty())
                    details.add(Tuple2.of("artifactId", params.artifactId));
                if(null != params.contractId && !params.contractId.isEmpty())
                    details.add(Tuple2.of("contractId", params.contractId));
                if(null != params.ruleId && !params.ruleId.isEmpty())
                    details.add(Tuple2.of("ruleId", params.ruleId));

                return new ActionError(e, details).toResponse();
            });

        return result;
    }
}
