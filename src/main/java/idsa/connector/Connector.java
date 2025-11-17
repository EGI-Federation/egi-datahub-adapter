package idsa.connector;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;

import java.util.List;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import idsa.connector.model.*;


@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RegisterProvider(value = ConnectorExceptionMapper.class)
@RegisterClientHeaders(ConnectorHeadersFactory.class)
public interface Connector {

    public static final String APPLICATION_HALJSON = "application/hal+json";

    @GET
    @Path("/configurations/active")
    @Produces(APPLICATION_HALJSON)
    Uni<Configuration> getActiveConfigurationAsync();

    @GET
    @Path("/catalogs/{catalogId}")
    Uni<Catalog> getCatalogAsync(@PathParam("catalogId") String catalogId);

    @POST
    @Path("/catalogs")
    Uni<Catalog> createCatalogAsync(CatalogDescription catalog);

    @POST
    @Path("/offers")
    Uni<Offer> createOfferAsync(OfferDescription offer);

    @POST
    @Path("/representations")
    Uni<Representation> createRepresentationAsync(RepresentationBase representation);

    @GET
    @Path("/artifacts/{artifactId}")
    Uni<Artifact> getArtifactAsync(@PathParam("artifactId") String artifactId);

    @POST
    @Path("/artifacts")
    Uni<Artifact> createArtifactAsync(ArtifactDescription artifact);

    @PUT
    @Path("/artifacts/{artifactId}")
    Uni<Artifact> updateArtifactAsync(@PathParam("artifactId") String artifactId, ArtifactDescription artifact);

    @GET
    @Path("/artifacts/{artifactId}/representations")
    Uni<Embedded<Representation>> listArtifactRepresentationsAsync(@PathParam("artifactId") String artifactId);

    @GET
    @Path("/representations/{representationId}/offers")
    Uni<Embedded<Offer>> listRepresentationOffersAsync(@PathParam("representationId") String representationId);

    @GET
    @Path("/offers/{offerId}/contracts")
    Uni<Embedded<Contract>> listOfferContractsAsync(@PathParam("offerId") String offerId);

    @GET
    @Path("/contracts/{contractId}/rules")
    Uni<Embedded<Rule>> listContractRulesAsync(@PathParam("contractId") String contractId);

    @POST
    @Path("/contracts")
    Uni<Contract> createContractAsync(ContractDescription contract);

    @POST
    @Path("/rules")
    Uni<Rule> createRuleAsync(RuleDescription rule);

    @DELETE
    @Path("/rules/{ruleId}")
    Uni<Void> deleteRuleAsync(@PathParam("ruleId") String ruleId);

    @POST
    @Path("/representations/{representationId}/artifacts")
    @Produces(APPLICATION_HALJSON)
    Uni<Embedded<Artifact>> addArtifactsToRepresentationAsync(@PathParam("representationId") String representationId, List<String> artifacts);

    @POST
    @Path("/offers/{resourceId}/representations")
    @Produces(APPLICATION_HALJSON)
    Uni<Embedded<Representation>> addRepresentationsToResourceAsync(@PathParam("resourceId") String resourceId, List<String> representations);

    @POST
    @Path("/contracts/{contractId}/rules")
    @Produces(APPLICATION_HALJSON)
    Uni<Embedded<Rule>> addRulesToContractAsync(@PathParam("contractId") String contractId, List<String> rules);

    @DELETE
    @Path("/contracts/{contractId}/rules")
    @Produces(APPLICATION_HALJSON)
    Uni<Void> removeRulesFromContractAsync(@PathParam("contractId") String contractId, List<String> rules);

    @POST
    @Path("/offers/{resourceId}/contracts")
    @Produces(APPLICATION_HALJSON)
    Uni<Embedded<Contract>> addContractsToResourceAsync(@PathParam("resourceId") String resourceId, List<String> contracts);

    @POST
    @Path("/catalogs/{catalogId}/offers")
    @Produces(APPLICATION_HALJSON)
    Uni<Embedded<Offer>> addResourcesToCatalogAsync(@PathParam("catalogId") String catalogId, List<String> resources);
}
