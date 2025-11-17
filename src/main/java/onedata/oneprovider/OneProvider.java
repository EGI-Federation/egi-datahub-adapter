package onedata.oneprovider;

import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Map;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import onedata.oneprovider.model.*;


@Path("/api/v3/oneprovider")
@Produces(MediaType.APPLICATION_JSON)
@RegisterProvider(value = OneProviderExceptionMapper.class)
@RegisterClientHeaders(OneProviderHeadersFactory.class)
public interface OneProvider {

    @GET
    @Path("/spaces/{spaceId}")
    Uni<SpaceRoot> getSpaceAsync(@PathParam("spaceId") String spaceId);

    @GET
    @Path("/data/{fileId}/children")
    Uni<Folder> listFolderContentAsync(@PathParam("fileId") String fileId,
                                       @QueryParam("offset") @DefaultValue("0") int offset,
                                       @QueryParam("limit") @DefaultValue("1000")int limit);

    @GET
    @Path("/data/{fileId}")
    Uni<File> getFileAsync(@PathParam("fileId") String fileId);

    @GET
    @Path("/data/{fileId}/metadata/xattrs")
    Uni<Map<String, String>> getFileMetadataAsync(@PathParam("fileId") String fileId);

    @PUT
    @Path("/data/{fileId}/metadata/xattrs")
    @Consumes(MediaType.APPLICATION_JSON)
    Uni<Response> setFileMetadataAsync(@PathParam("fileId") String fileId, Map<String, String> metadata);

}
