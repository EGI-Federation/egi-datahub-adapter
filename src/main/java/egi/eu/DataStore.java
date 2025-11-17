package egi.eu;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import egi.eu.model.DataStoreItem;
import io.smallrye.mutiny.tuples.Tuple2;
import onedata.oneprovider.model.Folder;
import onedata.onezone.model.Token;
import onedata.onezone.model.TokenRequest;

import javax.ws.rs.PathParam;


/***
 * Generic data store abstraction
 */
public interface DataStore {

    /***
     * Get the human-readable name of the data store.
     * @return Name of the transfer service.
     */
    public abstract String name();

    /***
     * Retrieve root path in this data store
     * @return Root path.
     */
    public abstract String root();

    /***
     * Retrieve separator used in this data store
     * @return Separator character (or string).
     */
    public abstract String separator();

    /***
     * Retrieve id of the space being published.
     * @return Space id
     */
    public abstract String getSpaceId();

    /***
     * Retrieve name of the space being published.
     * @return Space name
     */
    public abstract String getSpaceName();

    /***
     * Retrieve URL of the backend service.
     * @return Base URL for the service
     */
    public abstract String getServiceUrl();

    /***
     * Initialize the backend data store, in this case DataHub.
     * @param config The configuration of the backend.
     *               Note: It is an opaque object to allow custom beans that map config properties
     *               to be injected in the resource class. If the implementation of the backend would
     *               be injected instead, its lifetime cannot be controlled efficiently.
     * @param spaceId The space to work with
     * @return Returns the details of the space
     */
    public abstract Uni<DataStoreItem> initialize(Object config, String spaceId);

    /***
     * Get URL to access storage element.
     * @param id Specifies the target item
     * @return URL to access specified item
     */
    public abstract String buildItemUrl(String id);

    /***
     * Get an access token for a storage element.
     * @param id Specifies the target item
     * @return Access token restricted to specified item and point in time after which it should be updated
     */
    public abstract Uni<Tuple2<String, LocalDateTime>> getItemToken(String id);

    /***
     * Fetch the metadata of a storage element.
     * @param id Specifies the target item
     * @return Metadata of indicated item
     */
    public abstract Uni<Map<String, String>> getItemMetadata(String id);

    /***
     * Update the metadata of a storage element.
     * @param id Specifies the target item
     * @param metadata The metadata to set on the target item
     * @return True on success
     */
    public abstract Uni<Boolean> setItemMetadata(String id, Map<String, String> metadata);

    /***
     * List the content of a folder.
     * @param id Specifies the target item, must be a folder
     * @return Content of indicated item
     */
    public abstract Multi<DataStoreItem> listFolderContent(String id);

}
