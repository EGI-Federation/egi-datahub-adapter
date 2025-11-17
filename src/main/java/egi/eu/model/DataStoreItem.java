package egi.eu.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import onedata.oneprovider.model.File;


/**
 * Details of a file or folder in the data store
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataStoreItem {

    public boolean isFolder;
    public boolean isIgnored;
    public String id;
    public String name;
    public long size;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Date createdAt;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Date accessedAt;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Date modifiedAt;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> metadata;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String accessToken;

    /***
     * Constructor
     */
    public DataStoreItem(String id, String name) {
        this.id = id;
        this.name = name;
        this.isFolder = false;
        this.isIgnored = false;
    }

    /***
     * Construct when we also know the type
     */
    public DataStoreItem(String id, String name, boolean isFolder) {
        this(id,name);
        this.isFolder = isFolder;
        this.isIgnored = false;
    }

    /***
     * Copy constructor
     */
    public DataStoreItem(DataStoreItem item) {
        this.id = item.id;
        this.name = item.name;
        this.isFolder = item.isFolder;
        this.isIgnored = item.isIgnored;
        this.size = item.size;
        this.createdAt = item.createdAt;
        this.modifiedAt = item.modifiedAt;
        this.accessedAt = item.accessedAt;
        this.accessToken = item.accessToken;

        this.metadata = (null != item.metadata) ? new HashMap<>() : null;
        if(null != this.metadata)
            this.metadata.putAll(item.metadata);
    }

    /***
     * Construct from DataHub file
     */
    public DataStoreItem(File file) {
        this(file.fileId, file.name, file.type.equals(File.TYPE_FOLDER));
        this.size = file.size;

        file.loadDates();
        this.createdAt = file.created;
        this.modifiedAt = file.modified;
        this.accessedAt = file.accessed;
    }

    /***
     * Serialize the metadata to a string
     * @return Metadata ready to be logged, empty string if no metadata present
     */
    public String metadataAsString() {
        String result = "";
        if(null != this.metadata && !this.metadata.isEmpty())
            for (var e : this.metadata.entrySet()) {
                if(!result.isEmpty())
                    result += "\n";

                result += String.format("\t%s -> %s", e.getKey(), e.getValue());
            }

        return result;
    }
}
