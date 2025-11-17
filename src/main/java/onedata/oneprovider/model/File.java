package onedata.oneprovider.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * The details of a folder item
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class File {

    public static final String TYPE_FILE = "reg";
    public static final String TYPE_FOLDER = "dir";
    public static final String TYPE_IGNORE = "nope";

    public String name;
    public String type;

    @JsonProperty("file_id")
    public String fileId;
    public Long size;
    public String mode;

    @JsonProperty("owner_id")
    public String ownerId;

    @JsonProperty("provider_id")
    public String providerId;

    private long ctime;
    private long atime;
    private long mtime;

    @JsonIgnore
    public Date created = null;

    @JsonIgnore
    public Date modified = null;

    @JsonIgnore
    public Date accessed = null;


    /**
     * Convert loaded dates to Date objects
     */
    public void loadDates() {
        if(null == created && ctime > 0)
            created  = new Date(TimeUnit.SECONDS.toMillis(ctime));

        if(null == modified && mtime > 0)
            modified = new Date(TimeUnit.SECONDS.toMillis(mtime));

        if(null == accessed && atime > 0)
            accessed = new Date(TimeUnit.SECONDS.toMillis(atime));
    }
}
