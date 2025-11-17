package onedata.onezone.model;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;


/**
 * The details of a user
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

    public String name;
    public String userId;
    public String username;
    public String login;
    public String fullName;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> emails;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> emailList;

    public long creationTime;
    public boolean blocked;
    public boolean basicAuthEnabled;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String alias;
}
