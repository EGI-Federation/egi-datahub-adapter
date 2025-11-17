package idsa.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;


/**
 * The basic details of an offer
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class OfferBase {

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String title;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String description;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> keywords;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String publisher;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String sovereign;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String language;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String license;
}
