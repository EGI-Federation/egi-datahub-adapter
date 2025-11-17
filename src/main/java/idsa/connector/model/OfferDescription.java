package idsa.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;


/**
 * The basic details of an offer (used to create one)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OfferDescription extends OfferBase {

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String paymentMethod;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String endpointDocumentation;

    public String filename;
    public String path;


    public OfferDescription() {}

    public OfferDescription(String filename, String path) {
        this.filename = filename;
        this.path = path;
        this.title = String.format("%s/%s", path.length() > 1 ? path : "", filename);
    }
}
