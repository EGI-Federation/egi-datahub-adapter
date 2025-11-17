package egi.eu;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;

import egi.eu.model.DataStoreItem;
import egi.eu.model.PublishPolicy;
import idsa.connector.Connector;


/**
 * The parameters of an action to perform (e.g. publish).
 * Allows implementing actions as instances of Function<JobInfo, Response>.
 *
 */
public class ActionParameters {

    // IDSA connector details
    public Connector connector;
    public String connectorId;

    // Entities from the IDSA connector involved in the action
    public String catalogId;
    public String offerId;
    public String representationId;
    public String artifactId;
    public String ruleId;
    public String contractId;

    // Publisher details
    public String publisher;
    public String sovereign;
    public String language;
    public String license;
    public List<String> keywords;
    public PublishPolicy policy;

    // Backend wrapper
    public DataStore backend;

    // Action target
    public DataStoreItem item;
    public String path; // Linux style path, root is "/"

    // Action result
    public Response response;


    /**
     * Constructor
     */
    public ActionParameters() {
        this.response = Response.ok().build();
    }

    /**
     * Copy constructor
     */
    public ActionParameters(ActionParameters ap) {
        assign(ap);
    }

    /**
     * Copy does deep copy
     */
    public ActionParameters assign(ActionParameters ap) {
        this.backend = ap.backend;
        this.connector = ap.connector;
        this.connectorId = ap.connectorId;
        this.catalogId = ap.catalogId;

        /* These do not need passing to each file or subfolder (are different for each)
        this.resourceId = ap.resourceId;
        this.representationId = ap.representationId;
        this.artifactId = ap.artifactId;
        this.ruleId = ap.ruleId;
        this.contractId = ap.contractId;
        this.item = null == ap.item ? null : new DataStoreItem(ap.item);
        this.path = ap.path;
        this.response = ap.response;
        */

        this.publisher = ap.publisher;
        this.sovereign = ap.sovereign;
        this.language = ap.language;
        this.license = ap.license;
        this.policy = ap.policy;

        this.keywords = (null != ap.keywords) ? new ArrayList<>() : null;
        if(null != this.keywords)
            this.keywords.addAll(ap.keywords);

        return this;
    }

    /**
     * Check if the result is a success
     */
    public boolean succeeded() {
        return Family.familyOf(this.response.getStatus()) == Family.SUCCESSFUL;
    }

    /**
     * Check if the result is a failure
     */
    public boolean failed() {
        return Family.familyOf(this.response.getStatus()) != Family.SUCCESSFUL;
    }

}
