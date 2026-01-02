/*
 * Copyright (c) 2025. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 */
package com.facebook.presto.plugin.denodo.arrow.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DenodoArrowSplitData
{
    private final String vqlQuery;
    private final String identityIdentifier;

    @JsonCreator
    public DenodoArrowSplitData(
            @JsonProperty("vqlQuery") String vqlQuery,
            @JsonProperty("identityIdentifier") String identityIdentifier)
    {
        this.vqlQuery = vqlQuery;
        this.identityIdentifier = identityIdentifier;
    }

    @JsonProperty
    public String getVqlQuery()
    {
        return this.vqlQuery;
    }

    @JsonProperty
    public String identityIdentifier()
    {
        return this.identityIdentifier;
    }
}
