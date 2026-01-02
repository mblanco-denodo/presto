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

import com.facebook.plugin.arrow.ArrowSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

public class DenodoArrowSplit
            extends ArrowSplit
{
    private final DenodoArrowSplitData denodoArrowSplitData;

    @JsonCreator
    public DenodoArrowSplit(
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("flightEndpointBytes") byte[] flightEndpointBytes,
            @JsonProperty("denodoArrowSplitData") @Nullable DenodoArrowSplitData denodoArrowSplitData)
    {
        super(schemaName, tableName, flightEndpointBytes);
        this.denodoArrowSplitData = denodoArrowSplitData;
    }

    @JsonProperty
    public DenodoArrowSplitData getDenodoArrowSplitData()
    {
        return this.denodoArrowSplitData;
    }
}
