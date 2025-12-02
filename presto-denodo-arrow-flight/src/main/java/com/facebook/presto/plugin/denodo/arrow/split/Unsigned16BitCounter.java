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

public class Unsigned16BitCounter
{
    private static int counter;

    public Unsigned16BitCounter()
    {
        // void
    }

    public void increment()
    {
        this.counter++;
        // 0xFFFF is the 16-bit mask (0000...00001111111111111111)
        // This ensures the value wraps around when it exceeds 65535
        this.counter &= 0xFFFF;
    }

    public int getValue()
    {
        return this.counter;
    }
}
