package com.example;

import java.io.Serializable;

public class ClickHouseRow implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String table;
    public final Object[] values;

    public ClickHouseRow(String table, Object... values) {
        this.table = table;
        this.values = values;
    }
}
