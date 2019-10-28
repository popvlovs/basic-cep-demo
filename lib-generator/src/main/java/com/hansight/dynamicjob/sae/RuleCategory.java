package com.hansight.dynamicjob.sae;

import org.apache.commons.lang3.StringUtils;

public enum RuleCategory {
    UNKNOWN(-1, "UNKNOWN"),
    SAE(0, "SAE"),
    HQL(1, "HQL");

    private int type;
    private String name;

    RuleCategory(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public static RuleCategory getCategory(String name) {
        if(name == null)
            return UNKNOWN;
        switch (StringUtils.lowerCase(name)) {
            case "sae":
                return SAE;
            case "hql":
                return HQL;
            default:
                return UNKNOWN;
        }
    }
}
