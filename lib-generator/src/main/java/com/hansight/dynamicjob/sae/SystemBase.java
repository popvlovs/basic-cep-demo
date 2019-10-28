package com.hansight.dynamicjob.sae;

/**
 * Description: super class for Rule/Template class, contains system attribute
 *
 * @Date: 2018/5/16
 */
public class SystemBase {
    @Deprecated
    protected int system = 0; // default user mode

    /**
     * System built-in resource id, global unique (instead of 'system' field)
     */
    protected String sysId;

    public boolean isSystemBuiltin() {
        return sysId != null;
    }

    @Deprecated
    public int getSystem() {
        return sysId != null ? 1 : 0;
    }

    @Deprecated
    public void setSystem(int system) {
        this.system = system;
    }

    public String getSysId() {
        return sysId;
    }

    public void setSysId(String sysId) {
        this.sysId = sysId;
    }
}
