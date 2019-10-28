package com.hansight.dynamicjob.sae;

/**
 * Description: inner event entity definition, for parse 'innerEvent' field(map) purpose
 */
public class Inner {
    private boolean enabled;
    private String name;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "InnerEvent{" +
                "enable=" + enabled +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Inner)) return false;

        Inner inner = (Inner) o;

        if (enabled != inner.enabled) return false;
        return name != null ? name.equals(inner.name) : inner.name == null;
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
