package com.hansight.dynamicjob.sae;

import com.google.common.base.Joiner;
import com.hansight.dynamicjob.tool.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

/**
 * Description: Rule Raw entity definition, for parse raw attribute in Rule entity usage.
 *
 * @Date: 2018/5/17
 */
public class RuleRawEntity extends SystemBase {
    protected Integer id;   //rule id
    protected String name;   //rule name
    protected String desc;   //rule description
    private Boolean descChanged;
    protected int status;   //rule status, start(1) or stop(0)
    private int templateId;   //rule template id, sae needed
    protected String type;   //rule type
    //private int system;

    private List<EventDef> events;   //rule related event, sae needed
    private RepeatDef patternRepeat;   //repeat define, sae needed
    protected List<SelectDef> selects;   //rule output
    private TimeWindow window;   //时间窗口配置, sae needed
    private List<EventAttrDef> groupBy;   //分组条件配置, statistic sae needed
    private String where;   //关联条件， follow by及repeat-until类规则会用到, sae needed
    private Inner innerEvent;   //内部事件, sae needed
    private Map<String, Object> having;   //触发条件中的个数配置, sae needed
    protected Alert alert;   //告警配置

    protected RuleCategory ruleCategory = RuleCategory.SAE;

    public Boolean getDescChanged() {
        return descChanged;
    }

    public void setDescChanged(Boolean descChanged) {
        this.descChanged = descChanged;
    }

    public RuleCategory getRuleCategory() {
        return ruleCategory;
    }

    public void setRuleCategory(RuleCategory ruleCategory) {
        this.ruleCategory = ruleCategory;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getHaving() {
        return having;
    }

    public void setHaving(Map<String, Object> having) {
        this.having = having;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getTemplateId() {
        return templateId;
    }

    public void setTemplateId(int templateId) {
        this.templateId = templateId;
    }

    public List<EventDef> getEvents() {
        return events;
    }

    public void setEvents(List<EventDef> events) {
        this.events = events;
    }

    public void addEvent(EventDef event) {
        if (this.events == null) {
            this.events = new ArrayList<>();
        }
        this.events.add(event);
    }

    public List<SelectDef> getSelects() {
        return selects;
    }

    public void setSelects(List<SelectDef> selects) {
        this.selects = selects;
    }

    public String buildSelectJoins() {
        return Joiner.on(", ").join(selects);
    }

    public TimeWindow getWindow() {
        return window;
    }

    public void setWindow(TimeWindow window) {
        this.window = window;
    }

    public String buildGroupByJoins() {
        if (groupBy == null || groupBy.isEmpty()) {
            return "";
        }
        return Joiner.on(", ").join(groupBy);
    }

    public List<EventAttrDef> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<EventAttrDef> groupBy) {
        this.groupBy = groupBy;
    }

    public RepeatDef getPatternRepeat() {
        return patternRepeat;
    }

    public void setPatternRepeat(RepeatDef patternRepeat) {
        this.patternRepeat = patternRepeat;
    }

    public Alert getAlert() {
        return alert;
    }

    public void setAlert(Alert alert) {
        this.alert = alert;
    }

    public Inner getInnerEvent() {
        return innerEvent;
    }

    public void setInnerEvent(Inner innerEvent) {
        this.innerEvent = innerEvent;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Map<String, String> buildTpl() {
        Map<String, String> data = new HashMap<>();
        if (having == null) {
            return data;
        }
        having.keySet().forEach(e -> {
            Object value = having.get(e);
            data.put(name, value.toString());
        });
        return data;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    @Override
    public String toString() {
        return "RuleRawEntity{" +
                "alert=" + alert +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", status=" + status +
                ", templateId=" + templateId +
                ", type='" + type + '\'' +
                ", events=" + events +
                ", patternRepeat=" + patternRepeat +
                ", selects=" + selects +
                ", window=" + window +
                ", groupBy=" + groupBy +
                ", innerEvent=" + innerEvent +
                ", having=" + having +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RuleRawEntity rule = (RuleRawEntity) o;

        if (id != rule.id) return false;
        if (name != null ? !name.equals(rule.name) : rule.name != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }


    /**
     * Description: single event entity definition, for parse item in 'events' field(list) purpose
     */
    public static class Event {
        private String id;
        private String name;
        private String typeId;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTypeId() {
            return typeId;
        }

        public void setTypeId(String typeId) {
            this.typeId = typeId;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Event event = (Event) o;

            return new EqualsBuilder()
                    .append(id, event.id)
                    .append(name, event.name)
                    .append(typeId, event.typeId)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(id)
                    .append(name)
                    .append(typeId)
                    .toHashCode();
        }
    }

    /**
     * Description: event entity definition, for parse 'events' field(list) purpose
     */
    public static class EventDef {
        private Event event;
        private String as;
        private String filter;

        public Event getEvent() {
            return event;
        }

        public void setEvent(Event event) {
            this.event = event;
        }

        public String getAs() {
            return as;
        }

        public void setAs(String as) {
            this.as = as;
        }

        public String getFilter() {
            return filter;
        }

        public void setFilter(String filter) {
            this.filter = filter;
        }

        @Override
        public String toString() {
            return "EventDef{" +
                    "as='" + as + '\'' +
                    ", event=" + event +
                    ", filter='" + filter + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EventDef eventDef = (EventDef) o;
            return Objects.equals(event, eventDef.event) &&
                    Objects.equals(as, eventDef.as) &&
                    StringUtil.equalsIgnoreCaseAndEmpty(filter, eventDef.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event, as, filter);
        }
    }

    /**
     * Description: repeat entity definition
     */
    public static class RepeatDef {
        private Integer low;
        private Integer high;
        private Boolean changed;

        public Boolean getChanged() {
            return changed;
        }

        public void setChanged(Boolean changed) {
            this.changed = changed;
        }

        public Integer getHigh() {
            return high;
        }

        public void setHigh(Integer high) {
            this.high = high;
        }

        public Integer getLow() {
            return low;
        }

        public void setLow(Integer low) {
            this.low = low;
        }
    }

    /**
     * Description: EventAttrDef entity definition, for parse event purpose, can use in 'window' field and 'selects' field
     */
    public static class EventAttrDef {
        private String as;
        private String attrField;
        private String attrName;
        private String attrType;
        private Event event;

        public String parseMe() {
            return as + "." + attrField;
        }

        public String parseMe(int index) {
            return as + "[" + index + "]." + attrField;
        }

        public String getAs() {
            return as;
        }

        public void setAs(String as) {
            this.as = as;
        }

        public String getAttrField() {
            return attrField;
        }

        public void setAttrField(String attrField) {
            this.attrField = attrField;
        }

        public String getAttrName() {
            return attrName;
        }

        public void setAttrName(String attrName) {
            this.attrName = attrName;
        }

        public String getAttrType() {
            return attrType;
        }

        public void setAttrType(String attrType) {
            this.attrType = attrType;
        }

        public Event getEvent() {
            return event;
        }

        public void setEvent(Event event) {
            this.event = event;
        }

        @Override
        public String toString() {
            return "EventAttrDef{" +
                    "as='" + as + '\'' +
                    ", attrField='" + attrField + '\'' +
                    ", attrName='" + attrName + '\'' +
                    ", attrType='" + attrType + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            EventAttrDef that = (EventAttrDef) o;

            return new EqualsBuilder()
                    .appendSuper(StringUtil.equalsIgnoreCaseAndEmpty(as, that.as))
                    .append(attrField, that.attrField)
                    .append(attrName, that.attrName)
                    .append(attrType, that.attrType)
                    .append(event, that.event)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(as)
                    .append(attrField)
                    .append(attrName)
                    .append(attrType)
                    .append(event)
                    .toHashCode();
        }
    }

    /**
     * Description: TimeWindow entity definition, for parse 'window' field(map) purpose
     */
    public static class TimeWindow {
        private String type;
        private int value;
        private String unit;
        private EventAttrDef event;
        /**
         * Means the time window of sae rule (builtin) is changed by user
         */
        private Boolean changed;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public String getUnit() {
            return unit;
        }

        public void setUnit(String unit) {
            this.unit = unit;
        }

        public EventAttrDef getEvent() {
            return event;
        }

        public void setEvent(EventAttrDef event) {
            this.event = event;
        }

        public Boolean getChanged() {
            return changed;
        }

        public void setChanged(Boolean changed) {
            this.changed = changed;
        }

        @Override
        public String toString() {
            if (event == null) {
                return null;
            }
            return ".win:" + type + "(" + event.getAttrField() + "," + value + " " + unit + ") ";
        }

        public String toRepeatString() {
            StringBuilder sb = new StringBuilder(".win:");
            sb.append(type).append("(").append(event.getAs()).append("[0].")
                    .append(event.getAttrField()).append(",")
                    .append(value).append(" ").append(unit).append(")");
            return sb.toString();
        }

        public String toStringWithAs() {
            if (event == null) {
                return ".win:" + type + "(" + value + " " + unit + ")";
            }
            return ".win:" + type + "(" + event.getAs() + "." + event.getAttrField() + "," + value + " " + unit + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimeWindow that = (TimeWindow) o;
            return value == that.value &&
                    Objects.equals(type, that.type) &&
                    Objects.equals(unit, that.unit) &&
                    Objects.equals(event, that.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, value, unit, event);
        }
    }

    /**
     * Description: select entity definition, for parse 'selects' field(list) purpose
     */
    public static class SelectDef {
        private EventAttrDef event;
        private boolean hasAlias;
        private boolean hasFn;
        private boolean hasTags;
        private String alias;
        private String fn;
        private String[] tags;
        /**
         * Means the select def of sae rule (builtin) is create by user
         */
        private Boolean userEdit;

        // 20181221 新增需求
        private String constValue;
        private static Set<String> constKeyword;

        static {
            constKeyword = new HashSet<>(Arrays.asList(
                    //"id",
                    "alarm_level",
                    "alarm_name",
                    "alarm_stage",
                    "alarm_status",
                    "alarm_count",
                    "alarm_content",
                    "alarm_advice"

            ));
        }

        public String[] parseMe() {
            String[] aliasAndFiled = new String[2];
            StringBuilder sb = new StringBuilder();

            if (hasFn) {
                sb.append(fn);
                sb.append("(");
            }
            sb.append(event.parseMe());
            if (hasFn) {
                sb.append(")");
            }
            aliasAndFiled[0] = sb.toString();

            if (hasAlias) {
                aliasAndFiled[1] = alias;
            } else {
                aliasAndFiled[1] = event.getAttrField();
            }
            if (StringUtils.isEmpty(aliasAndFiled[0]) || StringUtils.isEmpty(aliasAndFiled[1])) {
                return null;
            }
            return aliasAndFiled;
        }

        public String[] parseMe(int index) {
            String[] aliasAndFiled = new String[2];
            StringBuilder sb = new StringBuilder();

            if (hasFn) {
                sb.append(fn);
                sb.append("(");
            }
            sb.append(event.parseMe(index));
            if (hasFn) {
                sb.append(")");
            }
            aliasAndFiled[0] = sb.toString();

            if (hasAlias) {
                aliasAndFiled[1] = alias;
            } else {
                aliasAndFiled[1] = event.getAttrField();
            }
            if (StringUtils.isEmpty(aliasAndFiled[0]) || StringUtils.isEmpty(aliasAndFiled[1])) {
                return null;
            }
            return aliasAndFiled;
        }

        public EventAttrDef getEvent() {
            return event;
        }

        public void setEvent(EventAttrDef event) {
            this.event = event;
        }

        public boolean isHasAlias() {
            return hasAlias;
        }

        public void setHasAlias(boolean hasAlias) {
            this.hasAlias = hasAlias;
        }

        public boolean isHasFn() {
            return hasFn;
        }

        public void setHasFn(boolean hsFn) {
            this.hasFn = hsFn;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getFn() {
            return fn;
        }

        public void setFn(String fn) {
            this.fn = fn;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(String[] tags) {
            this.tags = tags;
        }

        public boolean isHasTags() {
            return hasTags;
        }

        public void setHasTags(boolean hasTags) {
            this.hasTags = hasTags;
        }

        public String getConstValue() {
            return constValue;
        }

        public void setConstValue(String constValue) {
            this.constValue = constValue;
        }

        public Boolean getUserEdit() {
            return userEdit;
        }

        public void setUserEdit(Boolean userEdit) {
            this.userEdit = userEdit;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            SelectDef selectDef = (SelectDef) o;

            String thisTags = Arrays.stream(Optional.ofNullable(tags).orElse(new String[0]))
                    .sorted().reduce((a, b) -> a + b)
                    .orElse("");
            String otherTags = Arrays.stream(Optional.ofNullable(selectDef.tags).orElse(new String[0]))
                    .sorted().reduce((a, b) -> a + b)
                    .orElse("");
            boolean isTagsEqual = StringUtils.equals(thisTags, otherTags);
            boolean isConstValueEqual = (StringUtils.isBlank(constValue) && StringUtils.isBlank(selectDef.constValue)) ||
                    StringUtils.equals(constValue, selectDef.constValue);

            return new EqualsBuilder()
                    .append(hasAlias, selectDef.hasAlias)
                    .append(hasFn, selectDef.hasFn)
                    .append(hasTags, selectDef.hasTags)
                    .append(event, selectDef.event)
                    .append(alias, selectDef.alias)
                    .append(fn, selectDef.fn)
                    .appendSuper(isTagsEqual)
                    .appendSuper(isConstValueEqual)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(event)
                    .append(hasAlias)
                    .append(hasFn)
                    .append(hasTags)
                    .append(alias)
                    .append(fn)
                    .append(tags)
                    .append(userEdit)
                    .append(constValue)
                    .toHashCode();
        }

        @Override
        public String toString() {
            if (constKeyword.contains(event.attrField)) {
                throw new RuntimeException("illegal select attribute:" + event.attrField);
            }
            StringBuilder sb = new StringBuilder();
            if (StringUtils.isNotEmpty(constValue)) {
                switch (StringUtils.lowerCase(event.attrType)) {
                    case "byte":
                    case "short":
                    case "int":
                    case "integer":
                    case "long":
                    case "date":
                        sb.append(constValue);
                        sb.append("L");
                        break;
                    case "float":
                    case "double":
                        sb.append(constValue);
                        sb.append("F");
                        break;
                    case "bool":
                    case "boolean":
                        sb.append(Boolean.parseBoolean(constValue));
                        break;
                    case "string":
                        sb.append("'");
                        sb.append(constValue);
                        sb.append("'");
                        break;
                    default:
                        sb.append("'");
                        sb.append(constValue);
                        sb.append("'");
                }

                sb.append(" AS ");
                sb.append("`");
                sb.append(event.attrField);
                sb.append("`");
            } else if (hasFn) {
                sb.append(fn);
                sb.append("(");
                sb.append(event.as);
                sb.append(".");
                sb.append("`");
                sb.append(event.attrField);
                sb.append("`");
                sb.append(")");

                if (hasAlias) {
                    sb.append(" AS ");
                    sb.append("`");
                    sb.append(alias);
                    sb.append("`");
                } else {
                    sb.setLength(0);
                }
            } else {
                sb.append(event.as);
                sb.append(".");
                sb.append("`");
                sb.append(event.attrField);
                sb.append("`");

                if (hasAlias) {
                    sb.append(" AS ");
                    sb.append("`");
                    sb.append(alias);
                    sb.append("`");
                } else {
                    sb.append(" AS ");
                    sb.append("`");
                    sb.append(event.attrField);
                    sb.append("`");
                }
            }
            return sb.toString();
        }
    }
}